package feed_filter.exchanges.binancefut;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import feed_filter.Exchange;
import feed_filter.FeedLogger;
import feed_filter.FundingInfo;
import feed_filter.FundingRateStore;
import feed_filter.InMemoryOrderBookStore;
import feed_filter.MarketDataFeed;
import feed_filter.OrderBookDepthConfig;
import feed_filter.OrderBookLevel;
import feed_filter.OrderBookSnapshot;
import feed_filter.OrderBookStore;
import okhttp3.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;

public class BinanceFuturesFeed implements MarketDataFeed {

    public record Sub(String internalSymbol, String externalSymbol) {}

    private final List<Sub> subs;
    private final OrderBookStore store;
    private final Function<String, String> externalToInternalMapper;
    private final ObjectMapper jsonMapper = new ObjectMapper();
    private final Map<String, Consumer<String>> activeHandlers = new HashMap<>();
    private final FundingRateStore fundingStore;
    private final long fundingPollIntervalMs;
    private final OkHttpClient fundingClient;
    private final ScheduledExecutorService fundingExec;
    private volatile ScheduledFuture<?> fundingTask;
    private final Map<String, Integer> fundingIntervalHoursBySymbol = new ConcurrentHashMap<>();
    private final Map<String, Long> fundingIntervalFetchMs = new ConcurrentHashMap<>();
    private static final long FUNDING_INTERVAL_CACHE_MS = 6 * 60 * 60 * 1000L;

    public BinanceFuturesFeed(List<Sub> subs,
                              OrderBookStore store,
                              Function<String, String> externalToInternalMapper,
                              FundingRateStore fundingStore,
                              long fundingPollIntervalMs) {
        this.subs = subs;
        this.store = store;
        this.externalToInternalMapper = externalToInternalMapper;
        this.fundingStore = fundingStore;
        this.fundingPollIntervalMs = fundingPollIntervalMs;
        this.fundingClient = new OkHttpClient.Builder()
                .callTimeout(5, TimeUnit.SECONDS)
                .build();
        this.fundingExec = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "binancefut-funding");
            t.setDaemon(true);
            return t;
        });
    }

    @Override
    public void start() {
        FeedLogger.info("BINANCEFUT", "Starting feed for " + subs.size() + " symbols...");
        
        // Log symbol list
        StringBuilder sb = new StringBuilder("[BINANCEFUT] Symbols: ");
        for (int i = 0; i < subs.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(subs.get(i).internalSymbol());
        }
        System.out.println(sb.toString());
        
        for (Sub s : subs) {
            String external = s.externalSymbol().toLowerCase(Locale.ROOT);
            String internal = s.internalSymbol();
            Consumer<String> handler = json -> handleMessage(internal, json);
            activeHandlers.put(external, handler);
            Shared.register(external, handler);
        }

        startFundingPoller();
    }

    @Override
    public void stop() {
        FeedLogger.info("BINANCEFUT", "Stopping feed...");
        for (Map.Entry<String, Consumer<String>> entry : activeHandlers.entrySet()) {
            Shared.unregister(entry.getKey(), entry.getValue());
        }
        activeHandlers.clear();
        stopFundingPoller();
    }

    private void startFundingPoller() {
        if (fundingStore == null || fundingPollIntervalMs <= 0 || subs.isEmpty()) return;
        if (fundingTask != null) return;
        fundingTask = fundingExec.scheduleAtFixedRate(
                this::pollFundingRates,
                1000L,
                fundingPollIntervalMs,
                TimeUnit.MILLISECONDS
        );
    }

    private void stopFundingPoller() {
        ScheduledFuture<?> task = fundingTask;
        fundingTask = null;
        if (task != null) {
            try { task.cancel(false); } catch (Exception ignored) {}
        }
        fundingExec.shutdownNow();
    }

    public void refreshFundingNow() {
        if (fundingStore == null || subs.isEmpty()) return;
        fundingExec.execute(this::pollFundingRates);
    }

    private void pollFundingRates() {
        if (subs.isEmpty() || fundingStore == null) return;
        int ok = 0;
        int fail = 0;
        for (Sub s : subs) {
            FundingInfo info = fetchFundingInfo(s.externalSymbol());
            if (info != null) {
                fundingStore.put(Exchange.BINANCEFUT, s.internalSymbol(), info);
                ok++;
            } else {
                fail++;
            }
        }
        FeedLogger.info("BINANCEFUT", "Funding poll: ok=" + ok + " fail=" + fail);
    }

    private FundingInfo fetchFundingInfo(String externalSymbol) {
        if (externalSymbol == null || externalSymbol.isEmpty()) return null;
        String url = "https://fapi.binance.com/fapi/v1/premiumIndex?symbol=" + externalSymbol.toUpperCase(Locale.ROOT);
        Request req = new Request.Builder().url(url).get().build();
        try (Response resp = fundingClient.newCall(req).execute()) {
            if (!resp.isSuccessful() || resp.body() == null) return null;
            String body = resp.body().string();
            JsonNode root = jsonMapper.readTree(body);
            double rate = root.path("lastFundingRate").asDouble(Double.NaN);
            if (Double.isNaN(rate)) return null;
            long nextFundingTime = root.path("nextFundingTime").asLong(0L);
            long serverTime = root.path("time").asLong(0L);
            Integer intervalHours = getFundingIntervalHours(externalSymbol);
            long sourceTs = serverTime > 0 ? serverTime : System.currentTimeMillis();
            Long nextTs = nextFundingTime > 0 ? nextFundingTime : null;
            return new FundingInfo(rate, intervalHours, nextTs, null, sourceTs);
        } catch (Exception e) {
            return null;
        }
    }

    private Integer getFundingIntervalHours(String externalSymbol) {
        if (externalSymbol == null || externalSymbol.isEmpty()) return null;
        String symbol = externalSymbol.toUpperCase(Locale.ROOT);
        Long lastFetch = fundingIntervalFetchMs.get(symbol);
        if (lastFetch != null && (System.currentTimeMillis() - lastFetch) < FUNDING_INTERVAL_CACHE_MS) {
            return fundingIntervalHoursBySymbol.get(symbol);
        }
        Integer interval = fetchFundingIntervalHours(symbol);
        if (interval != null) {
            fundingIntervalHoursBySymbol.put(symbol, interval);
            fundingIntervalFetchMs.put(symbol, System.currentTimeMillis());
        }
        return interval;
    }

    private Integer fetchFundingIntervalHours(String symbol) {
        String url = "https://fapi.binance.com/fapi/v1/fundingRate?symbol=" + symbol + "&limit=2";
        Request req = new Request.Builder().url(url).get().build();
        try (Response resp = fundingClient.newCall(req).execute()) {
            if (!resp.isSuccessful() || resp.body() == null) return null;
            String body = resp.body().string();
            JsonNode root = jsonMapper.readTree(body);
            if (!root.isArray() || root.size() < 2) return null;
            long t0 = root.get(0).path("fundingTime").asLong(0L);
            long t1 = root.get(1).path("fundingTime").asLong(0L);
            if (t0 <= 0 || t1 <= 0) return null;
            long diffMs = Math.abs(t0 - t1);
            if (diffMs <= 0) return null;
            return (int) Math.round(diffMs / 3_600_000.0);
        } catch (Exception e) {
            return null;
        }
    }

    private void handleMessage(String internalSymbol, String json) {
        try {
            JsonNode root = jsonMapper.readTree(json);

            long eventTime = 0;
            if (root.has("E")) eventTime = root.get("E").asLong();
            else eventTime = System.currentTimeMillis();

            JsonNode bidsNode = root.has("b") ? root.get("b") : root.get("bids");
            JsonNode asksNode = root.has("a") ? root.get("a") : root.get("asks");

            List<OrderBookLevel> bids = parseLevels(bidsNode);
            List<OrderBookLevel> asks = parseLevels(asksNode);

            OrderBookSnapshot snap = new OrderBookSnapshot(
                    Exchange.BINANCEFUT,
                    internalSymbol,
                    eventTime,
                    bids,
                    asks
            );

            if (store instanceof InMemoryOrderBookStore) {
                ((InMemoryOrderBookStore) store).putSnapshot(snap);
            } else {
                store.update(snap);
            }

        } catch (Exception e) {
            if (ThreadLocalRandom.current().nextDouble() < 0.001) {
                FeedLogger.error("BINANCEFUT", "Parse error for " + internalSymbol, e);
            }
        }
    }

    private List<OrderBookLevel> parseLevels(JsonNode node) {
        if (node == null || !node.isArray()) return Collections.emptyList();
        List<OrderBookLevel> levels = new ArrayList<>(node.size());
        for (JsonNode el : node) {
            double px = Double.parseDouble(el.get(0).asText());
            double sz = Double.parseDouble(el.get(1).asText());
            if (sz > 0) {
                levels.add(new OrderBookLevel(px, sz));
            }
        }
        return levels;
    }

    // ======================================================================================
    // SHARED CONNECTION MANAGER
    // ======================================================================================
    private static final class Shared {
        // Tolerant watchdog (60s)
        private static final long GLOBAL_SILENCE_TIMEOUT_MS = 60_000L;
        private static final long WATCHDOG_INTERVAL_MS      = 2_000L;
        private static final long REBUILD_DEBOUNCE_MS       = 1_000L;
        private static final int  MAX_STREAMS_PER_CONN      = 100;  // Binance limit is 200, use 100 to reduce connections
        private static final long BACKOFF_BASE_MS           = 500L;
        private static final long BACKOFF_MAX_MS            = 10_000L;

        private static final Pattern SYMBOL_OK = Pattern.compile("^[a-z0-9]+$");
        private static final ObjectMapper MAPPER = new ObjectMapper();

        // Keep-alive pings every 30s to prevent server closing "idle" connections
        // (Binance has 5min timeout, but intermediate proxies may close earlier)
        private static final OkHttpClient CLIENT = new OkHttpClient.Builder()
                .readTimeout(0, TimeUnit.MILLISECONDS)
                .pingInterval(30, TimeUnit.SECONDS)
                .retryOnConnectionFailure(true)
                .build();

        private static final ScheduledExecutorService EXEC =
                Executors.newScheduledThreadPool(4, r -> {
                    Thread t = new Thread(r, "binance-ws-shared");
                    t.setDaemon(true);
                    return t;
                });

        private static final ConcurrentHashMap<String, CopyOnWriteArrayList<Consumer<String>>> handlersBySymbol =
                new ConcurrentHashMap<>();

        private static final Object lock = new Object();
        private static final ArrayList<WsGroup> groups = new ArrayList<>();
        private static final AtomicLong lastAnyMessageMs = new AtomicLong(System.currentTimeMillis());
        private static final AtomicBoolean watchdogStarted = new AtomicBoolean(false);
        private static final AtomicBoolean rebuildScheduled = new AtomicBoolean(false);

        static void register(String symbol, Consumer<String> handler) {
            String s = sanitizeSymbol(symbol);
            if (s == null || handler == null) return;
            handlersBySymbol.computeIfAbsent(s, k -> new CopyOnWriteArrayList<>()).add(handler);
            startWatchdogIfNeeded();
            scheduleRebuild("register");
        }

        static void unregister(String symbol, Consumer<String> handler) {
            String s = sanitizeSymbol(symbol);
            if (s == null) return;
            if (handlersBySymbol.containsKey(s)) {
                handlersBySymbol.get(s).remove(handler);
                if (handlersBySymbol.get(s).isEmpty()) {
                    handlersBySymbol.remove(s);
                }
            }
            scheduleRebuild("unregister");
        }

        private static String sanitizeSymbol(String symbol) {
            if (symbol == null) return null;
            String s = symbol.toLowerCase(Locale.ROOT).trim();
            return SYMBOL_OK.matcher(s).matches() ? s : null;
        }

        private static void startWatchdogIfNeeded() {
            if (!watchdogStarted.compareAndSet(false, true)) return;
            EXEC.scheduleAtFixedRate(() -> {
                if (handlersBySymbol.isEmpty()) return;
                long diff = System.currentTimeMillis() - lastAnyMessageMs.get();
                if (diff > GLOBAL_SILENCE_TIMEOUT_MS) {
                    FeedLogger.error("BINANCEFUT", "Global Silence (" + diff + "ms). Reconnecting all.");
                    lastAnyMessageMs.set(System.currentTimeMillis());
                    rebuildConnections("global-silence");
                }
            }, WATCHDOG_INTERVAL_MS, WATCHDOG_INTERVAL_MS, TimeUnit.MILLISECONDS);
        }

        private static void scheduleRebuild(String reason) {
            if (!rebuildScheduled.compareAndSet(false, true)) return;
            EXEC.schedule(() -> {
                rebuildScheduled.set(false);
                rebuildConnections(reason);
            }, REBUILD_DEBOUNCE_MS, TimeUnit.MILLISECONDS);
        }

        private static void rebuildConnections(String reason) {
            synchronized (lock) {
                int depth = OrderBookDepthConfig.BINANCE_FUT_DEPTH;
                if (depth != 5 && depth != 10 && depth != 20) {
                    depth = 20;
                }

                List<String> streamNames = new ArrayList<>();
                for (String sym : handlersBySymbol.keySet()) {
                    streamNames.add(sym + "@depth" + depth + "@100ms");
                }
                Collections.sort(streamNames);

                closeAll("rebuild:" + reason);
                groups.clear();

                if (streamNames.isEmpty()) return;

                List<List<String>> chunks = chunk(streamNames, MAX_STREAMS_PER_CONN);
                int id = 0;
                for (List<String> c : chunks) {
                    WsGroup g = new WsGroup(id++, c);
                    groups.add(g);
                    g.connect(0L);
                }

                lastAnyMessageMs.set(System.currentTimeMillis());
            }
        }

        private static void closeAll(String reason) {
            for (WsGroup g : groups) g.close(reason);
        }

        private static List<List<String>> chunk(List<String> items, int size) {
            List<List<String>> out = new ArrayList<>();
            for (int i = 0; i < items.size(); i += size) {
                out.add(items.subList(i, Math.min(items.size(), i + size)));
            }
            return out;
        }

        private static final class WsGroup extends WebSocketListener {
            private final int id;
            private final String url;
            private WebSocket ws;
            private boolean isClosed = false;
            private final AtomicInteger failureStreak = new AtomicInteger(0);

            WsGroup(int id, List<String> streams) {
                this.id = id;
                String joined = String.join("/", streams);
                this.url = "wss://fstream.binance.com/stream?streams=" + joined;
            }

            void connect(long delayMs) {
                if (isClosed) return;
                if (delayMs > 0) {
                    EXEC.schedule(this::doConnect, delayMs, TimeUnit.MILLISECONDS);
                } else {
                    doConnect();
                }
            }

            private void doConnect() {
                if (isClosed) return;
                Request req = new Request.Builder()
                        .url(url)
                        .header("User-Agent", "Mozilla/5.0") // Standard UA
                        .build();
                CLIENT.newWebSocket(req, this);
            }

            void close(String reason) {
                isClosed = true;
                if (ws != null) {
                    try { ws.close(1000, reason); } catch (Exception e) {}
                }
            }

            @Override
            public void onOpen(WebSocket webSocket, Response response) {
                this.ws = webSocket;
                FeedLogger.info("BINANCEFUT", "G" + id + " Connected.");
                lastAnyMessageMs.set(System.currentTimeMillis());
                failureStreak.set(0);
            }

            @Override
            public void onMessage(WebSocket webSocket, String text) {
                if (isClosed) return;
                lastAnyMessageMs.set(System.currentTimeMillis());

                try {
                    JsonNode root = MAPPER.readTree(text);
                    JsonNode streamNode = root.get("stream");
                    JsonNode dataNode = root.get("data");

                    if (streamNode == null || dataNode == null) return;

                    String streamName = streamNode.asText();
                    int atIndex = streamName.indexOf('@');
                    if (atIndex <= 0) return;

                    String sym = streamName.substring(0, atIndex);

                    List<Consumer<String>> handlers = handlersBySymbol.get(sym);
                    if (handlers != null) {
                        String payload = dataNode.toString();
                        for (Consumer<String> h : handlers) h.accept(payload);
                    }
                } catch (Exception e) {
                    // ignore
                }
            }

            @Override
            public void onFailure(WebSocket webSocket, Throwable t, Response response) {
                if (isClosed) return;

                String msg = t.getMessage();
                if (msg == null) msg = t.getClass().getSimpleName();

                FeedLogger.error("BINANCEFUT", "G" + id + " Fail: " + msg);

                int streak = failureStreak.incrementAndGet();
                long backoff = Math.min(BACKOFF_MAX_MS, BACKOFF_BASE_MS * (1L << Math.min(streak, 6)));

                try { if (ws != null) ws.cancel(); } catch(Exception e) {}
                ws = null;

                connect(backoff);
            }

            @Override
            public void onClosed(WebSocket webSocket, int code, String reason) {
                if (isClosed) return;
                onFailure(webSocket, new Exception("Closed: " + reason), null);
            }
        }
    }
}