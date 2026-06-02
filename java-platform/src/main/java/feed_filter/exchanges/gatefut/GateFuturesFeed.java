package feed_filter.exchanges.gatefut;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import feed_filter.*;
import okhttp3.*;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * Gate.io USDT futures orderbook feed.
 * Supports batch subscriptions to multiple contracts.
 */
public class GateFuturesFeed implements MarketDataFeed {

    private static final String WS_URL = "wss://fx-ws.gateio.ws/v4/ws/usdt";
    private static final String REST_CONTRACT_URL = "https://api.gateio.ws/api/v4/futures/usdt/contracts/";
    private static final long PING_INTERVAL_MS =
            Long.parseLong(System.getenv().getOrDefault("GATEFUT_WS_PING_MS", "10000"));
    private static final String OB_UPDATE_INTERVAL =
            System.getenv().getOrDefault("GATEFUT_OB_UPDATE_INTERVAL", "100ms");
    private static final long RESYNC_MIN_INTERVAL_MS =
            Long.parseLong(System.getenv().getOrDefault("GATEFUT_OB_RESYNC_MIN_MS", "5000"));
    private static final long RECONNECT_BASE_DELAY_MS = 1_000L;
    private static final long RECONNECT_MAX_DELAY_MS = 15_000L;

    public record Sub(String internalSymbol, String externalSymbol, BigDecimal contractSize) {}

    private final List<Sub> subs;
    private final OrderBookStore store;
    private final Function<String, String> externalToInternal;
    private final int depth;
    private final long globalSilenceTimeoutMs;
    private final FundingRateStore fundingStore;
    private final long fundingPollIntervalMs;

    private final OkHttpClient client;
    private final ObjectMapper mapper = new ObjectMapper();
    private final ScheduledExecutorService exec;
    private final ScheduledExecutorService fundingExec;
    private volatile ScheduledFuture<?> fundingTask;

    private volatile WebSocket webSocket;
    private volatile boolean running = false;
    private volatile ScheduledFuture<?> pingTask;
    private volatile ScheduledFuture<?> watchdogTask;

    private final AtomicLong lastAnyMessageMs = new AtomicLong(0L);
    private final AtomicBoolean reconnectInFlight = new AtomicBoolean(false);
    private volatile int reconnectStreak = 0;

    // Contract size cache: externalSymbol -> contractSize
    private final Map<String, Double> contractSizeMap = new ConcurrentHashMap<>();
    private final Map<String, String> externalToInternalMap = new ConcurrentHashMap<>();
    private final Map<String, BookState> books = new ConcurrentHashMap<>();
    private final Map<String, Long> lastResyncMs = new ConcurrentHashMap<>();

    private static final class BookState {
        final TreeMap<Double, Double> bids = new TreeMap<>(Comparator.reverseOrder());
        final TreeMap<Double, Double> asks = new TreeMap<>();
        volatile boolean initialized = false;
        volatile long lastSeqId = 0L;
        volatile long lastTs = 0L;
        volatile String internalSymbol;
        volatile double contractSize = 1.0;
    }

    public GateFuturesFeed(List<Sub> subs, OrderBookStore store, 
                          Function<String, String> externalToInternal, int depth,
                          long globalSilenceTimeoutMs,
                          FundingRateStore fundingStore,
                          long fundingPollIntervalMs) {
        this.subs = subs;
        this.store = store;
        this.externalToInternal = externalToInternal;
        this.depth = depth;
        this.globalSilenceTimeoutMs = globalSilenceTimeoutMs > 0 ? globalSilenceTimeoutMs : 60_000L;
        this.fundingStore = fundingStore;
        this.fundingPollIntervalMs = fundingPollIntervalMs;

        for (Sub s : subs) {
            double cs = s.contractSize != null ? s.contractSize.doubleValue() : 1.0;
            contractSizeMap.put(s.externalSymbol, cs);
            externalToInternalMap.put(s.externalSymbol, s.internalSymbol);
            BookState st = new BookState();
            st.internalSymbol = s.internalSymbol;
            st.contractSize = cs;
            books.put(s.externalSymbol, st);
        }

        this.client = new OkHttpClient.Builder()
                .readTimeout(0, TimeUnit.MILLISECONDS)
                .pingInterval(0, TimeUnit.MILLISECONDS)
                .build();

        this.exec = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "gatefut-feed");
            t.setDaemon(true);
            return t;
        });
        this.fundingExec = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "gatefut-funding");
            t.setDaemon(true);
            return t;
        });
    }

    @Override
    public void start() {
        running = true;
        lastAnyMessageMs.set(System.currentTimeMillis());

        FeedLogger.info("GATEFUT", "Starting feed for " + subs.size() + " contracts");
        startWatchdog();
        connect();
        startFundingPoller();
    }

    @Override
    public void stop() {
        running = false;
        stopPingLoop();
        stopWatchdog();

        WebSocket ws = this.webSocket;
        this.webSocket = null;
        if (ws != null) {
            try { ws.close(1000, "shutdown"); } catch (Exception ignored) {}
            try { ws.cancel(); } catch (Exception ignored) {}
        }

        exec.shutdownNow();
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
                fundingStore.put(Exchange.GATEFUT, s.internalSymbol(), info);
                ok++;
            } else {
                fail++;
            }
        }
        FeedLogger.info("GATEFUT", "Funding poll: ok=" + ok + " fail=" + fail);
    }

    private FundingInfo fetchFundingInfo(String contract) {
        if (contract == null || contract.isEmpty()) return null;
        String url = REST_CONTRACT_URL + contract;
        Request req = new Request.Builder().url(url).get().build();
        try (Response resp = client.newCall(req).execute()) {
            if (!resp.isSuccessful() || resp.body() == null) return null;
            String body = resp.body().string();
            JsonNode root = mapper.readTree(body);
            double rate = root.path("funding_rate").asDouble(Double.NaN);
            if (Double.isNaN(rate)) return null;
            long nextFunding = root.path("funding_next_apply").asLong(0L) * 1000L;
            long lastFunding = root.path("funding_last_apply").asLong(0L) * 1000L;
            Integer intervalHours = null;
            JsonNode intervalNode = root.get("funding_interval");
            if (intervalNode != null && !intervalNode.isNull()) {
                long intervalSec = intervalNode.asLong(0L);
                if (intervalSec > 0) {
                    intervalHours = (int) Math.round(intervalSec / 3600.0);
                }
            }
            Long nextTs = nextFunding > 0 ? nextFunding : null;
            Long lastTs = lastFunding > 0 ? lastFunding : null;
            return new FundingInfo(rate, intervalHours, nextTs, lastTs, System.currentTimeMillis());
        } catch (Exception e) {
            return null;
        }
    }

    private void connect() {
        if (!running) return;

        WebSocket prev = this.webSocket;
        this.webSocket = null;
        if (prev != null) {
            try { prev.close(1000, "reconnect"); } catch (Exception ignored) {}
            try { prev.cancel(); } catch (Exception ignored) {}
        }

        Request request = new Request.Builder().url(WS_URL).build();
        this.webSocket = client.newWebSocket(request, new WebSocketListener() {

            @Override
            public void onOpen(WebSocket ws, Response response) {
                FeedLogger.info("GATEFUT", "Connected");
                reconnectStreak = 0;
                reconnectInFlight.set(false);
                lastAnyMessageMs.set(System.currentTimeMillis());

                // Subscribe to all contracts
                for (Sub s : subs) {
                    long nowSec = System.currentTimeMillis() / 1000L;
                    String subSnap = String.format(
                            "{\"time\":%d,\"channel\":\"futures.order_book\",\"event\":\"subscribe\",\"payload\":[\"%s\",\"%d\",\"0\"]}",
                            nowSec, s.externalSymbol, depth
                    );
                    String subUpdate = String.format(
                            "{\"time\":%d,\"channel\":\"futures.order_book_update\",\"event\":\"subscribe\",\"payload\":[\"%s\",\"%d\",\"%s\"]}",
                            nowSec, s.externalSymbol, depth, OB_UPDATE_INTERVAL
                    );
                    ws.send(subSnap);
                    ws.send(subUpdate);
                }
                FeedLogger.info("GATEFUT", "Subscribed to " + subs.size() + " contracts (depth=" + depth + ")");

                startPingLoop(ws);
            }

            @Override
            public void onMessage(WebSocket ws, String text) {
                if (text == null) return;
                lastAnyMessageMs.set(System.currentTimeMillis());

                if (text.contains("\"channel\":\"futures.order_book\"")
                        || text.contains("\"channel\":\"futures.order_book_update\"")) {
                    handleOrderBookMessage(text);
                }
            }

            @Override
            public void onFailure(WebSocket ws, Throwable t, Response r) {
                if (!running) return;
                String msg = t.getClass().getSimpleName() + (t.getMessage() != null ? ": " + t.getMessage() : "");
                FeedLogger.error("GATEFUT", "WS failure: " + msg);
                cleanupAndReconnect(ws, "failure");
            }

            @Override
            public void onClosed(WebSocket ws, int code, String reason) {
                if (!running) return;
                FeedLogger.info("GATEFUT", "WS closed: " + code + " " + reason);
                cleanupAndReconnect(ws, "closed");
            }
        });
    }

    private void cleanupAndReconnect(WebSocket ws, String reason) {
        stopPingLoop();
        if (this.webSocket == ws) {
            this.webSocket = null;
        }
        try { ws.cancel(); } catch (Exception ignored) {}
        scheduleReconnect(reason);
    }

    private void startPingLoop(WebSocket ws) {
        stopPingLoop();
        pingTask = exec.scheduleAtFixedRate(() -> {
            if (!running || ws != webSocket) return;
            try {
                long nowSec = System.currentTimeMillis() / 1000L;
                String ping = String.format("{\"time\":%d,\"channel\":\"futures.ping\"}", nowSec);
                ws.send(ping);
            } catch (Exception ignored) {}
        }, PING_INTERVAL_MS, PING_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    private void stopPingLoop() {
        ScheduledFuture<?> p = pingTask;
        pingTask = null;
        if (p != null) {
            try { p.cancel(false); } catch (Exception ignored) {}
        }
    }

    private void startWatchdog() {
        stopWatchdog();
        watchdogTask = exec.scheduleAtFixedRate(() -> {
            if (!running) return;

            // Global silence check - if no data for configured timeout, reconnect
            long silenceDiff = System.currentTimeMillis() - lastAnyMessageMs.get();
            if (silenceDiff > globalSilenceTimeoutMs) {
                if (reconnectInFlight.compareAndSet(false, true)) {
                    FeedLogger.warn("GATEFUT", "Global silence (" + silenceDiff + "ms) -> reconnecting");
                    forceReconnect();
                }
            }
        }, 5000L, 5000L, TimeUnit.MILLISECONDS);  // check every 5s instead of 1s
    }

    private void stopWatchdog() {
        ScheduledFuture<?> w = watchdogTask;
        watchdogTask = null;
        if (w != null) {
            try { w.cancel(false); } catch (Exception ignored) {}
        }
    }

    private void forceReconnect() {
        exec.execute(() -> {
            if (!running) {
                reconnectInFlight.set(false);
                return;
            }
            stopPingLoop();
            WebSocket ws = this.webSocket;
            this.webSocket = null;
            if (ws != null) {
                try { ws.close(1000, "reconnect"); } catch (Exception ignored) {}
                try { ws.cancel(); } catch (Exception ignored) {}
            }
            connect();
        });
    }

    private void scheduleReconnect(String reason) {
        if (!running) return;
        if (!reconnectInFlight.compareAndSet(false, true)) return;

        reconnectStreak++;
        long delay = Math.min(
                RECONNECT_MAX_DELAY_MS,
                RECONNECT_BASE_DELAY_MS * (1L << Math.min(10, reconnectStreak - 1))
        );

        FeedLogger.info("GATEFUT", "Reconnect scheduled in " + delay + "ms (" + reason + ")");

        exec.schedule(() -> {
            if (!running) {
                reconnectInFlight.set(false);
                return;
            }
            WebSocket ws = this.webSocket;
            this.webSocket = null;
            if (ws != null) {
                try { ws.close(1000, "reconnect"); } catch (Exception ignored) {}
                try { ws.cancel(); } catch (Exception ignored) {}
            }
            connect();
        }, delay, TimeUnit.MILLISECONDS);
    }

    private void handleOrderBookMessage(String text) {
        try {
            JsonNode root = mapper.readTree(text);
            String channel = root.path("channel").asText();
            JsonNode result = root.get("result");
            if (result == null || result.isNull()) return;

            String contract = result.path("contract").asText();
            if (contract == null || contract.isEmpty()) return;

            BookState book = books.computeIfAbsent(contract, c -> {
                BookState st = new BookState();
                st.internalSymbol = resolveInternalSymbol(c);
                st.contractSize = contractSizeMap.getOrDefault(c, 1.0);
                return st;
            });

            if ("futures.order_book".equals(channel)) {
                applySnapshot(result, book);
                publishSnapshot(book);
            } else if ("futures.order_book_update".equals(channel)) {
                if (applyUpdate(result, book)) {
                    publishSnapshot(book);
                }
            }
        } catch (Exception ignored) {
        }
    }

    private void applySnapshot(JsonNode result, BookState book) {
        long ts = result.path("t").asLong(0L);
        long seq = result.path("id").asLong(0L);

        synchronized (book) {
            book.bids.clear();
            book.asks.clear();
            parseLevels(result.get("bids"), book.contractSize, false, book.bids);
            parseLevels(result.get("asks"), book.contractSize, false, book.asks);
            book.initialized = true;
            book.lastSeqId = seq > 0 ? seq : 0L;
            book.lastTs = ts > 0 ? ts : System.currentTimeMillis();
        }
    }

    private boolean applyUpdate(JsonNode result, BookState book) {
        long seq = result.path("id").asLong(0L);

        synchronized (book) {
            if (!book.initialized) {
                requestResync(result.path("contract").asText());
                return false;
            }

            if (seq > 0 && book.lastSeqId > 0) {
                if (seq <= book.lastSeqId) {
                    return false; // out-of-order
                }
                if (seq != book.lastSeqId + 1) {
                    requestResync(result.path("contract").asText());
                    return false;
                }
            }

            parseLevels(result.get("bids"), book.contractSize, true, book.bids);
            parseLevels(result.get("asks"), book.contractSize, true, book.asks);
            if (seq > 0) {
                book.lastSeqId = seq;
            }
            long ts = result.path("t").asLong(0L);
            book.lastTs = ts > 0 ? ts : System.currentTimeMillis();
            return true;
        }
    }

    private void publishSnapshot(BookState book) {
        if (book.internalSymbol == null || book.internalSymbol.isEmpty()) return;
        OrderBookSnapshot snap = new OrderBookSnapshot(
                Exchange.GATEFUT,
                book.internalSymbol,
                book.lastTs > 0 ? book.lastTs : System.currentTimeMillis(),
                toLevels(book.bids, depth),
                toLevels(book.asks, depth)
        );
        if (store instanceof InMemoryOrderBookStore) {
            ((InMemoryOrderBookStore) store).putSnapshot(snap);
        } else {
            store.update(snap);
        }
    }

    private void requestResync(String contract) {
        if (contract == null || contract.isEmpty()) return;
        long now = System.currentTimeMillis();
        long last = lastResyncMs.getOrDefault(contract, 0L);
        if (now - last < RESYNC_MIN_INTERVAL_MS) return;
        lastResyncMs.put(contract, now);

        BookState book = books.get(contract);
        if (book != null) {
            synchronized (book) {
                book.initialized = false;
                book.lastSeqId = 0L;
            }
        }

        WebSocket ws = this.webSocket;
        if (ws == null) return;
        long nowSec = now / 1000L;
        String subSnap = String.format(
                "{\"time\":%d,\"channel\":\"futures.order_book\",\"event\":\"subscribe\",\"payload\":[\"%s\",\"%d\",\"0\"]}",
                nowSec, contract, depth
        );
        ws.send(subSnap);
    }

    private String resolveInternalSymbol(String contract) {
        String mapped = externalToInternalMap.get(contract);
        if (mapped != null && !mapped.isEmpty()) return mapped;
        if (externalToInternal != null) {
            try {
                String alt = externalToInternal.apply(contract);
                if (alt != null && !alt.isEmpty()) return alt;
            } catch (Exception ignored) {
            }
        }
        return contract.replace("_", "");
    }

    private static void parseLevels(JsonNode levelsNode,
                                    double contractSize,
                                    boolean allowZero,
                                    TreeMap<Double, Double> out) {
        if (levelsNode == null || !levelsNode.isArray()) return;
        for (JsonNode level : levelsNode) {
            double price;
            double sizeContracts;
            if (level.isObject()) {
                price = parseDouble(level.path("p").asText("0"));
                sizeContracts = level.path("s").asDouble(0.0);
            } else if (level.isArray() && level.size() >= 2) {
                price = parseDouble(level.get(0).asText("0"));
                sizeContracts = parseDouble(level.get(1).asText("0"));
            } else {
                continue;
            }

            if (!(price > 0)) continue;
            double size = sizeContracts * contractSize;
            if (size == 0.0 && !allowZero) continue;
            if (size <= 0.0) out.remove(price);
            else out.put(price, size);
        }
    }

    private static double parseDouble(String v) {
        try {
            return Double.parseDouble(v);
        } catch (Exception e) {
            return 0.0;
        }
    }

    private static List<OrderBookLevel> toLevels(NavigableMap<Double, Double> side, int depth) {
        List<OrderBookLevel> out = new ArrayList<>();
        int i = 0;
        for (Map.Entry<Double, Double> e : side.entrySet()) {
            out.add(new OrderBookLevel(e.getKey(), e.getValue()));
            if (++i >= depth) break;
        }
        return out;
    }
}
