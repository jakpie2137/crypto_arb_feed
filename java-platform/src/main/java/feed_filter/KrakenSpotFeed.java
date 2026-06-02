package feed_filter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.*;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Kraken Spot WS v2 orderbook feed (multi-symbol, single socket).
 *
 * Workaround/testing setup:
 *  - FeedFilterApp defines canonical hedge symbols quoted in USDT (e.g. BTCUSDT).
 *  - Kraken has EUR/USD quoted pairs; this feed keeps an internal mapping:
 *        native (e.g. XBT/EUR, ETH/USD) -> hedge (e.g. BTCUSDT, ETHUSDT)
 *  - We convert prices into USDT using FxRates:
 *        USD -> USD/USDT
 *        EUR -> EUR/USD * USD/USDT
 *
 * Resilience:
 *  - Global watchdog: if NO WS message for KRAKEN_WATCHDOG_MS -> reconnect socket.
 *  - Per-symbol stale: only after 30s without book updates for that symbol:
 *        REST verify top-of-book; only if REST shows movement while WS is silent -> resubscribe that symbol.
 *  - Avoid reconnect storms (single socket for all symbols).
 */
public class KrakenSpotFeed implements MarketDataFeed {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Example mappings (edit/extend as needed). */
    private static final LinkedHashMap<String, String> WS_TO_HEDGE = new LinkedHashMap<>();
    static {
        WS_TO_HEDGE.put("BNB/USD",  "BNBUSDT");
        WS_TO_HEDGE.put("DOGE/USD", "DOGEUSDT");
        WS_TO_HEDGE.put("LINK/USD", "LINKUSDT");
    }

    private static final class BookState {
        final String wsSymbol;
        final String hedge;
        final String restPair;
        final String quoteCcy;

        final NavigableMap<Double, Double> bids = new TreeMap<>(Comparator.reverseOrder());
        final NavigableMap<Double, Double> asks = new TreeMap<>();

        final AtomicLong lastBookUpdateTs = new AtomicLong(0);
        final AtomicLong lastRestCheckTs  = new AtomicLong(0);

        volatile double lastBestBid = Double.NaN; // native units
        volatile double lastBestAsk = Double.NaN;

        BookState(String wsSymbol, String hedge) {
            this.wsSymbol = wsSymbol;
            this.hedge = hedge;
            String[] p = wsSymbol.split("/");
            this.quoteCcy = (p.length == 2 ? p[1].trim().toUpperCase() : "USD");
            this.restPair = deriveRestPair(wsSymbol);
        }
    }

    private final Map<String, BookState> books = new ConcurrentHashMap<>();
    private final OrderBookStore store;

    private final OkHttpClient http;
    private final OkHttpClient wsClient;
    private volatile WebSocket ws;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ScheduledExecutorService ses;

    // --- reconnect control (prevents storms) ---
    private final AtomicBoolean reconnectScheduled = new AtomicBoolean(false);
    private volatile int reconnectAttempts = 0;
    private volatile long reconnectCooldownUntilMs = 0;
    private volatile long lastReconnectAttemptMs = 0;

    private static final long RECONNECT_BACKOFF_BASE_MS = 500;
    private static final long RECONNECT_BACKOFF_MAX_MS  = 60_000;
    private static final long RECONNECT_MIN_INTERVAL_MS = 5_000;

    private final AtomicLong lastAnyMsgTs = new AtomicLong(0);

    private final long globalWatchdogMs =
            Long.parseLong(System.getenv().getOrDefault("KRAKEN_WATCHDOG_MS", "45000"));
    private final long perBookStaleMs =
            Long.parseLong(System.getenv().getOrDefault("KRAKEN_BOOK_STALE_MS", "30000")); // 30s
    private final long restVerifyMinIntervalMs =
            Long.parseLong(System.getenv().getOrDefault("KRAKEN_REST_VERIFY_MIN_MS", "10000")); // 10s

    private final int depth =
            Integer.parseInt(System.getenv().getOrDefault("KRAKEN_DEPTH", "50"));

    public KrakenSpotFeed(OrderBookStore store) {
        this.store = store;

        for (Map.Entry<String, String> e : WS_TO_HEDGE.entrySet()) {
            books.put(e.getKey(), new BookState(e.getKey(), e.getValue()));
        }

        this.http = new OkHttpClient.Builder()
                .callTimeout(Duration.ofSeconds(10))
                .connectTimeout(Duration.ofSeconds(10))
                .readTimeout(Duration.ofSeconds(10))
                .build();

        this.wsClient = new OkHttpClient.Builder()
                .pingInterval(Duration.ofSeconds(20))
                .retryOnConnectionFailure(true)
                .build();

        this.ses = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "kraken-spot");
            t.setDaemon(true);
            return t;
        });
    }

    @Override
    public void start() {
        if (!running.compareAndSet(false, true)) return;
        connect();
        ses.scheduleAtFixedRate(this::watchdog, 1, 1, TimeUnit.SECONDS);
    }

    @Override
    public void stop() {
        running.set(false);
        tryClose("stop");
        ses.shutdownNow();
    }

    private void connect() {
        long now = System.currentTimeMillis();
        if (now < reconnectCooldownUntilMs) return;

        String url = System.getenv().getOrDefault("KRAKEN_WS_URL", "wss://ws.kraken.com/v2");
        Request req = new Request.Builder().url(url).build();

        lastAnyMsgTs.set(System.currentTimeMillis());
        ws = wsClient.newWebSocket(req, new WebSocketListener() {

            @Override
            public void onOpen(WebSocket webSocket, Response response) {
                lastAnyMsgTs.set(System.currentTimeMillis());
                reconnectAttempts = 0;
                System.out.println("[KRAKEN SPOT] connected, subscribing symbols=" + WS_TO_HEDGE.keySet());

                String symbolsJson = String.join("\",\"", WS_TO_HEDGE.keySet());
                String sub = "{"
                        + "\"method\":\"subscribe\","
                        + "\"params\":{"
                        + "\"channel\":\"book\","
                        + "\"symbol\":[\"" + symbolsJson + "\"],"
                        + "\"depth\":" + Math.max(10, depth) + ","
                        + "\"snapshot\":true"
                        + "}"
                        + "}";
                webSocket.send(sub);
            }

            @Override
            public void onMessage(WebSocket webSocket, String text) {
                long now = System.currentTimeMillis();
                lastAnyMsgTs.set(now);

                try {
                    JsonNode root = MAPPER.readTree(text);

                    if (!"book".equals(root.path("channel").asText())) return;
                    JsonNode data = root.get("data");
                    if (data == null || !data.isArray()) return;

                    String type = root.path("type").asText(""); // snapshot | update

                    for (JsonNode d : data) {
                        String sym = d.path("symbol").asText();
                        BookState st = books.get(sym);
                        if (st == null) continue;

                        JsonNode bids = d.get("bids");
                        JsonNode asks = d.get("asks");

                        synchronized (st) {
                            if ("snapshot".equals(type)) {
                                st.bids.clear();
                                st.asks.clear();
                            }
                            applyDelta(bids, st.bids);
                            applyDelta(asks, st.asks);
                        }

                        st.lastBookUpdateTs.set(now);
                        publishSnapshot(st, now);
                    }

                } catch (Exception ignored) {
                    // parser errors will make symbol go stale, triggering REST verify
                }
            }

            @Override
            public void onFailure(WebSocket webSocket, Throwable t, Response response) {
                int code = (response != null ? response.code() : -1);
                String msg = (t == null ? "" : String.valueOf(t.getMessage())).toLowerCase();
                if (msg.contains("canceled") || msg.contains("cancelled") || msg.contains("socket closed")) return;

                if (running.get()) {
                    boolean is429 = (code == 429) || msg.contains("429");
                    if (is429) {
                        long cool = 5 * 60_000L;
                        reconnectCooldownUntilMs = Math.max(reconnectCooldownUntilMs, System.currentTimeMillis() + cool);
                        System.err.println("[KRAKEN SPOT] failure 429 -> cooldown " + cool + "ms");
                    } else {
                        System.err.println("[KRAKEN SPOT] failure: " + (t == null ? "null" : t.getMessage()) + " -> reconnect");
                    }
                    scheduleReconnect(is429 ? "onFailure:429" : "onFailure", is429);
                }
            }

            @Override
            public void onClosed(WebSocket webSocket, int code, String reason) {
                if (running.get()) {
                    System.err.println("[KRAKEN SPOT] closed code=" + code + " reason=" + reason + " -> reconnect");
                    scheduleReconnect("onClosed:" + code, false);
                }
            }
        });
    }

    private void scheduleReconnect(String why, boolean hardCooldown) {
        if (!running.get()) return;
        if (!reconnectScheduled.compareAndSet(false, true)) return;

        long now = System.currentTimeMillis();

        if (hardCooldown) {
            reconnectCooldownUntilMs = Math.max(reconnectCooldownUntilMs, now + 5 * 60_000L);
        }

        long delay;
        long cooldownWait = reconnectCooldownUntilMs - now;
        if (cooldownWait > 0) {
            delay = cooldownWait;
        } else {
            reconnectAttempts = Math.min(reconnectAttempts + 1, 10);
            long exp = 1L << reconnectAttempts; // 2..1024
            delay = Math.min(RECONNECT_BACKOFF_MAX_MS, RECONNECT_BACKOFF_BASE_MS * exp);
        }

        long minWait = (lastReconnectAttemptMs + RECONNECT_MIN_INTERVAL_MS) - now;
        if (minWait > 0) delay = Math.max(delay, minWait);

        delay += ThreadLocalRandom.current().nextLong(100, 400);

        ses.schedule(() -> {
            try {
                if (!running.get()) return;

                long now2 = System.currentTimeMillis();
                if (now2 < reconnectCooldownUntilMs) {
                    reconnectScheduled.set(false);
                    scheduleReconnect("cooldownWait:" + why, false);
                    return;
                }

                lastReconnectAttemptMs = now2;

                tryClose("reconnect:" + why);
                connect();
            } finally {
                reconnectScheduled.set(false);
            }
        }, delay, TimeUnit.MILLISECONDS);
    }

    private void tryClose(String why) {
        try {
            WebSocket s = ws;
            ws = null;
            if (s != null) s.close(1000, why);
        } catch (Exception ignored) {}
    }

    private void watchdog() {
        if (!running.get()) return;
        long now = System.currentTimeMillis();

        long anySilence = now - lastAnyMsgTs.get();
        if (anySilence > globalWatchdogMs) {
            System.err.println("[KRAKEN SPOT] GLOBAL silence " + anySilence + "ms -> reconnect");
            scheduleReconnect("global", false);
            return;
        }

        for (BookState st : books.values()) {
            long last = st.lastBookUpdateTs.get();
            if (last == 0) continue;
            long silence = now - last;

            if (silence > perBookStaleMs) {
                if (now - st.lastRestCheckTs.get() < restVerifyMinIntervalMs) continue;
                st.lastRestCheckTs.set(now);

                Best best;
                try {
                    best = fetchBestFromRest(st.restPair);
                } catch (Exception e) {
                    continue;
                }
                if (best == null) continue;

                boolean moved;
                synchronized (st) {
                    moved = (Double.isFinite(st.lastBestBid) && Double.isFinite(st.lastBestAsk))
                            && ((best.bid != st.lastBestBid) || (best.ask != st.lastBestAsk));
                }

                if (moved) {
                    System.err.println("[KRAKEN " + st.wsSymbol + "] stale " + silence + "ms, REST moved (bid=" + best.bid + " ask=" + best.ask + ") -> resubscribe symbol");
                    resubscribeSymbol(st.wsSymbol);
                }
            }
        }
    }

    private void resubscribeSymbol(String wsSymbol) {
        WebSocket s = ws;
        if (s == null) {
            scheduleReconnect("resubscribe:noWS", false);
            return;
        }

        String unsub = "{"
                + "\"method\":\"unsubscribe\","
                + "\"params\":{"
                + "\"channel\":\"book\","
                + "\"symbol\":[\"" + wsSymbol + "\"]"
                + "}"
                + "}";
        String sub = "{"
                + "\"method\":\"subscribe\","
                + "\"params\":{"
                + "\"channel\":\"book\","
                + "\"symbol\":[\"" + wsSymbol + "\"],"
                + "\"depth\":" + Math.max(10, depth) + ","
                + "\"snapshot\":true"
                + "}"
                + "}";

        s.send(unsub);
        s.send(sub);
    }

    private void applyDelta(JsonNode arr, NavigableMap<Double, Double> side) {
        if (arr == null || !arr.isArray()) return;
        for (JsonNode lvl : arr) {
            if (!lvl.isArray() || lvl.size() < 2) continue;
            double price = Double.parseDouble(lvl.get(0).asText("0"));
            double size  = Double.parseDouble(lvl.get(1).asText("0"));
            if (size == 0.0) side.remove(price);
            else side.put(price, size);
        }
        while (side.size() > depth * 5) {
            side.pollLastEntry();
        }
    }

    private void publishSnapshot(BookState st, long tsMs) {
        double factor = FxRates.toUsdtFactor(st.quoteCcy);
        if (!(factor > 0) || !Double.isFinite(factor)) return;

        List<OrderBookLevel> outBids = new ArrayList<>(depth);
        List<OrderBookLevel> outAsks = new ArrayList<>(depth);

        synchronized (st) {
            int i = 0;
            for (Map.Entry<Double, Double> e : st.bids.entrySet()) {
                if (i++ >= depth) break;
                outBids.add(new OrderBookLevel(e.getKey() * factor, e.getValue()));
            }
            i = 0;
            for (Map.Entry<Double, Double> e : st.asks.entrySet()) {
                if (i++ >= depth) break;
                outAsks.add(new OrderBookLevel(e.getKey() * factor, e.getValue()));
            }

            if (!outBids.isEmpty()) st.lastBestBid = outBids.get(0).getPrice() / factor;
            if (!outAsks.isEmpty()) st.lastBestAsk = outAsks.get(0).getPrice() / factor;
        }

        if (outBids.isEmpty() || outAsks.isEmpty()) return;

        store.update(new OrderBookSnapshot(
                Exchange.KRAKEN,
                st.hedge,
                tsMs,
                outBids,
                outAsks
        ));
    }

    private static final class Best {
        final double bid;
        final double ask;
        Best(double bid, double ask) { this.bid = bid; this.ask = ask; }
    }

    private Best fetchBestFromRest(String restPair) throws IOException {
        // Kraken REST Depth:
        // https://api.kraken.com/0/public/Depth?pair=XBTEUR&count=1
        String url = "https://api.kraken.com/0/public/Depth?pair=" +
                URLEncoder.encode(restPair, StandardCharsets.UTF_8) + "&count=1";
        Request req = new Request.Builder().url(url).get().build();
        try (Response resp = http.newCall(req).execute()) {
            if (!resp.isSuccessful() || resp.body() == null) throw new IOException("HTTP " + resp.code());
            JsonNode root = MAPPER.readTree(resp.body().string());
            JsonNode result = root.get("result");
            if (result == null || !result.fields().hasNext()) return null;
            JsonNode pairObj = result.fields().next().getValue();

            JsonNode bids = pairObj.get("bids");
            JsonNode asks = pairObj.get("asks");
            double bid = (bids != null && bids.isArray() && bids.size() > 0) ? Double.parseDouble(bids.get(0).get(0).asText("0")) : Double.NaN;
            double ask = (asks != null && asks.isArray() && asks.size() > 0) ? Double.parseDouble(asks.get(0).get(0).asText("0")) : Double.NaN;
            if (!(bid > 0) || !(ask > 0)) return null;
            return new Best(bid, ask);
        }
    }

    private static String deriveRestPair(String wsSymbol) {
        // best-effort: "XBT/EUR" -> "XBTEUR"
        return wsSymbol.replace("/", "").replace("-", "").toUpperCase();
    }
}
