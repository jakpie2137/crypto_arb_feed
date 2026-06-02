package feed_filter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.*;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Bitvavo Spot WS orderbook feed (multi-market, single socket).
 *
 * Two modes:
 *  1) With marketToStoreSymbol map (from FeedFilterApp): publishes NATIVE EUR prices.
 *     Cross conversion is done in MM via cross1. Store symbol = native pair (e.g. BTC-EUR).
 *  2) Without map (legacy): uses hardcoded MARKET_TO_HEDGE, converts via FxRates to USDT.
 *
 * Initial sync: On connect, fetches full order book from REST per market, applies as base state,
 * then applies buffered WS deltas with nonce &gt; snapshot.nonce. Prevents crossed OB from delta-only start.
 *
 * Resilience:
 *  - Global watchdog: if NO WS message for BITVAVO_WATCHDOG_MS -> reconnect socket.
 *  - Per-market stale: only after 30s without book updates for that market:
 *        REST verify top-of-book; only if REST shows movement while WS is silent -> resubscribe that market.
 *        (resubscribe is preferred over reconnecting whole socket)
 *  - onFailure due to our close/cancel is ignored (no fake failures).
 */
public class BitvavoSpotFeed implements MarketDataFeed {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Example mappings (edit/extend as needed) – used when no map provided. */
    private static final LinkedHashMap<String, String> MARKET_TO_HEDGE = new LinkedHashMap<>();
    static {
        MARKET_TO_HEDGE.put("BNB-EUR",  "BNBUSDT");
        MARKET_TO_HEDGE.put("DOGE-EUR", "DOGEUSDT");
        MARKET_TO_HEDGE.put("LINK-EUR", "LINKUSDT");
    }

    /** When set: publish native EUR (no FxRates). Key = Bitvavo market (e.g. BTC-EUR), value = store symbol. */
    private final Map<String, String> marketToStoreSymbol;

    private static final class BookState {
        final String market;
        final String hedge;
        final String quoteCcy;

        final NavigableMap<Double, Double> bids = new TreeMap<>(Comparator.reverseOrder());
        final NavigableMap<Double, Double> asks = new TreeMap<>();

        volatile long lastNonce = 0;

        // gap tracking: when nonce gap is detected, we stop applying deltas & publishing for this market.
        volatile long gapDetectedAtMs = 0;
        volatile long nextGapResyncAtMs = 0;
        volatile long lastGapLogAtMs = 0;
        volatile long lastCrossedResubscribeAtMs = 0;

        final AtomicLong lastBookUpdateTs = new AtomicLong(0);
        final AtomicLong lastRestCheckTs  = new AtomicLong(0);

        volatile double lastBestBid = Double.NaN; // native units (not converted)
        volatile double lastBestAsk = Double.NaN;

        /** Events received before initial snapshot - apply after snapshot with nonce > snapshot.nonce */
        final Queue<BufferedBookEvent> pendingEvents = new ConcurrentLinkedQueue<>();

        BookState(String market, String hedge) {
            this.market = market;
            this.hedge = hedge;
            String[] p = market.split("-");
            this.quoteCcy = (p.length == 2 ? p[1].trim().toUpperCase() : "EUR");
        }
    }

    private final Map<String, BookState> books = new ConcurrentHashMap<>();

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
            Long.parseLong(System.getenv().getOrDefault("BITVAVO_WATCHDOG_MS", "45000"));
    private final long perBookStaleMs =
            Long.parseLong(System.getenv().getOrDefault("BITVAVO_BOOK_STALE_MS", "30000")); // 30s
    private final long restVerifyMinIntervalMs =
            Long.parseLong(System.getenv().getOrDefault("BITVAVO_REST_VERIFY_MIN_MS", "10000")); // 10s

    private final int depth =
            Integer.parseInt(System.getenv().getOrDefault("BITVAVO_DEPTH", "50"));
    private final long periodicSnapshotIntervalMs =
            Long.parseLong(System.getenv().getOrDefault("BITVAVO_PERIODIC_SNAPSHOT_MS", "45000")); // 45s (zagęszczone vs 60s)
    /** Delay between REST calls in periodicSnapshot (avoid rate limits). */
    private final long restCallDelayMs =
            Long.parseLong(System.getenv().getOrDefault("BITVAVO_REST_CALL_DELAY_MS", "50"));

    private final OrderBookStore store;

    private final int shardId;

    /** Legacy: hardcoded markets, FxRates conversion. */
    public BitvavoSpotFeed(OrderBookStore store) {
        this(store, null, -1);
    }

    /**
     * Preferred: use symbol mapping from config + SymbolRegistry.
     * Publishes native EUR (no FxRates). Map: Bitvavo market (e.g. BTC-EUR) -> store symbol.
     */
    public BitvavoSpotFeed(OrderBookStore store, Map<String, String> marketToStoreSymbol) {
        this(store, marketToStoreSymbol, -1);
    }

    /**
     * With shardId for batch feed – log prefix [BITVAVO G2] so ops see which shard restarted.
     */
    public BitvavoSpotFeed(OrderBookStore store, Map<String, String> marketToStoreSymbol, int shardId) {
        this.shardId = shardId;
        this.store = store;
        this.marketToStoreSymbol = (marketToStoreSymbol != null && !marketToStoreSymbol.isEmpty())
                ? new LinkedHashMap<>(marketToStoreSymbol) : null;

        Map<String, String> toUse = this.marketToStoreSymbol != null ? this.marketToStoreSymbol : MARKET_TO_HEDGE;
        for (Map.Entry<String, String> e : toUse.entrySet()) {
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
            Thread t = new Thread(r, shardId >= 0 ? "bitvavo-spot-G" + shardId : "bitvavo-spot");
            t.setDaemon(true);
            return t;
        });
    }

    private String logPrefix() {
        return shardId >= 0 ? "[BITVAVO G" + shardId + "]" : "[BITVAVO]";
    }

    @Override
    public void start() {
        if (!running.compareAndSet(false, true)) return;
        connect();
        ses.scheduleAtFixedRate(this::watchdog, 1, 1, TimeUnit.SECONDS);
        if (periodicSnapshotIntervalMs > 0) {
            // Stagger start per shard (np. 4 shardy: 0s, 3s, 6s, 9s) – unika jednoczesnego REST burst
            long initialDelayMs = periodicSnapshotIntervalMs + (shardId >= 0 ? shardId * 3000L : 0);
            ses.scheduleAtFixedRate(this::periodicSnapshot, initialDelayMs, periodicSnapshotIntervalMs, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void stop() {
        running.set(false);
        tryClose("stop");
        ses.shutdownNow();
    }

    private void connect() {
        String url = System.getenv().getOrDefault("BITVAVO_WS_URL", "wss://ws.bitvavo.com/v2/");
        Request req = new Request.Builder().url(url).build();
        lastAnyMsgTs.set(System.currentTimeMillis());

        ws = wsClient.newWebSocket(req, new WebSocketListener() {

            @Override
            public void onOpen(WebSocket webSocket, Response response) {
                lastAnyMsgTs.set(System.currentTimeMillis());
                reconnectAttempts = 0;
                System.out.println(logPrefix() + " connected, subscribing markets=" + books.keySet());

                // Reset state on connect so we init from snapshot (each connect = fresh start)
                for (BookState st : books.values()) {
                    synchronized (st) {
                        st.bids.clear();
                        st.asks.clear();
                        st.pendingEvents.clear();
                        st.lastNonce = 0;
                        st.gapDetectedAtMs = 0;
                        st.nextGapResyncAtMs = 0;
                        st.lastCrossedResubscribeAtMs = 0;
                    }
                }

                String marketsJson = String.join("\",\"", books.keySet());
                String sub = "{"
                        + "\"action\":\"subscribe\","
                        + "\"channels\":[{\"name\":\"book\",\"markets\":[\"" + marketsJson + "\"]}]"
                        + "}";
                webSocket.send(sub);

                // Fetch initial snapshot per market (REST) – run async to not block WS
                ses.execute(() -> fetchAndApplyInitialSnapshots());
            }

            @Override
            public void onMessage(WebSocket webSocket, String text) {
                long now = System.currentTimeMillis();
                lastAnyMsgTs.set(now);

                try {
                    JsonNode root = MAPPER.readTree(text);

                    // example:
                    // { "event":"book","market":"BTC-EUR","nonce":123,"bids":[["p","s"]], "asks":[...] }
                    if (!"book".equals(root.path("event").asText())) return;

                    String market = root.path("market").asText();
                    BookState st = books.get(market);
                    if (st == null) return;

                    long nonce = root.path("nonce").asLong(0);

                    // Before initial snapshot: buffer events; they will be applied after snapshot
                    if (st.lastNonce == 0) {
                        st.pendingEvents.add(new BufferedBookEvent(nonce, root));
                        return;
                    }

                    if (nonce != 0 && st.lastNonce != 0) {
                        if (nonce <= st.lastNonce) {
                            return; // ignore out-of-order / duplicates
                        }
                        // Nonce gap -> do NOT apply deltas (book would become inconsistent).
                        // We intentionally stop publishing updates for this market; watchdog will REST-verify after 30s stale.
                        if (nonce != (st.lastNonce + 1)) {
                            if (st.gapDetectedAtMs == 0) st.gapDetectedAtMs = now;
                            st.nextGapResyncAtMs = Math.max(st.nextGapResyncAtMs, st.gapDetectedAtMs + perBookStaleMs);

                            if (now - st.lastGapLogAtMs > 5000) { // log at most every 5s
                                st.lastGapLogAtMs = now;
                                System.err.println(logPrefix() + " " + st.market + " nonce gap: last=" + st.lastNonce + " got=" + nonce
                                        + " -> wait " + perBookStaleMs + "ms then REST verify");
                            }
                            // keep lastNonce? update to received nonce so we don't spam gaps forever on the same value
                            st.lastNonce = nonce;
                            return;
                        }
                    }
                    if (nonce != 0) st.lastNonce = nonce;

                    synchronized (st) {
                        applyDelta(root.get("bids"), st.bids);
                        applyDelta(root.get("asks"), st.asks);
                    }

                    st.lastBookUpdateTs.set(now);
                    publishSnapshot(st, now);

                } catch (Exception e) {
                    System.err.println(logPrefix() + " onMessage parse/apply error: " + e.getMessage() + " (msg len=" + (text != null ? text.length() : 0) + ")");
                }
            }

            @Override
            public void onFailure(WebSocket webSocket, Throwable t, Response response) {
                String msg = (t == null ? "" : String.valueOf(t.getMessage())).toLowerCase();
                if (msg.contains("canceled") || msg.contains("cancelled") || msg.contains("socket closed")) return;

                if (running.get()) {
                    System.err.println(logPrefix() + " failure: " + (t == null ? "null" : t.getMessage()) + " -> reconnect");
                    scheduleReconnect("onFailure", false);
                }
            }

            @Override
            public void onClosed(WebSocket webSocket, int code, String reason) {
                if (running.get()) {
                    System.err.println(logPrefix() + " closed code=" + code + " reason=" + reason + " -> reconnect");
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

    /** Periodic full snapshot from REST – resync to fix any drift from missed removes.
     *  restCallDelayMs between markets to avoid Bitvavo rate limits (4 shards × ~50 markets). */
    private void periodicSnapshot() {
        if (!running.get()) return;
        for (BookState st : books.values()) {
            if (!running.get()) return;
            try {
                if (restCallDelayMs > 0) {
                    Thread.sleep(restCallDelayMs);
                }
                JsonNode snapshot = fetchFullBookFromRest(st.market);
                if (snapshot == null) continue;
                long snapNonce = snapshot.path("nonce").asLong(0);
                synchronized (st) {
                    st.bids.clear();
                    st.asks.clear();
                    applyDelta(snapshot.get("bids"), st.bids);
                    applyDelta(snapshot.get("asks"), st.asks);
                    st.lastNonce = snapNonce;
                }
                st.lastBookUpdateTs.set(System.currentTimeMillis());
                publishSnapshot(st, System.currentTimeMillis());
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                System.err.println(logPrefix() + " " + st.market + " periodicSnapshot failed: " + e.getMessage());
            }
        }
    }

    private void watchdog() {
        if (!running.get()) return;
        long now = System.currentTimeMillis();

        long anySilence = now - lastAnyMsgTs.get();
        if (anySilence > globalWatchdogMs) {
            System.err.println(logPrefix() + " GLOBAL silence " + anySilence + "ms -> reconnect (this shard only)");
            scheduleReconnect("global", false);
            return;
        }

        // per market stale -> REST verify -> resubscribe only that market
        for (BookState st : books.values()) {
            long last = st.lastBookUpdateTs.get();
            if (last == 0) continue; // not yet initialized
            long silence = now - last;

            // Gap handling: only act after gap has been "old" (>= perBookStaleMs)
            if (st.gapDetectedAtMs != 0 && now >= st.nextGapResyncAtMs) {
                if (now - st.lastRestCheckTs.get() >= restVerifyMinIntervalMs) {
                    st.lastRestCheckTs.set(now);
                    try {
                        Best best = fetchBestFromRest(st.market);
                        if (best != null) {
                            boolean moved = !(Double.isFinite(st.lastBestBid) && Double.isFinite(st.lastBestAsk))
                                    || (best.bid != st.lastBestBid) || (best.ask != st.lastBestAsk);
                            if (moved) {
                                System.err.println(logPrefix() + " " + st.market + " nonce-gap old, REST moved -> resubscribe market");
                                st.gapDetectedAtMs = 0;
                                st.nextGapResyncAtMs = 0;
                                resubscribeMarket(st.market);
                            } else {
                                // market didn't move; clear gap flags to avoid spamming
                                st.gapDetectedAtMs = 0;
                                st.nextGapResyncAtMs = 0;
                            }
                        }
                    } catch (Exception ignore) {
                        // REST failing is not a reason to storm; retry next tick
                    }
                }
            }

            if (silence > perBookStaleMs) {
                if (now - st.lastRestCheckTs.get() < restVerifyMinIntervalMs) continue;
                st.lastRestCheckTs.set(now);

                Best best;
                try {
                    best = fetchBestFromRest(st.market);
                } catch (Exception e) {
                    continue; // let global watchdog handle truly dead sockets
                }
                if (best == null) continue;

                boolean moved;
                synchronized (st) {
                    moved = (Double.isFinite(st.lastBestBid) && Double.isFinite(st.lastBestAsk))
                            && ((best.bid != st.lastBestBid) || (best.ask != st.lastBestAsk));
                }

                if (moved) {
                    System.err.println(logPrefix() + " " + st.market + " stale " + silence + "ms, REST moved -> resubscribe market");
                    resubscribeMarket(st.market);
                }
            }
        }
    }

    private void resubscribeMarket(String market) {
        WebSocket s = ws;
        if (s == null) {
            scheduleReconnect("resubscribe:noWS", false);
            return;
        }

        BookState st = books.get(market);
        if (st != null) {
            // Reset state so we re-init from snapshot (buffer incoming until snapshot applied)
            synchronized (st) {
                st.bids.clear();
                st.asks.clear();
                st.pendingEvents.clear();
                st.lastNonce = 0;
                st.gapDetectedAtMs = 0;
                st.nextGapResyncAtMs = 0;
                st.lastCrossedResubscribeAtMs = 0;
            }
            // Fetch fresh snapshot and re-init
            ses.execute(() -> {
                if (!running.get() || st.lastNonce != 0) return; // already inited by another path
                try {
                    JsonNode snapshot = fetchFullBookFromRest(market);
                    if (snapshot == null) return;
                    long snapshotNonce = snapshot.path("nonce").asLong(0);
                    synchronized (st) {
                        if (st.lastNonce != 0) return; // already inited
                        st.bids.clear();
                        st.asks.clear();
                        applyDelta(snapshot.get("bids"), st.bids);
                        applyDelta(snapshot.get("asks"), st.asks);
                        st.lastNonce = snapshotNonce;
                        for (BufferedBookEvent ev : st.pendingEvents) {
                            if (ev.nonce <= snapshotNonce) continue;
                            if (ev.nonce != st.lastNonce + 1) break;
                            st.lastNonce = ev.nonce;
                            applyDelta(ev.root.get("bids"), st.bids);
                            applyDelta(ev.root.get("asks"), st.asks);
                        }
                        st.pendingEvents.clear();
                    }
                    st.lastBookUpdateTs.set(System.currentTimeMillis());
                    publishSnapshot(st, System.currentTimeMillis());
                    System.out.println(logPrefix() + " Resubscribe snapshot applied for " + market + " nonce=" + snapshotNonce);
                } catch (Exception e) {
                    System.err.println(logPrefix() + " " + market + " Resubscribe snapshot failed: " + e.getMessage());
                }
            });
        }

        String unsub = "{"
                + "\"action\":\"unsubscribe\","
                + "\"channels\":[{\"name\":\"book\",\"markets\":[\"" + market + "\"]}]"
                + "}";
        String sub = "{"
                + "\"action\":\"subscribe\","
                + "\"channels\":[{\"name\":\"book\",\"markets\":[\"" + market + "\"]}]"
                + "}";

        s.send(unsub);
        s.send(sub);
    }

    private void applyDelta(JsonNode arr, NavigableMap<Double, Double> side) {
        BitvavoBookUtils.applyDelta(arr, side, depth);
    }

    private void publishSnapshot(BookState st, long tsMs) {
        // Native EUR mode (from FeedFilterApp): no conversion. Cross applied in MM via cross1.
        double factor = (marketToStoreSymbol != null) ? 1.0 : FxRates.toUsdtFactor(st.quoteCcy);
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

            if (!outBids.isEmpty()) st.lastBestBid = st.bids.firstKey();
            if (!outAsks.isEmpty()) st.lastBestAsk = st.asks.firstKey();
        }

        if (outBids.isEmpty() || outAsks.isEmpty()) return;

        double bestBid = outBids.get(0).getPrice();
        double bestAsk = outAsks.get(0).getPrice();

        // Crossed book – publish for analysis, but restart stream (bad deltas)
        if (bestBid >= bestAsk) {
            System.err.println(logPrefix() + " " + st.market + " CROSSED: bestBid=" + bestBid + " bestAsk=" + bestAsk + " -> resubscribe");
            long now = System.currentTimeMillis();
            if (now - st.lastCrossedResubscribeAtMs > 30_000) {
                st.lastCrossedResubscribeAtMs = now;
                resubscribeMarket(st.market);
            }
        }

        OrderBookSnapshot snap = new OrderBookSnapshot(
                Exchange.BITVAVO,
                st.hedge,
                tsMs,
                outBids,
                outAsks
        );
        if (store instanceof InMemoryOrderBookStore) {
            ((InMemoryOrderBookStore) store).putSnapshot(snap);
        } else {
            store.update(snap);
        }
    }

    private static final class Best {
        final double bid;
        final double ask;
        Best(double bid, double ask) { this.bid = bid; this.ask = ask; }
    }

    /** Book event buffered until initial snapshot is applied. */
    private static final class BufferedBookEvent {
        final long nonce;
        final JsonNode root;
        BufferedBookEvent(long nonce, JsonNode root) { this.nonce = nonce; this.root = root; }
    }

    /**
     * Fetches full order book from REST, applies as initial snapshot, then applies buffered WS events
     * with nonce > snapshot.nonce. Per Bitvavo docs: snapshot first, then deltas.
     */
    private void fetchAndApplyInitialSnapshots() {
        for (BookState st : books.values()) {
            if (!running.get()) return;
            try {
                JsonNode snapshot = fetchFullBookFromRest(st.market);
                if (snapshot == null) continue;

                long snapshotNonce = snapshot.path("nonce").asLong(0);
                synchronized (st) {
                    st.bids.clear();
                    st.asks.clear();
                    applyDelta(snapshot.get("bids"), st.bids);
                    applyDelta(snapshot.get("asks"), st.asks);
                    st.lastNonce = snapshotNonce;

                    // Apply buffered events with nonce > snapshot.nonce (discard older)
                    for (BufferedBookEvent ev : st.pendingEvents) {
                        if (ev.nonce <= snapshotNonce) continue;
                        if (ev.nonce != st.lastNonce + 1) {
                            // Gap in buffer – stop applying to avoid inconsistent state
                            break;
                        }
                        st.lastNonce = ev.nonce;
                        applyDelta(ev.root.get("bids"), st.bids);
                        applyDelta(ev.root.get("asks"), st.asks);
                    }
                    st.pendingEvents.clear();
                }
                st.lastBookUpdateTs.set(System.currentTimeMillis());
                publishSnapshot(st, System.currentTimeMillis());
                System.out.println(logPrefix() + " Initial snapshot applied for " + st.market + " nonce=" + snapshotNonce);
            } catch (Exception e) {
                System.err.println(logPrefix() + " " + st.market + " Failed initial snapshot: " + e.getMessage());
            }
        }
    }

    private JsonNode fetchFullBookFromRest(String market) throws IOException {
        String url = "https://api.bitvavo.com/v2/" + market + "/book?depth=" + depth;
        Request req = new Request.Builder().url(url).get().build();
        try (Response resp = http.newCall(req).execute()) {
            if (!resp.isSuccessful() || resp.body() == null) throw new IOException("HTTP " + resp.code());
            return MAPPER.readTree(resp.body().string());
        }
    }

    private Best fetchBestFromRest(String market) throws IOException {
        // https://api.bitvavo.com/v2/{market}/book?depth=1
        String url = "https://api.bitvavo.com/v2/" + market + "/book?depth=1";
        Request req = new Request.Builder().url(url).get().build();
        try (Response resp = http.newCall(req).execute()) {
            if (!resp.isSuccessful() || resp.body() == null) throw new IOException("HTTP " + resp.code());
            JsonNode root = MAPPER.readTree(resp.body().string());

            JsonNode bids = root.get("bids");
            JsonNode asks = root.get("asks");

            double bid = Double.NaN;
            double ask = Double.NaN;

            if (bids != null && bids.isArray() && bids.size() > 0 && bids.get(0).isArray() && bids.get(0).size() > 0) {
                bid = Double.parseDouble(bids.get(0).get(0).asText("0"));
            }
            if (asks != null && asks.isArray() && asks.size() > 0 && asks.get(0).isArray() && asks.get(0).size() > 0) {
                ask = Double.parseDouble(asks.get(0).get(0).asText("0"));
            }
            if (!(bid > 0) || !(ask > 0)) return null;
            return new Best(bid, ask);
        }
    }
}
