package feed_filter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.*;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Bitvavo WS Market Data Pro orderbook feed.
 * Non-conflated L2, ~9x more messages than standard WS, startMdSeqNo/endMdSeqNo for sequence tracking.
 * Requires authentication: BITVAVO_API_KEY, BITVAVO_API_SECRET.
 * See: https://docs.bitvavo.com/docs/ws-market-data-pro-sync/
 */
public class BitvavoMarketDataProFeed implements MarketDataFeed {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String WS_URL = "wss://ws-mdpro.bitvavo.com/v2/";

    private final Map<String, String> marketToStoreSymbol;
    private final Map<String, MdProBookState> books = new ConcurrentHashMap<>();
    private final OkHttpClient wsClient;
    private volatile WebSocket ws;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ScheduledExecutorService ses;
    private final OrderBookStore store;

    private final int depth = Integer.parseInt(System.getenv().getOrDefault("BITVAVO_DEPTH", "50"));
    private final long periodicGetBookMs = Long.parseLong(System.getenv().getOrDefault("BITVAVO_PERIODIC_SNAPSHOT_MS", "60000"));
    private final AtomicLong lastAnyMsgTs = new AtomicLong(0);
    private volatile boolean authenticated = false;
    private final AtomicLong requestIdGen = new AtomicLong(1);
    private final Map<Long, String> pendingGetBook = new ConcurrentHashMap<>();

    private static final class MdProBookState {
        final String market;
        final String hedge;
        final NavigableMap<Double, Double> bids = new TreeMap<>(Comparator.reverseOrder());
        final NavigableMap<Double, Double> asks = new TreeMap<>();
        volatile long lastMdSeqNo = 0;
        final Queue<JsonNode> pendingEvents = new ConcurrentLinkedQueue<>();
        final AtomicLong lastBookUpdateTs = new AtomicLong(0);

        MdProBookState(String market, String hedge) {
            this.market = market;
            this.hedge = hedge;
        }
    }

    public BitvavoMarketDataProFeed(OrderBookStore store, Map<String, String> marketToStoreSymbol) {
        this.store = store;
        this.marketToStoreSymbol = new LinkedHashMap<>(marketToStoreSymbol);
        for (String market : marketToStoreSymbol.keySet()) {
            String hedge = marketToStoreSymbol.get(market);
            books.put(market, new MdProBookState(market, hedge));
        }
        this.wsClient = new OkHttpClient.Builder()
                .pingInterval(Duration.ofSeconds(20))
                .retryOnConnectionFailure(true)
                .build();
        this.ses = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "bitvavo-mdpro");
            t.setDaemon(true);
            return t;
        });
    }

    @Override
    public void start() {
        if (!running.compareAndSet(false, true)) return;
        connect();
        ses.scheduleAtFixedRate(this::watchdog, 1, 1, TimeUnit.SECONDS);
        if (periodicGetBookMs > 0) {
            ses.scheduleAtFixedRate(this::periodicGetBook, periodicGetBookMs, periodicGetBookMs, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void stop() {
        running.set(false);
        try { WebSocket s = ws; ws = null; if (s != null) s.close(1000, "stop"); } catch (Exception ignored) {}
        ses.shutdownNow();
    }

    private String getApiKey() {
        String v = router.SecretsLoader.loadSecret("bitvavo_feed_key");
        if (v != null && !v.isBlank()) return v.trim();
        v = System.getenv("BITVAVO_FEED_KEY");
        if (v != null && !v.isBlank()) return v.trim();
        return null;
    }

    private String getApiSecret() {
        String v = router.SecretsLoader.loadSecret("bitvavo_feed_secret");
        if (v != null && !v.isBlank()) return v.trim();
        v = System.getenv("BITVAVO_FEED_SECRET");
        if (v != null && !v.isBlank()) return v.trim();
        return null;
    }

    private String sign(long timestamp, String method, String path, String body) {
        String secret = getApiSecret();
        if (secret == null) return null;
        try {
            String payload = timestamp + method + path + (body != null ? body : "");
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            byte[] hash = mac.doFinal(payload.getBytes(StandardCharsets.UTF_8));
            return Base64.getEncoder().encodeToString(hash);
        } catch (Exception e) {
            System.err.println("[BITVAVO-MDPRO] Sign failed: " + e.getMessage());
            return null;
        }
    }

    private void connect() {
        String key = getApiKey();
        String secret = getApiSecret();
        if (key == null || secret == null) {
            System.err.println("[BITVAVO-MDPRO] BITVAVO_API_KEY and BITVAVO_API_SECRET required");
            return;
        }

        Request req = new Request.Builder().url(WS_URL).build();
        lastAnyMsgTs.set(System.currentTimeMillis());
        authenticated = false;

        ws = wsClient.newWebSocket(req, new WebSocketListener() {
            @Override
            public void onOpen(WebSocket webSocket, Response response) {
                lastAnyMsgTs.set(System.currentTimeMillis());
                long ts = System.currentTimeMillis();
                String sig = sign(ts, "GET", "/v2/websocket", null);
                if (sig == null) return;
                String auth = "{\"action\":\"authenticate\",\"key\":\"" + key + "\",\"signature\":\"" + sig + "\",\"timestamp\":" + ts + "}";
                webSocket.send(auth);
                System.out.println("[BITVAVO-MDPRO] Auth sent");
            }

            @Override
            public void onMessage(WebSocket webSocket, String text) {
                lastAnyMsgTs.set(System.currentTimeMillis());
                try {
                    JsonNode root = MAPPER.readTree(text);

                    if (root.has("action")) {
                        String action = root.path("action").asText();
                        if ("authenticate".equals(action)) {
                            if (root.has("error")) {
                                System.err.println("[BITVAVO-MDPRO] Auth failed: " + root.path("error").asText());
                                return;
                            }
                            authenticated = true;
                            System.out.println("[BITVAVO-MDPRO] Authenticated, subscribing");
                            String marketsJson = String.join("\",\"", books.keySet());
                            webSocket.send("{\"action\":\"subscribe\",\"channels\":[{\"name\":\"book\",\"markets\":[\"" + marketsJson + "\"]}]}");
                            ses.execute(() -> sendGetBookForAll(webSocket));
                            return;
                        }
                        if ("getBook".equals(action) && root.has("response")) {
                            handleGetBookResponse(root);
                            return;
                        }
                    }

                    if ("book".equals(root.path("event").asText())) {
                        handleBookEvent(root);
                    }
                } catch (Exception e) {
                    System.err.println("[BITVAVO-MDPRO] Parse error: " + e.getMessage());
                }
            }

            @Override
            public void onFailure(WebSocket webSocket, Throwable t, Response response) {
                if (running.get()) {
                    System.err.println("[BITVAVO-MDPRO] failure: " + (t != null ? t.getMessage() : "null"));
                    scheduleReconnect();
                }
            }

            @Override
            public void onClosed(WebSocket webSocket, int code, String reason) {
                if (running.get()) scheduleReconnect();
            }
        });
    }

    private void sendGetBookForAll(WebSocket ws) {
        for (String market : books.keySet()) {
            long rid = requestIdGen.incrementAndGet();
            pendingGetBook.put(rid, market);
            String req = "{\"action\":\"getBook\",\"requestId\":" + rid + ",\"market\":\"" + market + "\",\"depth\":" + depth + "}";
            ws.send(req);
        }
    }

    private void handleGetBookResponse(JsonNode root) {
        JsonNode resp = root.get("response");
        if (resp == null) return;
        String market = resp.path("market").asText();
        MdProBookState st = books.get(market);
        if (st == null) return;

        long mdSeqNo = resp.path("mdSeqNo").asLong(resp.path("nonce").asLong(0));
        synchronized (st) {
            st.bids.clear();
            st.asks.clear();
            BitvavoBookUtils.applyDelta(resp.get("bids"), st.bids, depth);
            BitvavoBookUtils.applyDelta(resp.get("asks"), st.asks, depth);
            st.lastMdSeqNo = mdSeqNo;
            for (JsonNode ev : st.pendingEvents) {
                long start = ev.path("startMdSeqNo").asLong(ev.path("nonce").asLong(0));
                long end = ev.path("endMdSeqNo").asLong(start);
                if (start <= st.lastMdSeqNo) continue;
                if (start != st.lastMdSeqNo + 1) break;
                st.lastMdSeqNo = end;
                BitvavoBookUtils.applyDelta(ev.get("bids"), st.bids, depth);
                BitvavoBookUtils.applyDelta(ev.get("asks"), st.asks, depth);
            }
            st.pendingEvents.clear();
        }
        st.lastBookUpdateTs.set(System.currentTimeMillis());
        publishSnapshot(st);
        pendingGetBook.remove(root.path("requestId").asLong(0));
        System.out.println("[BITVAVO-MDPRO] Snapshot applied " + market + " mdSeqNo=" + mdSeqNo);
    }

    private void handleBookEvent(JsonNode root) {
        String market = root.path("market").asText();
        MdProBookState st = books.get(market);
        if (st == null) return;

        long startMdSeqNo = root.path("startMdSeqNo").asLong(root.path("nonce").asLong(0));
        long endMdSeqNo = root.path("endMdSeqNo").asLong(startMdSeqNo);

        if (st.lastMdSeqNo == 0) {
            st.pendingEvents.add(root);
            return;
        }

        if (startMdSeqNo <= st.lastMdSeqNo) return;
        if (startMdSeqNo != st.lastMdSeqNo + 1) {
            System.err.println("[BITVAVO-MDPRO " + market + "] seq gap: last=" + st.lastMdSeqNo + " start=" + startMdSeqNo + " -> need resync");
            st.lastMdSeqNo = 0;
            st.pendingEvents.add(root);
            WebSocket s = ws;
            if (s != null) ses.execute(() -> sendGetBookForAll(s));
            return;
        }

        synchronized (st) {
            BitvavoBookUtils.applyDelta(root.get("bids"), st.bids, depth);
            BitvavoBookUtils.applyDelta(root.get("asks"), st.asks, depth);
            st.lastMdSeqNo = endMdSeqNo;
        }
        st.lastBookUpdateTs.set(System.currentTimeMillis());
        publishSnapshot(st);
    }

    private void publishSnapshot(MdProBookState st) {
        double factor = (marketToStoreSymbol != null) ? 1.0 : FxRates.toUsdtFactor("EUR");
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
        }
        if (outBids.isEmpty() || outAsks.isEmpty()) return;
        if (outBids.get(0).getPrice() >= outAsks.get(0).getPrice()) {
            System.err.println("[BITVAVO-MDPRO " + st.market + "] CROSSED before publish, skipping: bestBid="
                    + outBids.get(0).getPrice() + " bestAsk=" + outAsks.get(0).getPrice());
            return;
        }
        OrderBookSnapshot snap = new OrderBookSnapshot(Exchange.BITVAVO, st.hedge, System.currentTimeMillis(), outBids, outAsks);
        if (store instanceof InMemoryOrderBookStore) {
            ((InMemoryOrderBookStore) store).putSnapshot(snap);
        } else {
            store.update(snap);
        }
    }

    private void periodicGetBook() {
        if (!running.get() || !authenticated) return;
        WebSocket s = ws;
        if (s != null) sendGetBookForAll(s);
    }

    private void watchdog() {
        if (!running.get()) return;
        if (System.currentTimeMillis() - lastAnyMsgTs.get() > 45_000) {
            System.err.println("[BITVAVO-MDPRO] Silence -> reconnect");
            scheduleReconnect();
        }
    }

    private void scheduleReconnect() {
        ses.schedule(() -> {
            if (!running.get()) return;
            try { WebSocket s = ws; ws = null; if (s != null) s.close(1000, "reconnect"); } catch (Exception ignored) {}
            connect();
        }, 5, TimeUnit.SECONDS);
    }
}
