package feed_filter.exchanges.bybitfut;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import feed_filter.Exchange;
import feed_filter.FeedLogger;
import feed_filter.InMemoryOrderBookStore;
import feed_filter.OrderBookDepthConfig;
import feed_filter.OrderBookLevel;
import feed_filter.OrderBookSnapshot;
import feed_filter.OrderBookStore;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.WebSocket;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

/**
 * Bybit Futures WebSocket feed using V5 API.
 * Maintains local orderbook state from snapshot + delta updates.
 * Validates against REST snapshots periodically - reconnects only on drift.
 */
public class BybitFuturesBatchFeed {

    private static final String WS_URL = "wss://stream.bybit.com/v5/public/linear";
    private static final String REST_URL = "https://api.bybit.com/v5/market/orderbook";
    
    // Smart validation: check every 30s, reconnect only if drift detected
    private static final long SNAPSHOT_VALIDATION_INTERVAL_MS = 30_000; // 30 seconds
    private static final int DRIFT_CHECK_LEVELS = 5; // Compare top N levels
    private static final double DRIFT_PRICE_THRESHOLD = 0.0001; // 0.01% price difference = drift
    private static final double DRIFT_SIZE_THRESHOLD = 0.05; // 5% size difference = drift
    private static final int SYMBOLS_TO_VALIDATE_PER_CYCLE = 3; // Check multiple symbols per validation

    private final int shardId;
    private final List<BybitFuturesFeed.Sub> subs;
    private final OrderBookStore store;
    private final int targetDepth;

    // Local orderbook state: externalSymbol -> BookState
    private final Map<String, BookState> books = new ConcurrentHashMap<>();
    // Symbol mapping: externalSymbol -> internalSymbol
    private final Map<String, String> symbolMap = new HashMap<>();

    private final HttpClient client = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(15))
            .build();

    private final ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
    private final ObjectMapper mapper = new ObjectMapper();

    private volatile WebSocket ws;
    private volatile boolean running = false;
    private volatile ScheduledFuture<?> pingTask;
    private volatile ScheduledFuture<?> refreshTask;

    // Local book state with TreeMaps for sorted price levels
    private static class BookState {
        final String internalSymbol;
        final TreeMap<Double, Double> bids = new TreeMap<>(Collections.reverseOrder()); // highest first
        final TreeMap<Double, Double> asks = new TreeMap<>(); // lowest first
        volatile long lastTs = 0;
        volatile boolean initialized = false;

        BookState(String internalSymbol) {
            this.internalSymbol = internalSymbol;
        }
    }

    public BybitFuturesBatchFeed(int shardId,
                                 List<BybitFuturesFeed.Sub> subs,
                                 OrderBookStore store,
                                 java.util.function.Function<String, String> externalToInternal) {
        this.shardId = shardId;
        this.subs = subs;
        this.store = store;
        this.targetDepth = OrderBookDepthConfig.BYBIT_FUT_DEPTH;

        for (BybitFuturesFeed.Sub s : subs) {
            symbolMap.put(s.externalSymbol(), s.internalSymbol());
            books.put(s.externalSymbol(), new BookState(s.internalSymbol()));
        }
    }

    public void start() {
        running = true;
        long delay = (long) (Math.random() * 2000) + (shardId * 500L);
        exec.schedule(this::connect, delay, TimeUnit.MILLISECONDS);
        
        // Schedule smart validation - only reconnect if drift detected
        refreshTask = exec.scheduleAtFixedRate(
            this::validateAndReconnectIfNeeded,
            SNAPSHOT_VALIDATION_INTERVAL_MS + delay,
            SNAPSHOT_VALIDATION_INTERVAL_MS,
            TimeUnit.MILLISECONDS
        );
    }
    
    /**
     * Smart validation: fetch REST snapshots for multiple symbols, compare with local OB,
     * only reconnect if significant drift is detected.
     */
    private void validateAndReconnectIfNeeded() {
        if (!running || subs.isEmpty()) return;
        
        // Pick multiple random symbols from this shard to validate
        List<BybitFuturesFeed.Sub> toValidate = new ArrayList<>();
        List<BybitFuturesFeed.Sub> shuffled = new ArrayList<>(subs);
        Collections.shuffle(shuffled);
        for (int i = 0; i < Math.min(SYMBOLS_TO_VALIDATE_PER_CYCLE, shuffled.size()); i++) {
            toValidate.add(shuffled.get(i));
        }
        
        for (BybitFuturesFeed.Sub sub : toValidate) {
            String externalSymbol = sub.externalSymbol();
            BookState localBook = books.get(externalSymbol);
            
            if (localBook == null || !localBook.initialized) continue;
            
            try {
                // Fetch REST snapshot (doesn't disconnect WS!)
                RestSnapshot restSnapshot = fetchRestSnapshot(externalSymbol);
                if (restSnapshot == null) continue;
                
                // Compare top levels
                boolean driftDetected = checkDrift(localBook, restSnapshot);
                
                if (driftDetected) {
                    FeedLogger.warn("BYBITFUT", "S" + shardId + " Drift detected on " + externalSymbol + " - reconnecting");
                    if (ws != null) {
                        try {
                            ws.sendClose(1000, "drift");
                        } catch (Exception e) { }
                    }
                    return; // Reconnect will refresh all symbols, no need to check more
                }
            } catch (Exception e) {
                FeedLogger.error("BYBITFUT", "S" + shardId + " Validation error for " + externalSymbol + ": " + e.getMessage());
            }
        }
        // All validated symbols OK - no log needed
    }
    
    private static class RestSnapshot {
        final List<double[]> bids = new ArrayList<>(); // [price, size]
        final List<double[]> asks = new ArrayList<>();
    }
    
    private RestSnapshot fetchRestSnapshot(String symbol) {
        try {
            String url = REST_URL + "?category=linear&symbol=" + symbol + "&limit=" + DRIFT_CHECK_LEVELS;
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(5))
                    .GET()
                    .build();
            
            HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) return null;
            
            JsonNode root = mapper.readTree(resp.body());
            if (root.get("retCode").asInt() != 0) return null;
            
            JsonNode result = root.get("result");
            if (result == null) return null;
            
            RestSnapshot snap = new RestSnapshot();
            
            JsonNode bids = result.get("b");
            if (bids != null && bids.isArray()) {
                for (JsonNode b : bids) {
                    if (b.isArray() && b.size() >= 2) {
                        snap.bids.add(new double[]{
                            Double.parseDouble(b.get(0).asText()),
                            Double.parseDouble(b.get(1).asText())
                        });
                    }
                }
            }
            
            JsonNode asks = result.get("a");
            if (asks != null && asks.isArray()) {
                for (JsonNode a : asks) {
                    if (a.isArray() && a.size() >= 2) {
                        snap.asks.add(new double[]{
                            Double.parseDouble(a.get(0).asText()),
                            Double.parseDouble(a.get(1).asText())
                        });
                    }
                }
            }
            
            return snap;
        } catch (Exception e) {
            return null;
        }
    }
    
    private boolean checkDrift(BookState local, RestSnapshot rest) {
        synchronized (local) {
            // Check bids
            if (checkSideDrift(local.bids, rest.bids, true)) return true;
            // Check asks
            if (checkSideDrift(local.asks, rest.asks, false)) return true;
        }
        return false;
    }
    
    private boolean checkSideDrift(TreeMap<Double, Double> localSide, List<double[]> restSide, boolean isBid) {
        if (restSide.isEmpty()) return false;
        if (localSide.isEmpty()) return true; // Local empty but REST has data = drift
        
        // Get top N from local (already sorted correctly by TreeMap)
        List<Map.Entry<Double, Double>> localTop = new ArrayList<>();
        int count = 0;
        for (Map.Entry<Double, Double> e : localSide.entrySet()) {
            localTop.add(e);
            if (++count >= DRIFT_CHECK_LEVELS) break;
        }
        
        // Compare level by level
        int levelsToCheck = Math.min(localTop.size(), Math.min(restSide.size(), DRIFT_CHECK_LEVELS));
        for (int i = 0; i < levelsToCheck; i++) {
            double localPrice = localTop.get(i).getKey();
            double localSize = localTop.get(i).getValue();
            double restPrice = restSide.get(i)[0];
            double restSize = restSide.get(i)[1];
            
            // Check price drift (relative difference)
            double priceDiff = Math.abs(localPrice - restPrice) / restPrice;
            if (priceDiff > DRIFT_PRICE_THRESHOLD) {
                return true;
            }
            
            // Check size drift (relative difference) - only for top 2 levels
            if (i < 2 && restSize > 0) {
                double sizeDiff = Math.abs(localSize - restSize) / restSize;
                if (sizeDiff > DRIFT_SIZE_THRESHOLD) {
                    return true;
                }
            }
        }
        
        return false;
    }

    public void stop() {
        running = false;
        if (refreshTask != null) refreshTask.cancel(true);
        if (pingTask != null) pingTask.cancel(true);
        exec.shutdownNow();
        if (ws != null) ws.sendClose(1000, "bye");
    }

    private void connect() {
        if (!running) return;
        try {
            client.newWebSocketBuilder()
                    .buildAsync(URI.create(WS_URL), new WsListener())
                    .join();
        } catch (Exception e) {
            FeedLogger.error("BYBITFUT", "S" + shardId + " Connect fail: " + e.getMessage());
            scheduleReconnect();
        }
    }

    private void scheduleReconnect() {
        if (!running) return;
        // Reset all book states on reconnect
        for (BookState book : books.values()) {
            synchronized (book) {
                book.bids.clear();
                book.asks.clear();
                book.initialized = false;
            }
        }
        exec.schedule(this::connect, 5, TimeUnit.SECONDS);
    }

    private class WsListener implements WebSocket.Listener {
        private final StringBuilder buffer = new StringBuilder();

        @Override
        public void onOpen(WebSocket webSocket) {
            ws = webSocket;
            FeedLogger.info("BYBITFUT", "S" + shardId + " Connected.");

            sendSubscribe(webSocket);

            if (pingTask != null) pingTask.cancel(true);
            pingTask = exec.scheduleAtFixedRate(() -> {
                try {
                    webSocket.sendText("{\"op\":\"ping\"}", true);
                } catch (Exception e) { }
            }, 20, 20, TimeUnit.SECONDS);

            webSocket.request(1);
        }

        private void sendSubscribe(WebSocket w) {
            // Use depth 50 for good balance of data and update frequency
            int apiDepth = 50;
            if (targetDepth <= 1) apiDepth = 1;
            else if (targetDepth <= 50) apiDepth = 50;
            else if (targetDepth <= 200) apiDepth = 200;
            else apiDepth = 500;

            StringBuilder args = new StringBuilder();
            int i = 0;
            for (BybitFuturesFeed.Sub s : subs) {
                if (i > 0) args.append(",");
                args.append("\"orderbook.").append(apiDepth).append(".").append(s.externalSymbol()).append("\"");
                i++;
            }

            String msg = "{\"op\":\"subscribe\",\"args\":[" + args + "]}";
            w.sendText(msg, true);
            FeedLogger.info("BYBITFUT", "S" + shardId + " Subscribed to " + subs.size() + " orderbooks (depth=" + apiDepth + ")");
        }

        @Override
        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            webSocket.request(1);
            buffer.append(data);
            if (!last) return null;

            String json = buffer.toString();
            buffer.setLength(0);

            try {
                processMessage(json);
            } catch (Exception e) {
                FeedLogger.error("BYBITFUT", "S" + shardId + " Parse error: " + e.getMessage());
            }
            return null;
        }

        @Override
        public void onError(WebSocket webSocket, Throwable error) {
            FeedLogger.error("BYBITFUT", "S" + shardId + " WS Error: " + error.getMessage());
            ws = null;
            scheduleReconnect();
        }

        @Override
        public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
            FeedLogger.info("BYBITFUT", "S" + shardId + " Closed: " + reason);
            ws = null;
            scheduleReconnect();
            return null;
        }
    }

    private void processMessage(String json) throws Exception {
        if (json.contains("\"op\":\"pong\"")) return;

        JsonNode root = mapper.readTree(json);

        // Handle subscription response
        if (root.has("success")) {
            if (!root.get("success").asBoolean()) {
                String msg = root.has("ret_msg") ? root.get("ret_msg").asText() : "error";
                FeedLogger.error("BYBITFUT", "S" + shardId + " Subscription failed: " + msg);
            }
            return;
        }

        if (!root.has("topic") || !root.has("data") || !root.has("type")) return;

        String topic = root.get("topic").asText();
        String type = root.get("type").asText();
        
        // Extract symbol from topic: "orderbook.50.BTCUSDT" -> "BTCUSDT"
        int lastDot = topic.lastIndexOf('.');
        if (lastDot < 0) return;
        String externalSymbol = topic.substring(lastDot + 1);

        BookState book = books.get(externalSymbol);
        if (book == null) return;

        // Get data object (Bybit V5 sends data as object, not array)
        JsonNode dataNode = root.get("data");
        if (dataNode == null) return;
        
        // Handle both object and array formats
        JsonNode data = dataNode.isArray() && dataNode.size() > 0 ? dataNode.get(0) : dataNode;
        if (!data.isObject()) return;

        long ts = root.has("ts") ? root.get("ts").asLong() : System.currentTimeMillis();

        synchronized (book) {
            if ("snapshot".equals(type)) {
                // Full snapshot - clear and populate
                book.bids.clear();
                book.asks.clear();
                applyLevels(data.get("b"), book.bids);
                applyLevels(data.get("a"), book.asks);
                book.initialized = true;
                book.lastTs = ts;
            } else if ("delta".equals(type)) {
                // Delta update - only apply if we have initial snapshot
                if (!book.initialized) return;
                applyDeltas(data.get("b"), book.bids);
                applyDeltas(data.get("a"), book.asks);
                book.lastTs = ts;
            }

            // Only output if initialized and has data
            if (!book.initialized || (book.bids.isEmpty() && book.asks.isEmpty())) return;

            // Create and store full snapshot from current state
            OrderBookSnapshot snap = createSnapshot(book);
            
            if (store instanceof InMemoryOrderBookStore) {
                ((InMemoryOrderBookStore) store).putSnapshot(snap);
            } else {
                store.update(snap);
            }
        }
    }

    private void applyLevels(JsonNode arr, TreeMap<Double, Double> side) {
        if (arr == null || !arr.isArray()) return;
        for (JsonNode e : arr) {
            if (!e.isArray() || e.size() < 2) continue;
            try {
                double price = Double.parseDouble(e.get(0).asText());
                double size = Double.parseDouble(e.get(1).asText());
                if (price > 0 && size > 0) {
                    side.put(price, size);
                }
            } catch (NumberFormatException ex) { }
        }
    }

    private void applyDeltas(JsonNode arr, TreeMap<Double, Double> side) {
        if (arr == null || !arr.isArray()) return;
        for (JsonNode e : arr) {
            if (!e.isArray() || e.size() < 2) continue;
            try {
                double price = Double.parseDouble(e.get(0).asText());
                double size = Double.parseDouble(e.get(1).asText());
                if (price <= 0) continue;
                if (size == 0) {
                    side.remove(price);
                } else {
                    side.put(price, size);
                }
            } catch (NumberFormatException ex) { }
        }
    }

    private OrderBookSnapshot createSnapshot(BookState book) {
        List<OrderBookLevel> bidLevels = new ArrayList<>(Math.min(book.bids.size(), targetDepth));
        List<OrderBookLevel> askLevels = new ArrayList<>(Math.min(book.asks.size(), targetDepth));

        int count = 0;
        for (Map.Entry<Double, Double> e : book.bids.entrySet()) {
            bidLevels.add(new OrderBookLevel(e.getKey(), e.getValue()));
            if (++count >= targetDepth) break;
        }

        count = 0;
        for (Map.Entry<Double, Double> e : book.asks.entrySet()) {
            askLevels.add(new OrderBookLevel(e.getKey(), e.getValue()));
            if (++count >= targetDepth) break;
        }

        return new OrderBookSnapshot(
                Exchange.BYBITFUT,
                book.internalSymbol,
                book.lastTs,
                bidLevels,
                askLevels
        );
    }
}
