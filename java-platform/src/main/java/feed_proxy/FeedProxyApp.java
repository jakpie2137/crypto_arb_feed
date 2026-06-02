package feed_proxy;

import com.google.protobuf.Empty;
import feed.FeedProxyServiceGrpc;
import feed.FeedServiceGrpc;
import feed.InstrumentKey;
import feed.Level;
import feed.OrderbookUpdate;
import feed.SubscriptionRequest;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

/**
 * Feed Proxy: subscribes to Feed (gRPC), fans out to MM clients (gRPC).
 * Pass-through only - no quotation, contract_size, or precision calculations.
 */
public class FeedProxyApp {

    private static final Set<String> FUNDING_RECEIVED_KEYS = ConcurrentHashMap.newKeySet();

    private static final int FEED_GRPC_PORT = Integer.parseInt(
            System.getenv().getOrDefault("FEED_GRPC_PORT", "50050"));
    private static final String FEED_HOST = System.getenv().getOrDefault("FEED_HOST", "localhost");
    private static final int PROXY_GRPC_PORT = Integer.parseInt(
            System.getenv().getOrDefault("FEED_PROXY_GRPC_PORT", "50051"));
    private static final int PROXY_HTTP_PORT = Integer.parseInt(
            System.getenv().getOrDefault("FEED_PROXY_HTTP_PORT", "8080"));

    private final String feedTarget;
    private final OrderBookBuffer buffer = new OrderBookBuffer();
    private ManagedChannel feedChannel;
    private Server proxyServer;
    private com.sun.net.httpserver.HttpServer httpServer;
    private final ConcurrentHashMap<String, SubscriberContext> subscribers = new ConcurrentHashMap<>();
    private volatile boolean feedConnected = false;
    private final ScheduledExecutorService reconnectScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "feed-proxy-reconnect");
        t.setDaemon(true);
        return t;
    });

    public FeedProxyApp(String feedTarget) {
        this.feedTarget = feedTarget;
    }

    public static void main(String[] args) {
        String feedTarget = FEED_HOST + ":" + FEED_GRPC_PORT;
        System.out.println("[FEED-PROXY] Starting. Feed target: " + feedTarget);

        FeedProxyApp app = new FeedProxyApp(feedTarget);

        try {
            app.startProxyServer();
            app.startHttpServer();
            app.connectToFeed();
            app.scheduleReconnect();

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("[FEED-PROXY] Shutdown...");
                app.shutdown();
            }, "feed-proxy-shutdown"));

            System.out.println("[FEED-PROXY] Running. gRPC:" + PROXY_GRPC_PORT + " HTTP:" + PROXY_HTTP_PORT + ". Ctrl+C to stop.");
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println("[FEED-PROXY] Interrupted");
        }
    }

    private void startProxyServer() throws RuntimeException {
        proxyServer = ServerBuilder.forPort(PROXY_GRPC_PORT)
                .addService(new FeedProxyServiceImpl())
                .build();
        try {
            proxyServer.start();
            System.out.println("[FEED-PROXY] gRPC server listening on port " + PROXY_GRPC_PORT);
        } catch (IOException e) {
            throw new RuntimeException("Failed to start gRPC server", e);
        }
    }

    private void startHttpServer() throws RuntimeException {
        try {
            httpServer = com.sun.net.httpserver.HttpServer.create(new java.net.InetSocketAddress(PROXY_HTTP_PORT), 0);
            httpServer.createContext("/prices", exchange -> {
                if (!"GET".equals(exchange.getRequestMethod())) {
                    exchange.sendResponseHeaders(405, 0);
                    exchange.close();
                    return;
                }
                try {
                    long now = System.currentTimeMillis();
                    Map<String, Map<String, Object>> prices = new java.util.HashMap<>();
                    for (Map.Entry<String, OrderbookUpdate> e : buffer.snapshot().entrySet()) {
                        OrderbookUpdate u = e.getValue();
                        String ex = u.getExchange() != null ? u.getExchange().toUpperCase() : "";
                        String sym = u.getSymbol() != null ? u.getSymbol() : "";
                        if (ex.isEmpty() && sym.isEmpty()) continue;
                        double bestBid = (u.getBidsCount() > 0) ? u.getBids(0).getPrice() : 0.0;
                        double bestAsk = (u.getAsksCount() > 0) ? u.getAsks(0).getPrice() : 0.0;
                        double midPrice = (bestBid > 0 && bestAsk > 0) ? (bestBid + bestAsk) / 2.0
                                : (bestBid > 0 ? bestBid : bestAsk);
                        long ageMs = now - u.getTimestampMs();
                        Map<String, Object> symData = new java.util.HashMap<>();
                        symData.put("midPrice", midPrice);
                        symData.put("bestBid", bestBid);
                        symData.put("bestAsk", bestAsk);
                        symData.put("ageMs", ageMs);
                        symData.put("hasData", midPrice > 0);
                        prices.computeIfAbsent(ex, k -> new java.util.HashMap<>()).put(sym, symData);
                    }
                    StringBuilder json = new StringBuilder();
                    json.append("{\n");
                    boolean firstEx = true;
                    for (var exEntry : prices.entrySet()) {
                        if (!firstEx) json.append(",\n");
                        firstEx = false;
                        json.append("  \"").append(exEntry.getKey()).append("\": {\n");
                        boolean firstSym = true;
                        for (var symEntry : exEntry.getValue().entrySet()) {
                            if (!firstSym) json.append(",\n");
                            firstSym = false;
                            @SuppressWarnings("unchecked")
                            Map<String, Object> data = (Map<String, Object>) symEntry.getValue();
                            json.append("    \"").append(symEntry.getKey()).append("\": {\n");
                            json.append("      \"midPrice\": ").append(data.get("midPrice")).append(",\n");
                            json.append("      \"bestBid\": ").append(data.get("bestBid")).append(",\n");
                            json.append("      \"bestAsk\": ").append(data.get("bestAsk")).append(",\n");
                            json.append("      \"ageMs\": ").append(data.get("ageMs")).append(",\n");
                            json.append("      \"hasData\": ").append(data.get("hasData"));
                            json.append("\n    }");
                        }
                        json.append("\n  }");
                    }
                    json.append("\n}");
                    String body = json.toString();
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    exchange.sendResponseHeaders(200, body.length());
                    try (var os = exchange.getResponseBody()) { os.write(body.getBytes()); }
                } catch (Exception ex) {
                    System.err.println("[FEED-PROXY] /prices error: " + ex.getMessage());
                    exchange.sendResponseHeaders(500, 0);
                    exchange.close();
                }
            });
            httpServer.createContext("/orderbook", exchange -> {
                if (!"GET".equals(exchange.getRequestMethod())) {
                    exchange.sendResponseHeaders(405, 0);
                    exchange.close();
                    return;
                }
                try {
                    Map<String, String> params = parseQuery(exchange.getRequestURI().getQuery());
                    String exStr = params.get("ex");
                    String symStr = params.get("sym");
                    int levels = params.containsKey("levels") ? Integer.parseInt(params.get("levels")) : 20;
                    if (exStr == null || symStr == null) {
                        sendJson(exchange, 400, "{\"error\":\"Missing ex or sym\"}");
                        return;
                    }
                    OrderbookUpdate u = buffer.get(exStr, symStr);
                    StringBuilder json = new StringBuilder();
                    json.append("{\"exchange\":\"").append(exStr).append("\",\"symbol\":\"").append(symStr).append("\",");
                    if (u == null) {
                        json.append("\"hasData\":false}");
                    } else {
                        long ts = u.getTimestampMs() > 0 ? u.getTimestampMs() : System.currentTimeMillis();
                        long ageMs = System.currentTimeMillis() - ts;
                        json.append("\"hasData\":true,");
                        json.append("\"ts\":").append(ts).append(",");
                        json.append("\"ageMs\":").append(ageMs).append(",");
                        if (u.hasFundingRate()) {
                            json.append("\"fundingRate\":").append(u.getFundingRate()).append(",");
                            if (u.hasFundingIntervalHours()) {
                                json.append("\"fundingIntervalHours\":").append(u.getFundingIntervalHours()).append(",");
                                double fr24h = u.getFundingRate() * (24.0 / u.getFundingIntervalHours());
                                json.append("\"fundingRateNorm24h\":").append(fr24h).append(",");
                            }
                            if (u.hasNextFundingTimeMs()) {
                                json.append("\"nextFundingTimeMs\":").append(u.getNextFundingTimeMs()).append(",");
                            }
                        }
                        json.append("\"bids\":[");
                        appendLevels(json, u.getBidsList(), levels);
                        json.append("],\"asks\":[");
                        appendLevels(json, u.getAsksList(), levels);
                        json.append("]}");
                    }
                    sendJson(exchange, 200, json.toString());
                } catch (Exception ex) {
                    System.err.println("[FEED-PROXY] /orderbook error: " + ex.getMessage());
                    sendJson(exchange, 500, "{\"error\":\"" + ex.getMessage().replace("\"", "\\\"") + "\"}");
                }
            });
            httpServer.setExecutor(null);
            httpServer.start();
            System.out.println("[FEED-PROXY] HTTP server listening on port " + PROXY_HTTP_PORT + " (GET /prices, GET /orderbook)");
        } catch (IOException e) {
            throw new RuntimeException("Failed to start HTTP server", e);
        }
    }

    private void connectToFeed() {
        if (feedChannel != null && !feedChannel.isShutdown()) {
            feedChannel.shutdown();
        }
        String host = feedTarget.contains(":") ? feedTarget.substring(0, feedTarget.indexOf(":")) : feedTarget;
        int port = feedTarget.contains(":") ? Integer.parseInt(feedTarget.substring(feedTarget.indexOf(":") + 1)) : FEED_GRPC_PORT;
        feedChannel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();

        FeedServiceGrpc.FeedServiceStub stub = FeedServiceGrpc.newStub(feedChannel);
        stub.streamAllOrderbooks(Empty.getDefaultInstance(), new StreamObserver<OrderbookUpdate>() {
            @Override
            public void onNext(OrderbookUpdate update) {
                feedConnected = true;
                buffer.put(update);
                if (update.hasFundingRate()) {
                    String k = key(update.getExchange(), update.getSymbol());
                    if (FUNDING_RECEIVED_KEYS.add(k)) {
                        System.out.println("[FEED-PROXY] funding_rate OK from Feed: " + update.getExchange()
                                + " " + update.getSymbol() + " = " + String.format("%.6f (%.4f%%)",
                                update.getFundingRate(), update.getFundingRate() * 100));
                    }
                }
                fanOut(update);
            }

            @Override
            public void onError(Throwable t) {
                feedConnected = false;
                System.err.println("[FEED-PROXY] Feed stream error: " + t.getMessage());
            }

            @Override
            public void onCompleted() {
                feedConnected = false;
                System.out.println("[FEED-PROXY] Feed stream completed (reconnecting...)");
            }
        });
        System.out.println("[FEED-PROXY] Connected to Feed at " + feedTarget);
    }

    private void scheduleReconnect() {
        reconnectScheduler.scheduleWithFixedDelay(() -> {
            if (!feedConnected) {
                System.out.println("[FEED-PROXY] Reconnecting to Feed...");
                connectToFeed();
            }
        }, 5, 5, TimeUnit.SECONDS);
    }

    private void fanOut(OrderbookUpdate update) {
        String key = key(update.getExchange(), update.getSymbol());
        for (SubscriberContext ctx : subscribers.values()) {
            if (ctx.wants(key)) {
                ctx.offer(update);
            }
        }
    }

    private static String key(String exchange, String symbol) {
        return (exchange != null ? exchange.toUpperCase() : "") + "|" + (symbol != null ? symbol : "");
    }

    private static Map<String, String> parseQuery(String query) {
        Map<String, String> out = new java.util.HashMap<>();
        if (query == null || query.isBlank()) return out;
        for (String part : query.split("&")) {
            int idx = part.indexOf('=');
            if (idx <= 0) continue;
            out.put(part.substring(0, idx), part.substring(idx + 1));
        }
        return out;
    }

    private static void appendLevels(StringBuilder json, List<Level> levels, int limit) {
        if (levels == null || levels.isEmpty() || limit <= 0) return;
        int max = Math.min(limit, levels.size());
        for (int i = 0; i < max; i++) {
            Level lvl = levels.get(i);
            if (i > 0) json.append(",");
            json.append("{\"p\":").append(lvl.getPrice()).append(",\"s\":").append(lvl.getSize()).append("}");
        }
    }

    private static void sendJson(com.sun.net.httpserver.HttpExchange exchange, int status, String body) throws IOException {
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, body.length());
        try (var os = exchange.getResponseBody()) { os.write(body.getBytes()); }
    }

    private void shutdown() {
        if (reconnectScheduler != null) reconnectScheduler.shutdownNow();
        if (feedChannel != null) feedChannel.shutdown();
        if (httpServer != null) httpServer.stop(0);
        if (proxyServer != null) proxyServer.shutdown();
    }

    private class FeedProxyServiceImpl extends FeedProxyServiceGrpc.FeedProxyServiceImplBase {
        @Override
        public void streamOrderbooks(SubscriptionRequest request,
                                    StreamObserver<OrderbookUpdate> responseObserver) {
            Set<String> wanted = ConcurrentHashMap.newKeySet();
            for (InstrumentKey k : request.getInstrumentsList()) {
                wanted.add(key(k.getExchange(), k.getSymbol()));
            }

            String subId = "mm-" + System.currentTimeMillis() + "-" + subscribers.size();
            int capacity = 1000;
            SubscriberContext ctx = new SubscriberContext(subId, wanted, responseObserver, capacity);
            subscribers.put(subId, ctx);

            System.out.println("[FEED-PROXY] MM connected: " + subId + " (subscriptions: " + wanted.size() + ")");

            if (responseObserver instanceof ServerCallStreamObserver<?> serverObs) {
                serverObs.setOnCancelHandler(() -> {
                    subscribers.remove(subId);
                    ctx.shutdown();
                    System.out.println("[FEED-PROXY] MM disconnected: " + subId);
                });
            }

            ctx.startDrainThread();
        }
    }

    private static class SubscriberContext {
        final String id;
        final Set<String> wanted;
        final StreamObserver<OrderbookUpdate> observer;
        final BlockingQueue<OrderbookUpdate> queue;
        private volatile boolean running = true;
        private final ExecutorService drainExecutor;

        SubscriberContext(String id, Set<String> wanted, StreamObserver<OrderbookUpdate> observer, int queueCapacity) {
            this.id = id;
            this.wanted = wanted;
            this.observer = observer;
            this.queue = new LinkedBlockingQueue<>(queueCapacity);
            this.drainExecutor = Executors.newSingleThreadExecutor(r -> {
                Thread t = new Thread(r, "proxy-drain-" + id);
                t.setDaemon(true);
                return t;
            });
        }

        boolean wants(String key) {
            return wanted.contains(key);
        }

        void offer(OrderbookUpdate update) {
            if (!queue.offer(update)) {
                // Queue full - drop (backpressure)
            }
        }

        void startDrainThread() {
            drainExecutor.submit(() -> {
                while (running) {
                    try {
                        OrderbookUpdate msg = queue.poll(100, TimeUnit.MILLISECONDS);
                        if (msg != null) {
                            observer.onNext(msg);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    } catch (Exception e) {
                        System.err.println("[FEED-PROXY] Drain error for " + id + ": " + e.getMessage());
                    }
                }
            });
        }

        void shutdown() {
            running = false;
            drainExecutor.shutdownNow();
        }
    }

    /** In-memory buffer: latest OrderbookUpdate per (exchange, symbol). For /prices etc. */
    static class OrderBookBuffer {
        private final ConcurrentHashMap<String, OrderbookUpdate> map = new ConcurrentHashMap<>();

        void put(OrderbookUpdate update) {
            map.put(key(update.getExchange(), update.getSymbol()), update);
        }

        OrderbookUpdate get(String exchange, String symbol) {
            return map.get(key(exchange, symbol));
        }

        Map<String, OrderbookUpdate> snapshot() {
            return new ConcurrentHashMap<>(map);
        }
    }

    public OrderBookBuffer getBuffer() {
        return buffer;
    }
}
