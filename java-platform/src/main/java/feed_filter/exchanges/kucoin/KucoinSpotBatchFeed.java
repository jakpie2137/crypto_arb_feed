package feed_filter.exchanges.kucoin;

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
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class KucoinSpotBatchFeed {

    private final int shardId;
    private final List<KucoinSpotFeed.Sub> subs;
    private final OrderBookStore store;
    private final Function<String, String> externalToInternal;
    private final Duration staleThreshold;

    private final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
    private final ScheduledExecutorService exec = Executors.newScheduledThreadPool(2);
    private final ObjectMapper mapper = new ObjectMapper();

    private volatile WebSocket ws;
    private volatile boolean running = false;
    private volatile long lastMsgTime = System.currentTimeMillis();
    private volatile ScheduledFuture<?> pingTask;

    private final Map<String, String> topicMap = new ConcurrentHashMap<>();

    public KucoinSpotBatchFeed(int shardId,
                               List<KucoinSpotFeed.Sub> subs,
                               OrderBookStore store,
                               Function<String, String> externalToInternal,
                               Duration stale) {
        this.shardId = shardId;
        this.subs = subs;
        this.store = store;
        this.externalToInternal = externalToInternal;
        this.staleThreshold = stale;

        for (KucoinSpotFeed.Sub s : subs) {
            topicMap.put(s.externalSymbol(), s.internalSymbol());
        }
    }

    public void start() {
        running = true;
        // Random start delay to prevent hitting "Connect Rate Limits" (max 30/min usually)
        long initialDelay = (long) (Math.random() * 2000) + (shardId * 500L);
        exec.schedule(this::connect, initialDelay, TimeUnit.MILLISECONDS);
        exec.scheduleAtFixedRate(this::checkStale, 10, 5, TimeUnit.SECONDS);
    }

    public void stop() {
        running = false;
        exec.shutdownNow();
        if (ws != null) ws.sendClose(1000, "bye");
    }

    private void checkStale() {
        if (!running) return;
        long silence = System.currentTimeMillis() - lastMsgTime;
        // KuCoin is chatty, 60s silence is definitely broken
        if (silence > 60_000) {
            FeedLogger.error("KUCOIN", "S" + shardId + " Stale detected (" + silence + "ms). Reconnecting.");
            reconnect();
        }
    }

    private void connect() {
        if (!running) return;

        CompletableFuture.runAsync(() -> {
            try {
                // 1. Fetch NEW Token (Critical: Tokens expire!)
                String url = "https://api.kucoin.com/api/v1/bullet-public";
                HttpRequest req = HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .header("User-Agent", "Mozilla/5.0")
                        .POST(HttpRequest.BodyPublishers.noBody())
                        .build();
                HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());

                if (resp.statusCode() != 200) {
                    throw new RuntimeException("HTTP " + resp.statusCode());
                }

                JsonNode root = mapper.readTree(resp.body());
                JsonNode data = root.get("data");
                if (data == null) throw new RuntimeException("No data in handshake");

                String token = data.get("token").asText();
                String endpoint = data.get("instanceServers").get(0).get("endpoint").asText();

                // Dynamic Ping Interval (KuCoin sends this, usually 18000ms)
                long serverPingInterval = 18000;
                if (data.get("instanceServers").get(0).has("pingInterval")) {
                    serverPingInterval = data.get("instanceServers").get(0).get("pingInterval").asLong();
                }

                // Safety margin: Ping slightly faster than requested
                long myPingInterval = Math.max(2000, serverPingInterval - 2000);

                String wsUrl = endpoint + "?token=" + token;

                // Pass ping interval to listener
                client.newWebSocketBuilder()
                        .buildAsync(URI.create(wsUrl), new WsListener(myPingInterval))
                        .join();

            } catch (Exception e) {
                FeedLogger.error("KUCOIN", "S" + shardId + " Connect fail: " + e.getMessage());
                scheduleReconnect();
            }
        });
    }

    private void scheduleReconnect() {
        if (!running) return;
        exec.schedule(this::connect, 5, TimeUnit.SECONDS);
    }

    private void reconnect() {
        if (ws != null) {
            // Cancel ping task
            if (pingTask != null) pingTask.cancel(true);
            ws.sendClose(1000, "reconnect");
            ws = null;
        }
        scheduleReconnect();
    }

    private class WsListener implements WebSocket.Listener {
        private final long pingIntervalMs;
        StringBuilder buffer = new StringBuilder();

        WsListener(long pingIntervalMs) {
            this.pingIntervalMs = pingIntervalMs;
        }

        @Override
        public void onOpen(WebSocket webSocket) {
            ws = webSocket;
            lastMsgTime = System.currentTimeMillis();
            FeedLogger.info("KUCOIN", "S" + shardId + " Connected.");
            webSocket.request(1);

            // Start Ping Loop
            if (pingTask != null) pingTask.cancel(true);
            pingTask = exec.scheduleAtFixedRate(() -> {
                try {
                    if (ws != null) {
                        String ping = "{\"id\":\"" + System.currentTimeMillis() + "\",\"type\":\"ping\"}";
                        ws.sendText(ping, true);
                    }
                } catch (Exception e) {
                    // Ignored
                }
            }, pingIntervalMs, pingIntervalMs, TimeUnit.MILLISECONDS);

            subscribe(webSocket);
        }

        private void subscribe(WebSocket w) {
            // Wait a tiny bit for the connection to settle? No, KuCoin is usually instant.
            int targetDepth = OrderBookDepthConfig.KUCOIN_SPOT_DEPTH;
            String topicPrefix = (targetDepth <= 5) ? "/spotMarket/level2Depth5:" : "/spotMarket/level2Depth50:";

            StringBuilder topic = new StringBuilder(topicPrefix);
            int count = 0;
            for (KucoinSpotFeed.Sub s : subs) {
                if (count > 0) topic.append(",");
                topic.append(s.externalSymbol());
                count++;
            }

            String msg = String.format("{\"id\":\"%d\",\"type\":\"subscribe\",\"topic\":\"%s\",\"response\":true}",
                    System.nanoTime(), topic.toString());
            w.sendText(msg, true);
        }

        @Override
        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            webSocket.request(1);
            lastMsgTime = System.currentTimeMillis();
            buffer.append(data);
            if (!last) return null;

            String msg = buffer.toString();
            buffer.setLength(0);

            try {
                if (msg.contains("\"type\":\"pong\"")) return null; // Ignore pongs

                // Initial Welcome Message
                if (msg.contains("\"type\":\"welcome\"")) return null;

                JsonNode node = mapper.readTree(msg);
                if (!node.has("topic")) return null;

                String topic = node.get("topic").asText();
                int idx = topic.indexOf(':');
                if (idx < 0) return null;
                String external = topic.substring(idx + 1);

                String internal = topicMap.get(external);
                if (internal != null) {
                    JsonNode dataNode = node.get("data");

                    List<OrderBookLevel> bids = parseSide(dataNode.get("bids"));
                    List<OrderBookLevel> asks = parseSide(dataNode.get("asks"));

                    long ts = System.currentTimeMillis();
                    if (dataNode.has("timestamp")) {
                        ts = dataNode.get("timestamp").asLong();
                    }

                    OrderBookSnapshot snap = new OrderBookSnapshot(
                            Exchange.KUCOIN,
                            internal,
                            ts,
                            bids,
                            asks
                    );

                    if (store instanceof InMemoryOrderBookStore) {
                        ((InMemoryOrderBookStore) store).putSnapshot(snap);
                    } else {
                        store.update(snap);
                    }
                }

            } catch (Exception e) {
                // ignore
            }
            return null;
        }

        @Override
        public void onError(WebSocket webSocket, Throwable error) {
            FeedLogger.error("KUCOIN", "S" + shardId + " Error: " + error);
            reconnect();
        }

        @Override
        public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
            FeedLogger.info("KUCOIN", "S" + shardId + " Closed: " + reason);
            reconnect();
            return null;
        }
    }

    private List<OrderBookLevel> parseSide(JsonNode list) {
        if (list == null || !list.isArray()) return List.of();
        List<OrderBookLevel> out = new ArrayList<>(list.size());
        for (JsonNode e : list) {
            out.add(new OrderBookLevel(Double.parseDouble(e.get(0).asText()), Double.parseDouble(e.get(1).asText())));
        }
        return out;
    }
}