package feed_filter.exchanges.bitget;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import feed_filter.Exchange;
import feed_filter.FeedLogger;
import feed_filter.InMemoryOrderBookStore;
import feed_filter.OrderBookLevel;
import feed_filter.OrderBookSnapshot;
import feed_filter.OrderBookStore;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class BitgetSpotBatchFeed {

    private static final String WS_URL = "wss://ws.bitget.com/v2/ws/public";

    private final OrderBookStore store;
    private final Function<String, String> externalToInternal;
    private final Duration staleThreshold;

    private final HttpClient http = HttpClient.newHttpClient();
    private final ObjectMapper mapper = new ObjectMapper();

    private final List<GroupConn> groups = new ArrayList<>();
    private final ScheduledExecutorService monitor = Executors.newSingleThreadScheduledExecutor();

    public BitgetSpotBatchFeed(List<BitgetSpotFeed.Sub> subs,
                               OrderBookStore store,
                               Function<String, String> externalToInternal,
                               Duration staleAfter) {
        this.store = store;
        this.externalToInternal = externalToInternal;
        this.staleThreshold = staleAfter;

        int batchSize = 50;
        for (int i = 0; i < subs.size(); i += batchSize) {
            int end = Math.min(subs.size(), i + batchSize);
            groups.add(new GroupConn(i / batchSize, subs.subList(i, end)));
        }
    }

    public void start() {
        for (GroupConn g : groups) g.connect();
        monitor.scheduleAtFixedRate(this::checkStale, 10, 5, TimeUnit.SECONDS);
    }

    public void stop() {
        monitor.shutdownNow();
        for (GroupConn g : groups) g.close();
    }

    private void checkStale() {
        long now = System.currentTimeMillis();
        for (GroupConn g : groups) {
            if (now - g.lastActivity.get() > staleThreshold.toMillis() + 10_000) {
                FeedLogger.info("BITGET", "G" + g.id + " Stale socket. Reconnecting.");
                g.reconnect();
            }
        }
    }

    private final class GroupConn implements WebSocket.Listener {
        final int id;
        final List<BitgetSpotFeed.Sub> mySubs;
        final Map<String, String> extToInt = new HashMap<>();

        volatile WebSocket ws;
        volatile ScheduledFuture<?> pingTask;
        final AtomicLong lastActivity = new AtomicLong(System.currentTimeMillis());
        private final StringBuilder textBuf = new StringBuilder();

        GroupConn(int id, List<BitgetSpotFeed.Sub> subs) {
            this.id = id;
            this.mySubs = subs;
            for (BitgetSpotFeed.Sub s : subs) {
                extToInt.put(s.externalSymbol(), s.internalSymbol());
            }
        }

        void connect() {
            http.newWebSocketBuilder()
                    .connectTimeout(Duration.ofSeconds(30))
                    .buildAsync(URI.create(WS_URL), this)
                    .exceptionally(e -> {
                        FeedLogger.error("BITGET", "G" + id + " Connect fail: " + e.getMessage());
                        return null;
                    });
        }

        void reconnect() {
            close();
            try { Thread.sleep(2000); } catch (Exception e) {}
            connect();
        }

        void close() {
            if (pingTask != null) {
                pingTask.cancel(true);
                pingTask = null;
            }
            if (ws != null) {
                ws.abort();
                ws = null;
            }
        }

        @Override
        public void onOpen(WebSocket webSocket) {
            ws = webSocket;
            lastActivity.set(System.currentTimeMillis());
            FeedLogger.info("BITGET", "G" + id + " Connected.");
            webSocket.request(1);

            pingTask = monitor.scheduleAtFixedRate(() -> {
                try {
                    if (ws != null) ws.sendText("ping", true);
                } catch (Exception e) {}
            }, 20, 20, TimeUnit.SECONDS);

            sendSub(webSocket);
        }

        private void sendSub(WebSocket w) {
            StringBuilder sb = new StringBuilder();
            sb.append("{\"op\":\"subscribe\",\"args\":[");
            for (int i = 0; i < mySubs.size(); i++) {
                if (i > 0) sb.append(",");
                sb.append("{\"instType\":\"SPOT\",\"channel\":\"books\",\"instId\":\"")
                        .append(mySubs.get(i).externalSymbol()).append("\"}");
            }
            sb.append("]}");
            w.sendText(sb.toString(), true);
        }

        @Override
        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            webSocket.request(1);
            lastActivity.set(System.currentTimeMillis());

            synchronized (textBuf) {
                textBuf.append(data);
                if (!last) return null;

                String msg = textBuf.toString();
                textBuf.setLength(0);

                if (msg.equals("pong")) return null;

                try {
                    JsonNode root = mapper.readTree(msg);
                    if (!root.has("data")) return null;

                    String action = root.has("action") ? root.get("action").asText() : "update";
                    String symbol = root.get("arg").get("instId").asText();
                    String internal = extToInt.get(symbol);

                    if (internal != null) {
                        if ("snapshot".equals(action)) {
                            if (store instanceof InMemoryOrderBookStore) {
                                ((InMemoryOrderBookStore) store).reset(Exchange.BITGET, internal);
                            }
                        }

                        JsonNode dataNode = root.get("data");
                        if (dataNode.isArray() && dataNode.size() > 0) {
                            JsonNode d = dataNode.get(0);
                            long ts = d.has("ts") ? d.get("ts").asLong() : System.currentTimeMillis();

                            List<OrderBookLevel> bids = parseSide(d.get("bids"));
                            List<OrderBookLevel> asks = parseSide(d.get("asks"));

                            store.update(new OrderBookSnapshot(
                                    Exchange.BITGET,
                                    internal,
                                    ts,
                                    bids,
                                    asks
                            ));
                        }
                    }
                } catch (Exception e) {
                    // ignore
                }
            }
            return null;
        }

        private List<OrderBookLevel> parseSide(JsonNode list) {
            if (list == null || !list.isArray()) return List.of();
            List<OrderBookLevel> out = new ArrayList<>(list.size());
            for (JsonNode e : list) {
                out.add(new OrderBookLevel(Double.parseDouble(e.get(0).asText()), Double.parseDouble(e.get(1).asText())));
            }
            return out;
        }

        @Override
        public void onError(WebSocket webSocket, Throwable error) {
            FeedLogger.error("BITGET", "G" + id + " Error: " + error);
            reconnect();
        }

        @Override
        public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
            FeedLogger.info("BITGET", "G" + id + " Closed: " + reason);
            reconnect();
            return null;
        }
    }
}