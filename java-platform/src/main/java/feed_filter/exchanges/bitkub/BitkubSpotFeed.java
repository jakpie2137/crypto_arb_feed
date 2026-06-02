package feed_filter.exchanges.bitkub;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import feed_filter.Exchange;
import feed_filter.OrderBookStore;
import feed_filter.exchanges.common.SimpleOrderBookTracker;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class BitkubSpotFeed extends WebSocketClient {

    private static final String WS_BASE_URL = "wss://api.bitkub.com/websocket-api/";
    private final OrderBookStore store;
    private final Map<String, String> streamToInternalSymbol = new ConcurrentHashMap<>();
    private final Map<String, SimpleOrderBookTracker> trackers = new ConcurrentHashMap<>();
    private final ObjectMapper mapper = new ObjectMapper();

    private final Set<String> loggedSymbols = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private long lastUpdateMs = 0;
    private final long UPDATE_INTERVAL_MS = 100;

    public static BitkubSpotFeed create(OrderBookStore store, Map<String, String> symbolMapping) {
        StringBuilder streams = new StringBuilder();
        Map<String, String> streamMap = new ConcurrentHashMap<>();

        System.out.println("[BITKUB] Preparing feed for " + symbolMapping.size() + " symbols...");

        for (Map.Entry<String, String> entry : symbolMapping.entrySet()) {
            String rawExternal = entry.getKey();
            String streamName = "market.books." + rawExternal;

            if (streams.length() > 0) streams.append(",");
            streams.append(streamName);

            streamMap.put(streamName, entry.getValue());
            // System.out.println("[BITKUB] Mapping stream: " + streamName + " -> " + entry.getValue());
        }

        String url = WS_BASE_URL + streams.toString();
        System.out.println("[BITKUB] Connecting to URI: " + url);

        URI uri = URI.create(url);
        return new BitkubSpotFeed(uri, store, streamMap);
    }

    private BitkubSpotFeed(URI uri, OrderBookStore store, Map<String, String> streamMap) {
        super(uri);
        this.store = store;
        this.streamToInternalSymbol.putAll(streamMap);

        for (String stream : streamMap.keySet()) {
            trackers.put(stream, new SimpleOrderBookTracker());
        }
    }

    @Override
    public void onOpen(ServerHandshake handshakedata) {
        System.out.println("[BITKUB] WS Connected.");
    }

    @Override
    public void onMessage(String message) {
        try {
            JsonNode node = mapper.readTree(message);

            if (!node.has("stream") || !node.has("data")) {
                // System.out.println("[BITKUB] Ignored msg: " + message);
                return;
            }

            String streamName = node.get("stream").asText();
            String internalSymbol = streamToInternalSymbol.get(streamName);

            if (internalSymbol == null) {
                System.err.println("[BITKUB] WARN: Unknown stream: " + streamName);
                return;
            }

            SimpleOrderBookTracker tracker = trackers.get(streamName);
            if (tracker == null) return;

            // Log sukcesu
            if (loggedSymbols.add(internalSymbol)) {
                System.out.println("[BITKUB] >>> FIRST DATA RECEIVED for: " + internalSymbol + " (stream: " + streamName + ")");
            }

            JsonNode data = node.get("data");

            if (data.has("bids")) processLevels(tracker, data.get("bids"), true);
            if (data.has("asks")) processLevels(tracker, data.get("asks"), false);

            long now = System.currentTimeMillis();
            if (now - lastUpdateMs > UPDATE_INTERVAL_MS) {
                store.update(tracker.toSnapshot(internalSymbol, Exchange.BITKUB, 20));
                lastUpdateMs = now;
            }

        } catch (Exception e) {
            System.err.println("[BITKUB] Error msg: " + message);
            e.printStackTrace();
        }
    }

    private void processLevels(SimpleOrderBookTracker tracker, JsonNode levels, boolean isBid) {
        if (!levels.isArray()) return;
        for (JsonNode level : levels) {
            double price = level.get(0).asDouble();
            double size = level.get(1).asDouble();
            if (isBid) tracker.updateBid(price, size);
            else tracker.updateAsk(price, size);
        }
    }

    @Override
    public void onClose(int code, String reason, boolean remote) {
        System.out.println("[BITKUB] Closed. Code: " + code + ", Reason: " + reason);
    }

    @Override
    public void onError(Exception ex) {
        System.err.println("[BITKUB] WebSocket Error:");
        ex.printStackTrace();
    }
}