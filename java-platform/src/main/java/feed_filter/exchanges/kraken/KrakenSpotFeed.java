package feed_filter.exchanges.kraken;

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

public class KrakenSpotFeed extends WebSocketClient {

    private static final String WS_URL = "wss://ws.kraken.com";
    private final OrderBookStore store;
    private final Map<String, String> krakenToInternalSymbol = new ConcurrentHashMap<>();
    private final Map<String, SimpleOrderBookTracker> trackers = new ConcurrentHashMap<>();
    private final ObjectMapper mapper = new ObjectMapper();
    private final Set<String> loggedSymbols = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private long lastUpdateMs = 0;
    private final long UPDATE_INTERVAL_MS = 100;

    public KrakenSpotFeed(OrderBookStore store, Map<String, String> symbolMapping) {
        super(URI.create(WS_URL));
        this.store = store;

        System.out.println("[KRAKEN] Init feed with " + symbolMapping.size() + " symbols...");

        for (Map.Entry<String, String> entry : symbolMapping.entrySet()) {
            String rawExternal = entry.getKey();
            String internal = entry.getValue();

            // [FIX] Kraken requires "ISO 4217-A3" format (e.g., "BTC/USD").
            // If we receive "BTC_USD" or "BTCUSD", we must convert it.
            String krakenSymbol = normalizeToKrakenFormat(rawExternal);

            this.krakenToInternalSymbol.put(krakenSymbol, internal);
            this.trackers.put(krakenSymbol, new SimpleOrderBookTracker());
        }
    }

    // [ADDED] Helper to ensure slash separator
    private String normalizeToKrakenFormat(String symbol) {
        if (symbol.contains("/")) return symbol;
        if (symbol.contains("_")) return symbol.replace("_", "/");
        // If "BTCUSD", heuristic split is risky without knowing base length.
        // Assuming user provides valid external symbols in DB (e.g. "XBT/USD" or "BTC_USD").
        // If strict ISO is needed, we assume 3-char quote for now or rely on DB having "BTC/USD".
        return symbol;
    }

    @Override
    public void onOpen(ServerHandshake handshakedata) {
        System.out.println("[KRAKEN] WS Connected.");
        try {
            // Kraken allows max ~100 pairs per subscription message, but we can send multiple.
            // Let's split into batches of 50 just in case.
            int batchSize = 50;
            StringBuilder pairs = new StringBuilder();
            int count = 0;

            for (String kSym : krakenToInternalSymbol.keySet()) {
                if (pairs.length() > 0) pairs.append("\",\"");
                pairs.append(kSym);
                count++;

                if (count >= batchSize) {
                    sendSubscription(pairs.toString());
                    pairs.setLength(0);
                    count = 0;
                }
            }
            if (pairs.length() > 0) {
                sendSubscription(pairs.toString());
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void sendSubscription(String pairsList) {
        String json = String.format(
                "{\"event\":\"subscribe\", \"pair\":[\"%s\"], \"subscription\":{\"name\":\"book\", \"depth\":25}}",
                pairsList
        );
        System.out.println("[KRAKEN] Sending sub batch: " + json);
        send(json);
    }

    @Override
    public void onMessage(String message) {
        try {
            if (message.contains("\"event\":\"heartbeat\"")) return;
            if (message.contains("\"event\":")) {
                System.out.println("[KRAKEN] Event Rx: " + message);
                return;
            }

            JsonNode node = mapper.readTree(message);
            if (!node.isArray()) return;

            String pair = node.get(node.size() - 1).asText();
            String internalSymbol = krakenToInternalSymbol.get(pair);

            if (internalSymbol == null) {
                // Try to match without slash if Kraken sends "XBTUSD" but we subbed "XBT/USD"
                // (though Kraken usually echoes back what was requested or the official name)
                return;
            }

            SimpleOrderBookTracker tracker = trackers.get(pair);
            if (tracker == null) return;

            if (loggedSymbols.add(internalSymbol)) {
                System.out.println("[KRAKEN] >>> FIRST DATA for: " + internalSymbol);
            }

            JsonNode data = node.get(1);
            boolean updated = false;

            if (data.has("as")) { processLevels(tracker, data.get("as"), false); updated = true; }
            if (data.has("bs")) { processLevels(tracker, data.get("bs"), true); updated = true; }
            if (data.has("a"))  { processLevels(tracker, data.get("a"), false); updated = true; }
            if (data.has("b"))  { processLevels(tracker, data.get("b"), true); updated = true; }

            if (updated) {
                long now = System.currentTimeMillis();
                if (now - lastUpdateMs > UPDATE_INTERVAL_MS) {
                    store.update(tracker.toSnapshot(internalSymbol, Exchange.KRAKEN, 20));
                    lastUpdateMs = now;
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void processLevels(SimpleOrderBookTracker tracker, JsonNode levels, boolean isBid) {
        if (levels == null || !levels.isArray()) return;
        for (JsonNode level : levels) {
            double price = level.get(0).asDouble();
            double vol = level.get(1).asDouble();
            if (isBid) tracker.updateBid(price, vol);
            else tracker.updateAsk(price, vol);
        }
    }

    @Override
    public void onClose(int code, String reason, boolean remote) {
        System.out.println("[KRAKEN] Closed. Code: " + code + ", Reason: " + reason);
    }

    @Override
    public void onError(Exception ex) {
        ex.printStackTrace();
    }
}