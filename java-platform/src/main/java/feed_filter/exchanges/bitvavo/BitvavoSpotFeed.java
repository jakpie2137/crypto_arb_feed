package feed_filter.exchanges.bitvavo;

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

public class BitvavoSpotFeed extends WebSocketClient {

    private static final String WS_URL = "wss://ws.bitvavo.com/v2/";
    private final OrderBookStore store;
    private final Map<String, String> bitvavoToInternalSymbol = new ConcurrentHashMap<>();
    private final Map<String, SimpleOrderBookTracker> trackers = new ConcurrentHashMap<>();
    private final ObjectMapper mapper = new ObjectMapper();

    private final Set<String> loggedSymbols = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private long lastUpdateMs = 0;
    private final long UPDATE_INTERVAL_MS = 100;

    public BitvavoSpotFeed(OrderBookStore store, Map<String, String> symbolMapping) {
        super(URI.create(WS_URL));
        this.store = store;
        this.bitvavoToInternalSymbol.putAll(symbolMapping);

        System.out.println("[BITVAVO] Init feed with " + symbolMapping.size() + " symbols: " + symbolMapping.values());

        for (String bSym : symbolMapping.keySet()) {
            trackers.put(bSym, new SimpleOrderBookTracker());
        }
    }

    @Override
    public void onOpen(ServerHandshake handshakedata) {
        System.out.println("[BITVAVO] WS Connected.");

        try {
            StringBuilder markets = new StringBuilder();
            for (String bSym : bitvavoToInternalSymbol.keySet()) {
                if (markets.length() > 0) markets.append("\",\"");
                markets.append(bSym);
            }

            String json = String.format(
                    "{\"action\":\"subscribe\", \"channels\":[{\"name\":\"book\", \"markets\":[\"%s\"]}]}",
                    markets.toString()
            );
            System.out.println("[BITVAVO] Sending subscribe: " + json);
            send(json);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void onMessage(String message) {
        try {
            JsonNode node = mapper.readTree(message);

            if (node.has("event") && !node.get("event").asText().equals("book")) {
                // Logowanie innych zdarzeń (np. potwierdzenie subskrypcji)
                System.out.println("[BITVAVO] Event: " + node.get("event").asText());
                return;
            }

            if (!node.has("market")) return;

            String market = node.get("market").asText();
            String internalSymbol = bitvavoToInternalSymbol.get(market);

            if (internalSymbol == null) {
                System.err.println("[BITVAVO] WARN: Unknown market received: " + market);
                return;
            }

            SimpleOrderBookTracker tracker = trackers.get(market);
            if (tracker == null) return;

            // Log sukcesu
            if (loggedSymbols.add(internalSymbol)) {
                System.out.println("[BITVAVO] >>> FIRST DATA RECEIVED for: " + internalSymbol);
            }

            if (node.has("bids")) processLevels(tracker, node.get("bids"), true);
            if (node.has("asks")) processLevels(tracker, node.get("asks"), false);

            long now = System.currentTimeMillis();
            if (now - lastUpdateMs > UPDATE_INTERVAL_MS) {
                store.update(tracker.toSnapshot(internalSymbol, Exchange.BITVAVO, 20));
                lastUpdateMs = now;
            }

        } catch (Exception e) {
            System.err.println("[BITVAVO] Error processing msg: " + message);
            e.printStackTrace();
        }
    }

    private void processLevels(SimpleOrderBookTracker tracker, JsonNode levels, boolean isBid) {
        for (JsonNode level : levels) {
            double price = level.get(0).asDouble();
            double size = level.get(1).asDouble();
            if (isBid) tracker.updateBid(price, size);
            else tracker.updateAsk(price, size);
        }
    }

    @Override
    public void onClose(int code, String reason, boolean remote) {
        System.out.println("[BITVAVO] Closed. Code: " + code + ", Reason: " + reason);
    }

    @Override
    public void onError(Exception ex) {
        System.err.println("[BITVAVO] WebSocket Error:");
        ex.printStackTrace();
    }
}