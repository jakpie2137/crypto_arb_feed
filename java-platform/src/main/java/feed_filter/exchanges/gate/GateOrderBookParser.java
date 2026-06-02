package feed_filter.exchanges.gate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import feed_filter.Exchange;
import feed_filter.OrderBookLevel;
import feed_filter.OrderBookSnapshot;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Parser for Gate.io Spot orderbook messages.
 */
public class GateOrderBookParser {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Parse Gate spot orderbook update.
     * Channel: spot.order_book, event: update (or snapshot)
     */
    public static OrderBookSnapshot parse(String json) {
        try {
            JsonNode root = MAPPER.readTree(json);

            String channel = root.path("channel").asText();
            if (!"spot.order_book".equals(channel) && !"spot.order_book_update".equals(channel)) {
                return null;
            }

            String event = root.path("event").asText();
            // Accept both "update" and "snapshot"
            if (!"update".equals(event) && !"snapshot".equals(event) && !"all".equals(event)) {
                return null;
            }

            JsonNode result = root.get("result");
            if (result == null || result.isNull()) {
                return null;
            }

            // spot.order_book uses "s" for symbol (e.g., "BTC_USDT")
            String pair = result.path("s").asText();
            if (pair == null || pair.isEmpty()) {
                pair = result.path("currency_pair").asText();
            }
            if (pair == null || pair.isEmpty()) {
                return null;
            }

            long tsMs = result.path("t").asLong();
            if (tsMs == 0) {
                tsMs = System.currentTimeMillis();
            }

            List<OrderBookLevel> bids = new ArrayList<>();
            List<OrderBookLevel> asks = new ArrayList<>();

            // Spot format: arrays of [price, size]
            JsonNode asksNode = result.get("asks");
            if (asksNode != null && asksNode.isArray()) {
                for (JsonNode level : asksNode) {
                    if (level.isArray() && level.size() >= 2) {
                        double price = Double.parseDouble(level.get(0).asText("0"));
                        double size = Double.parseDouble(level.get(1).asText("0"));
                        if (size > 0) {
                            asks.add(new OrderBookLevel(price, size));
                        }
                    }
                }
            }

            JsonNode bidsNode = result.get("bids");
            if (bidsNode != null && bidsNode.isArray()) {
                for (JsonNode level : bidsNode) {
                    if (level.isArray() && level.size() >= 2) {
                        double price = Double.parseDouble(level.get(0).asText("0"));
                        double size = Double.parseDouble(level.get(1).asText("0"));
                        if (size > 0) {
                            bids.add(new OrderBookLevel(price, size));
                        }
                    }
                }
            }

            String symbol = pair.replace("_", "");

            return new OrderBookSnapshot(
                    Exchange.GATE,
                    symbol,
                    tsMs,
                    bids,
                    asks
            );
        } catch (IOException | NumberFormatException e) {
            System.err.println("Gate spot parse error: " + e.getMessage());
            return null;
        }
    }
}
