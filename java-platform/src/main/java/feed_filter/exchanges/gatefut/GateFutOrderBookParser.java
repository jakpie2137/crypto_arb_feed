package feed_filter.exchanges.gatefut;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import feed_filter.Exchange;
import feed_filter.OrderBookLevel;
import feed_filter.OrderBookSnapshot;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Parser for Gate.io Futures orderbook messages.
 */
public class GateFutOrderBookParser {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Parse Gate futures orderbook update.
     * Channel: futures.order_book, event: all
     */
    public static OrderBookSnapshot parse(String json, double contractSize) {
        try {
            JsonNode root = MAPPER.readTree(json);

            if (!"futures.order_book".equals(root.path("channel").asText())) {
                return null;
            }
            // We want event "all" with full OB
            if (!"all".equals(root.path("event").asText())) {
                return null;
            }

            JsonNode result = root.get("result");
            if (result == null || result.isNull()) {
                return null;
            }

            String contract = result.path("contract").asText(); // BTC_USDT
            if (contract == null || contract.isEmpty()) {
                return null;
            }

            long tsMs = result.path("t").asLong();

            List<OrderBookLevel> bids = new ArrayList<>();
            List<OrderBookLevel> asks = new ArrayList<>();

            // asks: [{ "p": "97.1", "s": 2245 }, ...]
            JsonNode asksNode = result.get("asks");
            if (asksNode != null && asksNode.isArray()) {
                for (JsonNode level : asksNode) {
                    double price = Double.parseDouble(level.path("p").asText("0"));
                    double sizeInContracts = level.path("s").asDouble(0.0);
                    double size = sizeInContracts * contractSize;
                    if (size > 0) {
                        asks.add(new OrderBookLevel(price, size));
                    }
                }
            }

            // bids: [{ "p": "97.1", "s": 2245 }, ...]
            JsonNode bidsNode = result.get("bids");
            if (bidsNode != null && bidsNode.isArray()) {
                for (JsonNode level : bidsNode) {
                    double price = Double.parseDouble(level.path("p").asText("0"));
                    double sizeInContracts = level.path("s").asDouble(0.0);
                    double size = sizeInContracts * contractSize;
                    if (size > 0) {
                        bids.add(new OrderBookLevel(price, size));
                    }
                }
            }

            String symbol = contract.replace("_", "");

            return new OrderBookSnapshot(
                    Exchange.GATEFUT,
                    symbol,
                    tsMs,
                    bids,
                    asks
            );
        } catch (IOException | NumberFormatException e) {
            System.err.println("Gate futures parse error: " + e.getMessage());
            return null;
        }
    }
}
