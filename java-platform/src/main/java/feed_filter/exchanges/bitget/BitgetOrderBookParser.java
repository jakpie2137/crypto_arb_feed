// BitgetOrderBookParser.java

package feed_filter.exchanges.bitget;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import feed_filter.Exchange;
import feed_filter.OrderBookLevel;
import feed_filter.OrderBookSnapshot;

import java.util.ArrayList;
import java.util.List;

public final class BitgetOrderBookParser {

    private static final ObjectMapper M = new ObjectMapper();

    private BitgetOrderBookParser() {}

    /**
     * Parses Bitget WS "books" message.
     *
     * Expected shape:
     * {
     *   "arg":{"instType":"SPOT","channel":"books","instId":"BTCUSDT"},
     *   "data":[{"bids":[["price","size"],...],"asks":[...],"ts":"169..."}]
     * }
     */
    public static Parsed parseBooks(String json) {
        try {
            JsonNode root = M.readTree(json);

            // ignore events/acks/errors
            if (root.has("event")) return null;

            JsonNode arg = root.get("arg");
            JsonNode data = root.get("data");
            if (arg == null || data == null || !data.isArray() || data.isEmpty()) return null;

            String instId = text(arg, "instId");
            if (instId == null) return null;

            JsonNode d0 = data.get(0);
            String tsStr = text(d0, "ts");
            long ts = tsStr != null ? Long.parseLong(tsStr) : System.currentTimeMillis();

            List<OrderBookLevel> bids = parseSide(d0.get("bids"));
            List<OrderBookLevel> asks = parseSide(d0.get("asks"));

            OrderBookSnapshot snap = new OrderBookSnapshot(
                    Exchange.BITGET,
                    instId,   // external for now; caller will map to internal
                    ts,
                    bids,
                    asks
            );

            return new Parsed(instId, ts, snap);

        } catch (Exception e) {
            return null;
        }
    }

    private static List<OrderBookLevel> parseSide(JsonNode arr) {
        List<OrderBookLevel> out = new ArrayList<>();
        if (arr == null || !arr.isArray()) return out;
        for (JsonNode lvl : arr) {
            if (!lvl.isArray() || lvl.size() < 2) continue;
            double p = Double.parseDouble(lvl.get(0).asText());
            double s = Double.parseDouble(lvl.get(1).asText());
            out.add(new OrderBookLevel(p, s));
        }
        return out;
    }

    private static String text(JsonNode node, String field) {
        JsonNode v = node.get(field);
        return (v == null || v.isNull()) ? null : v.asText();
    }

    public record Parsed(String externalSymbol, long timestampMs, OrderBookSnapshot snapshot) {}
}
