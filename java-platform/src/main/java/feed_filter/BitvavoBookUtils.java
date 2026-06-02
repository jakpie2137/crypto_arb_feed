package feed_filter;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.NavigableMap;

/** Shared order book delta logic for Bitvavo (standard WS and Market Data Pro). */
final class BitvavoBookUtils {

    private static final double REMOVE_PRICE_EPSILON = 1e-10;

    /** Apply delta levels to side. size<=0 removes the level. */
    static void applyDelta(JsonNode arr, NavigableMap<Double, Double> side, int depthLimit) {
        if (arr == null || !arr.isArray()) return;
        for (JsonNode lvl : arr) {
            if (!lvl.isArray() || lvl.size() < 2) continue;
            double price = lvl.get(0).asDouble(0);
            double size  = lvl.get(1).asDouble(0);
            if (size <= 0.0 || !Double.isFinite(size)) {
                removeLevel(side, price);
            } else {
                side.put(price, size);
            }
        }
        while (side.size() > depthLimit * 5) {
            side.pollLastEntry();
        }
    }

    /** Remove level; fuzzy match when exact Double key fails (precision). */
    static void removeLevel(NavigableMap<Double, Double> side, double price) {
        if (side.remove(price) != null) return;
        double tol = Math.max(REMOVE_PRICE_EPSILON, Math.abs(price) * REMOVE_PRICE_EPSILON);
        Double floor = side.floorKey(price + tol);
        if (floor != null && Math.abs(floor - price) <= tol) {
            side.remove(floor);
            return;
        }
        Double ceiling = side.ceilingKey(price - tol);
        if (ceiling != null && Math.abs(ceiling - price) <= tol) {
            side.remove(ceiling);
        }
    }
}
