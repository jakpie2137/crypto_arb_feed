package feed_filter.exchanges.common;

import feed_filter.Exchange; // [ADDED]
import feed_filter.OrderBookLevel;
import feed_filter.OrderBookSnapshot;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

public class SimpleOrderBookTracker {
    // TreeMap trzyma posortowane ceny (Asks rosnąco, Bids malejąco)
    private final NavigableMap<Double, Double> bids = new ConcurrentSkipListMap<>(Collections.reverseOrder());
    private final NavigableMap<Double, Double> asks = new ConcurrentSkipListMap<>();

    public void updateBid(double price, double size) {
        if (size <= 0) bids.remove(price);
        else bids.put(price, size);
    }

    public void updateAsk(double price, double size) {
        if (size <= 0) asks.remove(price);
        else asks.put(price, size);
    }

    public void clear() {
        bids.clear();
        asks.clear();
    }

    // [CHANGED] exchangeName (String) -> exchange (Exchange)
    public OrderBookSnapshot toSnapshot(String symbol, Exchange exchange, int depthLimit) {
        List<OrderBookLevel> bidLevels = new ArrayList<>();
        List<OrderBookLevel> askLevels = new ArrayList<>();

        int count = 0;
        for (Map.Entry<Double, Double> entry : bids.entrySet()) {
            if (count++ >= depthLimit) break;
            bidLevels.add(new OrderBookLevel(entry.getKey(), entry.getValue()));
        }

        count = 0;
        for (Map.Entry<Double, Double> entry : asks.entrySet()) {
            if (count++ >= depthLimit) break;
            askLevels.add(new OrderBookLevel(entry.getKey(), entry.getValue()));
        }

        return new OrderBookSnapshot(
                exchange, // Pass Enum directly
                symbol,
                System.currentTimeMillis(),
                bidLevels,
                askLevels
        );
    }
}