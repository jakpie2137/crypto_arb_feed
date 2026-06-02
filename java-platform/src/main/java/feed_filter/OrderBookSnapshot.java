// OrderBookSnapshot.java

package feed_filter;

import java.util.List;

public class OrderBookSnapshot {
    private final Exchange exchange;
    private final String symbol;
    private final long eventTimeMillis;
    private final java.util.List<OrderBookLevel> bids;
    private final java.util.List<OrderBookLevel> asks;

    public OrderBookSnapshot(Exchange exchange,
                             String symbol,
                             long eventTimeMillis,
                             List<OrderBookLevel> bids,
                             List<OrderBookLevel> asks) {
        this.exchange = exchange;
        this.symbol = symbol;
        this.eventTimeMillis = eventTimeMillis;
        this.bids = bids;
        this.asks = asks;
    }

    public Exchange getExchange() {
        return exchange;
    }

    public String getSymbol() {
        return symbol;
    }

    public long getEventTimeMillis() {
        return eventTimeMillis;
    }

    public List<OrderBookLevel> getBids() {
        return bids;
    }

    public List<OrderBookLevel> getAsks() {
        return asks;
    }
}
