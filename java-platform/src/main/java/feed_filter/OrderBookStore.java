// OrderBookStore.java

package feed_filter;

public interface OrderBookStore {
    void update(OrderBookSnapshot snapshot);
    OrderBookSnapshot get(Exchange exchange, String symbol);
}
