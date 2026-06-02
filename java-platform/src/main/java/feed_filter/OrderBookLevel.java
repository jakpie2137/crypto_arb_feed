// OrderBookLevel.java

package feed_filter;

public class OrderBookLevel {
    private final double price;
    private final double size;

    public OrderBookLevel(double price, double size) {
        this.price = price;
        this.size = size;
    }

    public double getPrice() {
        return price;
    }

    public double getSize() {
        return size;
    }

    @Override
    public String toString() {
        return String.format("%f x %f", price, size);
    }
}
