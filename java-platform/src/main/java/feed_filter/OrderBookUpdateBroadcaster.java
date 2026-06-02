package feed_filter;

import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Broadcasts orderbook updates to all registered listeners.
 * Non-blocking: listeners are invoked synchronously; they should not block.
 * Used to push updates from Feed store to gRPC stream (and later to Proxy).
 */
public class OrderBookUpdateBroadcaster {

    private final CopyOnWriteArrayList<OrderBookUpdateListener> listeners = new CopyOnWriteArrayList<>();

    public void addListener(OrderBookUpdateListener listener) {
        if (listener != null) {
            listeners.add(listener);
        }
    }

    public void removeListener(OrderBookUpdateListener listener) {
        listeners.remove(listener);
    }

    /**
     * Broadcast update to all listeners. Called from store wrapper on each OB update.
     * Listeners should not block.
     */
    public void onUpdate(OrderBookSnapshot snapshot) {
        if (snapshot == null) return;
        for (OrderBookUpdateListener listener : listeners) {
            try {
                listener.onOrderBookUpdate(snapshot);
            } catch (Exception e) {
                System.err.println("[BROADCAST] Listener error: " + e.getMessage());
            }
        }
    }

    public int getListenerCount() {
        return listeners.size();
    }

    @FunctionalInterface
    public interface OrderBookUpdateListener {
        void onOrderBookUpdate(OrderBookSnapshot snapshot);
    }
}
