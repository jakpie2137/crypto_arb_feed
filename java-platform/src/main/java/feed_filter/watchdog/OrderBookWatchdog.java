package feed_filter.watchdog;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Generic watchdog for per-topic staleness.
 *
 * Usage:
 *  - onUpdate(externalSymbol) whenever you process an OB update
 *  - tick() periodically (e.g. 1s)
 *  - if stale -> call onStale.accept(externalSymbol)
 */
public class OrderBookWatchdog {

    private final long staleAfterMs;
    private final Map<String, Long> lastUpdateMs = new ConcurrentHashMap<>();
    private final Consumer<String> onStale;
    private volatile long nowMsOverride = -1;

    public OrderBookWatchdog(Duration staleAfter, Consumer<String> onStale) {
        this.staleAfterMs = staleAfter.toMillis();
        this.onStale = onStale;
    }

    public void onUpdate(String externalSymbol, long nowMs) {
        lastUpdateMs.put(externalSymbol, nowMs);
    }

    public void onUpdate(String externalSymbol) {
        lastUpdateMs.put(externalSymbol, nowMs());
    }

    public void tick() {
        long now = nowMs();
        for (Map.Entry<String, Long> e : lastUpdateMs.entrySet()) {
            long age = now - e.getValue();
            if (age >= staleAfterMs) {
                onStale.accept(e.getKey());
                // reset timer so we don't spam; the exchange handler decides what to do next
                lastUpdateMs.put(e.getKey(), now);
            }
        }
    }

    // for tests
    public void setNowMsOverride(long nowMs) { this.nowMsOverride = nowMs; }

    private long nowMs() {
        if (nowMsOverride >= 0) return nowMsOverride;
        return System.currentTimeMillis();
    }
}
