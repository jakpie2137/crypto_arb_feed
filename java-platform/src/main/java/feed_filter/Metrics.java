package feed_filter;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Ultra-simple metrics for feed performance diagnostics.
 */
public class Metrics {

    public static final AtomicLong SNAPSHOTS_TOTAL = new AtomicLong();
    public static final AtomicLong SNAPSHOTS_LAST_SECOND = new AtomicLong();

    private static volatile long lastPrintMillis = System.currentTimeMillis();

    public static void onSnapshot() {
        SNAPSHOTS_TOTAL.incrementAndGet();
        long now = System.currentTimeMillis();
        long delta = now - lastPrintMillis;
        if (delta >= 1000L) {
            long perSec = SNAPSHOTS_LAST_SECOND.getAndSet(0);
            long total = SNAPSHOTS_TOTAL.get();
            System.out.println("METRICS snapshots/s=" + perSec + " total=" + total);
            lastPrintMillis = now;
        } else {
            SNAPSHOTS_LAST_SECOND.incrementAndGet();
        }
    }
}
