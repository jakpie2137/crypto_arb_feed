// OrderBookSnapshotService.java

package feed_filter;

import db_pg.MarketDataOrderBookSnapshotWriter;
import db_pg.PostgresConfig;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Service robiący snapshoty wszystkich orderbooków co X ms
 * i zrzucający je do Postgresa.
 *
 * Startujesz go raz:
 *   snapshotService.start(intervalMillis);
 * gdzie intervalMillis pochodzi np. z GUI.
 */
public class OrderBookSnapshotService {

    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "orderbook-snapshot-service");
                t.setDaemon(true);
                return t;
            });

    private final OrderBookStore store;
    private final Exchange[] exchanges;
    private final List<String> symbols;
    private final MarketDataOrderBookSnapshotWriter writer;

    private volatile boolean started = false;
    private long ticks = 0L;

    public OrderBookSnapshotService(
            OrderBookStore store,
            Exchange[] exchanges,
            List<String> symbols,
            PostgresConfig config
    ) {
        this.store = store;
        this.exchanges = exchanges;
        this.symbols = symbols;
        this.writer = new MarketDataOrderBookSnapshotWriter(config);
    }

    /**
     * Startuje snapshoty z podanym interwałem (w milisekundach).
     * Jeśli już wystartował, kolejne wywołania są ignorowane.
     */
    public synchronized void start(long intervalMillis) {
        if (started) {
            System.out.println("OrderBookSnapshotService already started, ignoring start()");
            return;
        }
        started = true;

        scheduler.scheduleAtFixedRate(() -> {
            long snapshotTime = System.currentTimeMillis();

            try {
                writer.writeFullSnapshotTick(snapshotTime, store, exchanges, symbols);
            } catch (Exception e) {
                System.err.println("Snapshot tick error: " + e.getMessage());
                e.printStackTrace();
            }

            // co 60 ticków robimy cleanup >24h (przy 3s to ~3min; przy innym interwale to "co 60 snapshotów")
            try {
                ticks++;
                if (ticks % 60 == 0) {
                    writer.cleanupOldSnapshots();
                }
            } catch (Exception e) {
                System.err.println("Cleanup error: " + e.getMessage());
                e.printStackTrace();
            }
        }, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        scheduler.shutdownNow();
        try {
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
