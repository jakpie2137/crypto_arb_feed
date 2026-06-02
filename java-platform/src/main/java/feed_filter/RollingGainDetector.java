// RollingGainDetector.java

package feed_filter;

import db_pg.ArbRollingGainRecord;
import db_pg.MarketDataArbRollingGainWriter;
import manager_client.SymbolRegistry;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Background worker that, every ~30s, computes rolling 30s average
 * VWAP gain%% for a fixed target quote volume for all (exchange, symbol, side)
 * combinations and writes the results into Postgres.
 *
 * Uses:
 * - RollingGainCalculator.computeRollingGain(...)
 * - MarketDataArbRollingGainWriter
 *
 * The goal is to have a dense time series of "typical" gain levels,
 * not only positive opportunities.
 */
public class RollingGainDetector implements Runnable {

    private static final long WINDOW_MILLIS = 30_000L;
    private static final long INTERVAL_MILLIS =
            envLong("ARB_ROLLING_INTERVAL_SEC", 30L) * 1000L;

    // total fees (taker+taker) used in gain calculations
    private static final double TOTAL_FEES_PCT =
            envDouble("ARB_DETECTOR_TOTAL_FEES_PCT", 0.0008);

    // dedicated rolling-volume param, separate from detector target volume
    private static final double ROLLING_TARGET_VOLUME_QUOTE =
            envDouble("ARB_ROLLING_VOLUME_USD", 1000.0);

    private final OrderBookStore store;
    private final List<Exchange> sourceExchanges;
    private final Map<String, Exchange> refExchangeMap;
    private final List<String> symbols;
    private final MarketDataArbRollingGainWriter writer;
    private final SymbolRegistry symbolRegistry;

    private volatile boolean running = true;

    public RollingGainDetector(
            OrderBookStore store,
            Exchange[] exchanges,
            Map<String, Exchange> refExchangeMap,
            List<String> symbols,
            MarketDataArbRollingGainWriter writer,
            SymbolRegistry symbolRegistry
    ) {
        this.store = store;
        this.sourceExchanges = new ArrayList<>();
        this.refExchangeMap = refExchangeMap;
        this.symbols = symbols;
        this.writer = writer;
        this.symbolRegistry = symbolRegistry;

        // all exchanges are treated as potential sources
        for (Exchange ex : exchanges) {
            this.sourceExchanges.add(ex);
        }
    }

    @Override
    public void run() {
        System.out.println("[RollingGainDetector] started. targetVolume=" +
                ROLLING_TARGET_VOLUME_QUOTE + ", feesPct=" + TOTAL_FEES_PCT);

        // Diagnostics: rolling gain needs order book history enabled in InMemoryOrderBookStore.
        if (store instanceof InMemoryOrderBookStore ims) {
            String dbg = ims.debugSizes();
            System.out.println("[RollingGainDetector] Store: " + dbg);
            if (dbg.contains("history=OFF")) {
                System.err.println("[RollingGainDetector] WARNING: OB_KEEP_HISTORY=false -> rolling gain window will be empty and nothing will be written to ob_arb_rolling_gain_30s.");
            }
        } else {
            System.err.println("[RollingGainDetector] WARNING: store is not InMemoryOrderBookStore (" + store.getClass().getName() + ") -> rolling gain computation will return null for all symbols.");
        }

        while (running) {
            long startTs = System.currentTimeMillis();
            List<ArbRollingGainRecord> batch = new ArrayList<>();

            for (String symbol : symbols) {

                Exchange refExchange = refExchangeMap.get(symbol);
                if (refExchange == null) {
                    continue; // Skip symbols without active reference
                }

                for (Exchange srcEx : sourceExchanges) {

                    if (srcEx == refExchange) continue;

                    for (BigGainSnapshot.Side side : BigGainSnapshot.Side.values()) {

                        RollingGainCalculator.RollingGainMetrics m =
                                RollingGainCalculator.computeRollingGain(
                                        store,
                                        srcEx,
                                        refExchange,
                                        symbol,
                                        side,
                                        TOTAL_FEES_PCT,
                                        ROLLING_TARGET_VOLUME_QUOTE,
                                        WINDOW_MILLIS,
                                        symbolRegistry
                                );

                        if (m != null) {
                            batch.add(new ArbRollingGainRecord(
                                    Instant.ofEpochMilli(startTs),
                                    symbol,
                                    srcEx.name(),
                                    refExchange.name(),
                                    side.name(),
                                    ROLLING_TARGET_VOLUME_QUOTE,
                                    m.getAvgGainPct(),
                                    m.getObservations()
                            ));
                        }
                    }
                }
            }

            if (batch.isEmpty()) {
                System.err.println("[RollingGainDetector] Batch is empty (no metrics). This usually means history window is empty (OB_KEEP_HISTORY=false) or no snapshots for src/ref exchange/symbol in last " + (WINDOW_MILLIS / 1000) + "s.");
            }
            writer.insertBatch(batch);

            long elapsed = System.currentTimeMillis() - startTs;
            long sleepMs = INTERVAL_MILLIS - elapsed;
            if (sleepMs < 5_000L) {
                sleepMs = 5_000L;
            }

            try {
                Thread.sleep(sleepMs);
            } catch (InterruptedException ignored) {
            }
        }

        System.out.println("[RollingGainDetector] stopped.");
    }

    public void stop() {
        running = false;
    }

    private static double envDouble(String name, double defaultValue) {
        String v = System.getenv(name);
        if (v == null || v.isEmpty()) {
            return defaultValue;
        }
        try {
            return Double.parseDouble(v);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private static long envLong(String name, long defaultValue) {
        String v = System.getenv(name);
        if (v == null || v.isEmpty()) {
            return defaultValue;
        }
        try {
            return Long.parseLong(v);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }
}