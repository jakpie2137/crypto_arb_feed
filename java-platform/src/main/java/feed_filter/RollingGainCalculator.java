// RollingGainCalculator.java

package feed_filter;

import manager_client.ManagerSymbol;
import manager_client.SymbolRegistry;

import java.math.BigDecimal;
import java.util.List;

/**
 * Computes rolling (time-window) average gain percentage for a given
 * target quote volume, based on order book history stored in
 * InMemoryOrderBookStore.
 *
 * This does NOT apply any filtering on sign of the gain; it simply
 * averages the per-snapshot VWAP gain% (where missing/invalid snapshots
 * yield NaN and are skipped).
 */
public final class RollingGainCalculator {

    private RollingGainCalculator() {
        // utility class
    }

    public static final class RollingGainMetrics {
        private final Exchange srcExchange;
        private final Exchange refExchange;
        private final String symbol;
        private final BigGainSnapshot.Side side;
        private final double targetVolumeQuote;
        private final double avgGainPct;
        private final int observations;

        public RollingGainMetrics(Exchange srcExchange,
                                  Exchange refExchange,
                                  String symbol,
                                  BigGainSnapshot.Side side,
                                  double targetVolumeQuote,
                                  double avgGainPct,
                                  int observations) {
            this.srcExchange = srcExchange;
            this.refExchange = refExchange;
            this.symbol = symbol;
            this.side = side;
            this.targetVolumeQuote = targetVolumeQuote;
            this.avgGainPct = avgGainPct;
            this.observations = observations;
        }

        public Exchange getSrcExchange() {
            return srcExchange;
        }

        public Exchange getRefExchange() {
            return refExchange;
        }

        public String getSymbol() {
            return symbol;
        }

        public BigGainSnapshot.Side getSide() {
            return side;
        }

        public double getTargetVolumeQuote() {
            return targetVolumeQuote;
        }

        public double getAvgGainPct() {
            return avgGainPct;
        }

        public int getObservations() {
            return observations;
        }
    }

    /**
     * Compute rolling average gain% over the last {@code windowMillis}
     * using order book history from the given store.
     *
     * For each source snapshot in the window, we align it with the latest
     * reference snapshot whose timestamp is <= source timestamp, and
     * compute a per-snapshot VWAP gain% for {@code targetVolumeQuote}.
     * The rolling average is a simple arithmetic mean of all valid
     * per-snapshot gains.
     *
     * @param symbolRegistry When non-null, fee rates from manager.symbol are used. Otherwise totalFeesPct/2.
     * Returns null if there is not enough data.
     */
    public static RollingGainMetrics computeRollingGain(
            OrderBookStore store,
            Exchange srcExchange,
            Exchange refExchange,
            String symbol,
            BigGainSnapshot.Side side,
            double totalFeesPct,
            double targetVolumeQuote,
            long windowMillis,
            SymbolRegistry symbolRegistry
    ) {
        if (!(store instanceof InMemoryOrderBookStore)) {
            return null;
        }
        if (srcExchange == null || refExchange == null || symbol == null || side == null) {
            return null;
        }

        double defaultFee = totalFeesPct / 2.0;
        double feeSource;
        double feeRef;
        if (symbolRegistry != null) {
            ManagerSymbol msSrc = symbolRegistry.getByInternalOrMain(srcExchange, symbol);
            ManagerSymbol msRef = symbolRegistry.getByInternalOrMain(refExchange, symbol);
            if (side == BigGainSnapshot.Side.ASK) {
                feeSource = toFee(msSrc != null ? msSrc.feeMarketBuy : null, defaultFee);
                feeRef = toFee(msRef != null ? msRef.feeMarketSell : null, defaultFee);
            } else {
                feeSource = toFee(msSrc != null ? msSrc.feeMarketSell : null, defaultFee);
                feeRef = toFee(msRef != null ? msRef.feeMarketBuy : null, defaultFee);
            }
        } else {
            feeSource = defaultFee;
            feeRef = defaultFee;
        }

        InMemoryOrderBookStore ims = (InMemoryOrderBookStore) store;

        java.util.List<InMemoryOrderBookStore.TimedOrderBookSnapshot> srcWindow =
                ims.getWindow(srcExchange, symbol, windowMillis);
        java.util.List<InMemoryOrderBookStore.TimedOrderBookSnapshot> refWindow =
                ims.getWindow(refExchange, symbol, windowMillis);

        if (srcWindow.isEmpty() || refWindow.isEmpty()) {
            return null;
        }

        int iRef = 0;
        int nRef = refWindow.size();
        double sumPct = 0.0;
        int count = 0;

        for (InMemoryOrderBookStore.TimedOrderBookSnapshot srcTs : srcWindow) {
            long ts = srcTs.getTsMillis();

            // Advance reference index to the latest snapshot with ts <= current source ts
            while (iRef + 1 < nRef && refWindow.get(iRef + 1).getTsMillis() <= ts) {
                iRef++;
            }

            InMemoryOrderBookStore.TimedOrderBookSnapshot refTs = refWindow.get(iRef);
            if (refTs.getTsMillis() > ts) {
                // Reference snapshot is newer than source snapshot -> skip
                continue;
            }

            OrderBookSnapshot srcSnap = srcTs.getSnapshot();
            OrderBookSnapshot refSnap = refTs.getSnapshot();

            double pct = BigGainScanner.computeVwapGainPctForVolume(
                    srcSnap,
                    refSnap,
                    side,
                    feeSource,
                    feeRef,
                    targetVolumeQuote
            );

            if (!Double.isNaN(pct)) {
                sumPct += pct;
                count++;
            }
        }

        if (count == 0) {
            return new RollingGainMetrics(
                    srcExchange,
                    refExchange,
                    symbol,
                    side,
                    targetVolumeQuote,
                    Double.NaN,
                    0
            );
        }

        double avgPct = sumPct / count;

        return new RollingGainMetrics(
                srcExchange,
                refExchange,
                symbol,
                side,
                targetVolumeQuote,
                avgPct,
                count
        );
    }

    private static double toFee(BigDecimal fee, double defaultFee) {
        if (fee == null) return defaultFee;
        double d = fee.doubleValue();
        return (d >= 0.0 && d <= 1.0) ? d : defaultFee;
    }
}
