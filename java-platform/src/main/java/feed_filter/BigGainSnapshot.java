package feed_filter;

public class BigGainSnapshot {

    public enum Side {
        BID,
        ASK
    }

    private final String symbol;
    private final Exchange exchange;
    private final Side side;

    // Existing metrics (ranking #1)
    private final double firstLevelGainPctLatest;
    private final double totalPositiveGainUsdLatest;
    private final double totalVolumeQuoteLatest;
    private final double avgGainPctTotalLatest;
    private final double minFirstLevelGainPct;
    private final double minTotalPositiveGainUsd;

    // New metrics for extended rankings

    // #2: best_first_line_arb
    private final double firstLevelVolumeQuote;
    private final double firstLevelGainUsd;

    // #3: best_x_dollars_volume_arb
    private final double targetVolumeQuote;
    private final double vwapGainPctForTargetVolume;

    // #4: best_x_dollars_gain_arb
    private final double targetGainUsd;
    private final double volumeQuoteToReachTargetGain;
    private final double gainPctAtTargetGain;

    /**
     * Backwards-compatible constructor matching the original signature.
     * New fields are initialised with NaN so callers that don't care about
     * them can keep using this constructor.
     */
    public BigGainSnapshot(
            String symbol,
            Exchange exchange,
            Side side,
            double firstLevelGainPctLatest,
            double totalPositiveGainUsdLatest,
            double totalVolumeQuoteLatest,
            double avgGainPctTotalLatest,
            double minFirstLevelGainPct,
            double minTotalPositiveGainUsd
    ) {
        this(
                symbol,
                exchange,
                side,
                firstLevelGainPctLatest,
                totalPositiveGainUsdLatest,
                totalVolumeQuoteLatest,
                avgGainPctTotalLatest,
                minFirstLevelGainPct,
                minTotalPositiveGainUsd,
                Double.NaN, // firstLevelVolumeQuote
                Double.NaN, // firstLevelGainUsd
                Double.NaN, // targetVolumeQuote
                Double.NaN, // vwapGainPctForTargetVolume
                Double.NaN, // targetGainUsd
                Double.NaN, // volumeQuoteToReachTargetGain
                Double.NaN  // gainPctAtTargetGain
        );
    }

    /**
     * Full constructor used by the extended scanner once all metrics
     * are computed.
     */
    public BigGainSnapshot(
            String symbol,
            Exchange exchange,
            Side side,
            double firstLevelGainPctLatest,
            double totalPositiveGainUsdLatest,
            double totalVolumeQuoteLatest,
            double avgGainPctTotalLatest,
            double minFirstLevelGainPct,
            double minTotalPositiveGainUsd,
            double firstLevelVolumeQuote,
            double firstLevelGainUsd,
            double targetVolumeQuote,
            double vwapGainPctForTargetVolume,
            double targetGainUsd,
            double volumeQuoteToReachTargetGain,
            double gainPctAtTargetGain
    ) {
        this.symbol = symbol;
        this.exchange = exchange;
        this.side = side;
        this.firstLevelGainPctLatest = firstLevelGainPctLatest;
        this.totalPositiveGainUsdLatest = totalPositiveGainUsdLatest;
        this.totalVolumeQuoteLatest = totalVolumeQuoteLatest;
        this.avgGainPctTotalLatest = avgGainPctTotalLatest;
        this.minFirstLevelGainPct = minFirstLevelGainPct;
        this.minTotalPositiveGainUsd = minTotalPositiveGainUsd;
        this.firstLevelVolumeQuote = firstLevelVolumeQuote;
        this.firstLevelGainUsd = firstLevelGainUsd;
        this.targetVolumeQuote = targetVolumeQuote;
        this.vwapGainPctForTargetVolume = vwapGainPctForTargetVolume;
        this.targetGainUsd = targetGainUsd;
        this.volumeQuoteToReachTargetGain = volumeQuoteToReachTargetGain;
        this.gainPctAtTargetGain = gainPctAtTargetGain;
    }

    public String getSymbol() {
        return symbol;
    }

    public Exchange getExchange() {
        return exchange;
    }

    public Side getSide() {
        return side;
    }

    public double getFirstLevelGainPctLatest() {
        return firstLevelGainPctLatest;
    }

    public double getTotalPositiveGainUsdLatest() {
        return totalPositiveGainUsdLatest;
    }

    public double getTotalVolumeQuoteLatest() {
        return totalVolumeQuoteLatest;
    }

    public double getAvgGainPctTotalLatest() {
        return avgGainPctTotalLatest;
    }

    public double getMinFirstLevelGainPct() {
        return minFirstLevelGainPct;
    }

    public double getMinTotalPositiveGainUsd() {
        return minTotalPositiveGainUsd;
    }

    // New getters

    // #2 – first line
    public double getFirstLevelVolumeQuote() {
        return firstLevelVolumeQuote;
    }

    public double getFirstLevelGainUsd() {
        return firstLevelGainUsd;
    }

    // #3 – VWAP dla targetVolumeQuote
    public double getTargetVolumeQuote() {
        return targetVolumeQuote;
    }

    public double getVwapGainPctForTargetVolume() {
        return vwapGainPctForTargetVolume;
    }

    // #4 – target gain
    public double getTargetGainUsd() {
        return targetGainUsd;
    }

    public double getVolumeQuoteToReachTargetGain() {
        return volumeQuoteToReachTargetGain;
    }

    public double getGainPctAtTargetGain() {
        return gainPctAtTargetGain;
    }
}
