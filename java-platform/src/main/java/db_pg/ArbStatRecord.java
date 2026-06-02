package db_pg;

import java.time.Instant;

/**
 * DTO representing a single arbitrage ranking entry to be written to Postgres.
 */
public class ArbStatRecord {

    public final Instant ts;
    public final String symbol;
    public final String srcExchange;
    public final String refExchange;
    public final String side; // "BID" / "ASK"
    public final short rankType;      // 1..4
    public final int rankPosition;    // 1..N within a single scan

    public final Double firstLevelGainPct;
    public final Double firstLevelGainUsd;
    public final Double firstLevelVolumeQuote;

    public final Double totalPositiveGainUsd;
    public final Double totalVolumeQuote;
    public final Double avgGainPctTotal;

    public final Double targetVolumeQuote;
    public final Double vwapGainPctForTargetVolume;

    public final Double targetGainUsd;
    public final Double volumeQuoteToReachTargetGain;
    public final Double gainPctAtTargetGain;

    public ArbStatRecord(
            Instant ts,
            String symbol,
            String srcExchange,
            String refExchange,
            String side,
            short rankType,
            int rankPosition,
            Double firstLevelGainPct,
            Double firstLevelGainUsd,
            Double firstLevelVolumeQuote,
            Double totalPositiveGainUsd,
            Double totalVolumeQuote,
            Double avgGainPctTotal,
            Double targetVolumeQuote,
            Double vwapGainPctForTargetVolume,
            Double targetGainUsd,
            Double volumeQuoteToReachTargetGain,
            Double gainPctAtTargetGain
    ) {
        this.ts = ts;
        this.symbol = symbol;
        this.srcExchange = srcExchange;
        this.refExchange = refExchange;
        this.side = side;
        this.rankType = rankType;
        this.rankPosition = rankPosition;
        this.firstLevelGainPct = firstLevelGainPct;
        this.firstLevelGainUsd = firstLevelGainUsd;
        this.firstLevelVolumeQuote = firstLevelVolumeQuote;
        this.totalPositiveGainUsd = totalPositiveGainUsd;
        this.totalVolumeQuote = totalVolumeQuote;
        this.avgGainPctTotal = avgGainPctTotal;
        this.targetVolumeQuote = targetVolumeQuote;
        this.vwapGainPctForTargetVolume = vwapGainPctForTargetVolume;
        this.targetGainUsd = targetGainUsd;
        this.volumeQuoteToReachTargetGain = volumeQuoteToReachTargetGain;
        this.gainPctAtTargetGain = gainPctAtTargetGain;
    }
}
