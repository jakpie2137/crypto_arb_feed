
package db_pg;

import java.time.Instant;

/**
 * DTO for rolling 30s arbitrage gain metrics.
 *
 * Mirrors the schema:
 *  market_data.ob_arb_rolling_gain_30s
 */
public class ArbRollingGainRecord {

    public final Instant ts;
    public final String symbol;
    public final String srcExchange;
    public final String refExchange;
    public final String side; // 'BID' / 'ASK'
    public final double targetVolumeQuote;
    public final Double avgGainPct30s; // can be null / NaN -> NULL in DB
    public final int observationsCount;

    public ArbRollingGainRecord(
            Instant ts,
            String symbol,
            String srcExchange,
            String refExchange,
            String side,
            double targetVolumeQuote,
            Double avgGainPct30s,
            int observationsCount
    ) {
        this.ts = ts;
        this.symbol = symbol;
        this.srcExchange = srcExchange;
        this.refExchange = refExchange;
        this.side = side;
        this.targetVolumeQuote = targetVolumeQuote;
        this.avgGainPct30s = avgGainPct30s;
        this.observationsCount = observationsCount;
    }
}
