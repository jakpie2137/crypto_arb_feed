package db_pg;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Writer for rolling 30s arbitrage gain metrics.
 *
 * Inserts into:
 *   market_data.ob_arb_rolling_gain_30s
 *
 * DB credentials are read from {@link PostgresConfig}.
 *
 * Source of truth (env):
 *   ARB_PG_URL, ARB_PG_USER, ARB_PG_PASS
 */
public class MarketDataArbRollingGainWriter {

    private final PostgresConfig config;

    public MarketDataArbRollingGainWriter(PostgresConfig config) {
        this.config = config;
    }

    private Connection getConnection() throws SQLException {
        PostgresConfig cfg = (config != null) ? config : PostgresConfig.fromEnvOrDefaults();
        if (cfg.url == null || cfg.url.isBlank()) {
            throw new SQLException("Missing ARB_PG_URL (empty/blank)");
        }
        return DriverManager.getConnection(cfg.url, cfg.user, cfg.password);
    }

    public void insertBatch(List<ArbRollingGainRecord> records) {
        if (records == null || records.isEmpty()) {
            // This is how you end up with "3 days no data" and no clue why.
            System.err.println("[MarketDataArbRollingGainWriter] insertBatch called with empty records list - nothing to write.");
            return;
        }

        // ArbRollingGainRecord fields:
        // ts, symbol, srcExchange, refExchange, side, targetVolumeQuote, avgGainPct30s, observationsCount
        String sql = "INSERT INTO market_data.ob_arb_rolling_gain_30s (" +
                "ts, symbol, src_exchange, ref_exchange, side, " +
                "target_volume_quote, avg_gain_pct_30s, observations_count" +
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            conn.setAutoCommit(false);

            for (ArbRollingGainRecord r : records) {
                int i = 1;

                Instant ts = (r.ts != null) ? r.ts : Instant.now();
                ps.setTimestamp(i++, Timestamp.from(ts));

                ps.setString(i++, r.symbol);
                ps.setString(i++, r.srcExchange);
                ps.setString(i++, r.refExchange);
                ps.setString(i++, r.side);

                ps.setDouble(i++, r.targetVolumeQuote);

                if (r.avgGainPct30s != null) {
                    ps.setDouble(i++, r.avgGainPct30s);
                } else {
                    ps.setNull(i++, Types.DOUBLE);
                }

                ps.setInt(i++, r.observationsCount);

                ps.addBatch();
            }

            ps.executeBatch();
            conn.commit();

        } catch (Exception e) {
            System.err.println("[MarketDataArbRollingGainWriter] DB INSERT FAILED: " + e.getMessage());
            e.printStackTrace(System.err);
        }
    }
}
