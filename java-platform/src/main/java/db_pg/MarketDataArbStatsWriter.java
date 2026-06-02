package db_pg;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

/**
 * Writer for arbitrage stats snapshots.
 *
 * Uses {@link PostgresConfig} as the single source of truth for DB creds.
 *
 * Source of truth (env):
 *   ARB_PG_URL, ARB_PG_USER, ARB_PG_PASS
 */
public class MarketDataArbStatsWriter {

    private final PostgresConfig config;

    public MarketDataArbStatsWriter(PostgresConfig config) {
        this.config = config;
    }

    private Connection getConnection() throws SQLException {
        PostgresConfig cfg = (config != null) ? config : PostgresConfig.fromEnvOrDefaults();
        if (cfg.url == null || cfg.url.isBlank()) {
            throw new SQLException("Missing ARB_PG_URL (empty/blank)");
        }
        return DriverManager.getConnection(cfg.url, cfg.user, cfg.password);
    }


    public void insertBatch(List<ArbStatRecord> records) {
        if (records == null || records.isEmpty()) {
            return;
        }

        String sql = "INSERT INTO market_data.ob_arb_stats_historic (" +
                "ts, symbol, src_exchange, ref_exchange, side, " +
                "rank_type, rank_position, " +
                "first_level_gain_pct, first_level_gain_usd, first_level_volume_quote, " +
                "total_positive_gain_usd, total_volume_quote, avg_gain_pct_total, " +
                "target_volume_quote, vwap_gain_pct_for_target_volume, " +
                "target_gain_usd, volume_quote_to_reach_target_gain, gain_pct_at_target_gain" +
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            for (ArbStatRecord r : records) {
                int idx = 1;
                Instant ts = (r.ts != null) ? r.ts : Instant.now();
                ps.setTimestamp(idx++, Timestamp.from(ts));
                ps.setString(idx++, r.symbol);
                ps.setString(idx++, r.srcExchange);
                ps.setString(idx++, r.refExchange);
                ps.setString(idx++, r.side);
                ps.setShort(idx++, r.rankType);
                ps.setInt(idx++, r.rankPosition);

                // nullable doubles
                if (r.firstLevelGainPct != null) {
                    ps.setDouble(idx++, r.firstLevelGainPct);
                } else {
                    ps.setNull(idx++, java.sql.Types.DOUBLE);
                }

                if (r.firstLevelGainUsd != null) {
                    ps.setDouble(idx++, r.firstLevelGainUsd);
                } else {
                    ps.setNull(idx++, java.sql.Types.DOUBLE);
                }

                if (r.firstLevelVolumeQuote != null) {
                    ps.setDouble(idx++, r.firstLevelVolumeQuote);
                } else {
                    ps.setNull(idx++, java.sql.Types.DOUBLE);
                }

                if (r.totalPositiveGainUsd != null) {
                    ps.setDouble(idx++, r.totalPositiveGainUsd);
                } else {
                    ps.setNull(idx++, java.sql.Types.DOUBLE);
                }

                if (r.totalVolumeQuote != null) {
                    ps.setDouble(idx++, r.totalVolumeQuote);
                } else {
                    ps.setNull(idx++, java.sql.Types.DOUBLE);
                }

                if (r.avgGainPctTotal != null) {
                    ps.setDouble(idx++, r.avgGainPctTotal);
                } else {
                    ps.setNull(idx++, java.sql.Types.DOUBLE);
                }

                if (r.targetVolumeQuote != null) {
                    ps.setDouble(idx++, r.targetVolumeQuote);
                } else {
                    ps.setNull(idx++, java.sql.Types.DOUBLE);
                }

                if (r.vwapGainPctForTargetVolume != null) {
                    ps.setDouble(idx++, r.vwapGainPctForTargetVolume);
                } else {
                    ps.setNull(idx++, java.sql.Types.DOUBLE);
                }

                if (r.targetGainUsd != null) {
                    ps.setDouble(idx++, r.targetGainUsd);
                } else {
                    ps.setNull(idx++, java.sql.Types.DOUBLE);
                }

                if (r.volumeQuoteToReachTargetGain != null) {
                    ps.setDouble(idx++, r.volumeQuoteToReachTargetGain);
                } else {
                    ps.setNull(idx++, java.sql.Types.DOUBLE);
                }

                if (r.gainPctAtTargetGain != null) {
                    ps.setDouble(idx++, r.gainPctAtTargetGain);
                } else {
                    ps.setNull(idx++, java.sql.Types.DOUBLE);
                }

                ps.addBatch();
            }

            ps.executeBatch();
        } catch (SQLException e) {
            System.err.println("[MarketDataArbStatsWriter] Failed to insert batch: " + e.getMessage());
            e.printStackTrace(System.err);
        }
    }
}

