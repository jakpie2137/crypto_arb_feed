package db_pg;

import feed_filter.Exchange;
import feed_filter.OrderBookLevel;
import feed_filter.OrderBookSnapshot;
import feed_filter.OrderBookStore;

import java.sql.*;
import java.time.Instant;
import java.util.List;

/**
 * Zapisuje snapshoty orderbooków do market_data.orderbook_snapshots
 * trzymając JEDNO połączenie do bazy i dwa PreparedStatementy.
 */
public class MarketDataOrderBookSnapshotWriter implements AutoCloseable {

    private final PostgresConfig config;

    private Connection conn;
    private PreparedStatement insertStmt;
    private PreparedStatement cleanupStmt;

    private static final String INSERT_SQL =
            "INSERT INTO market_data.orderbook_snapshots " +
                    "(snapshot_time, exchange, symbol, event_time, bids, asks) " +
                    "VALUES (?, ?, ?, ?, ?::jsonb, ?::jsonb)";

    private final int retentionMinutes;

    public MarketDataOrderBookSnapshotWriter(PostgresConfig config) {
        this.config = config;
        this.retentionMinutes = envInt("DENSE_SNAPSHOT_RETENTION_MINUTES", 15);
    }

    private String cleanupSql() {
        return "DELETE FROM market_data.orderbook_snapshots " +
                "WHERE snapshot_time < NOW() - INTERVAL '" + retentionMinutes + " minutes'";
    }

    private void ensureConnection() throws SQLException {
        if (conn != null && !conn.isClosed()) {
            try {
                if (conn.isValid(2)) {
                    return;
                }
            } catch (SQLFeatureNotSupportedException ignored) {
                // nie wszystkie drivery wspierają isValid, ale to nie dramat
                return;
            }
        }

        close(); // czyści stare jeśli coś było

        conn = DriverManager.getConnection(config.url, config.user, config.password);
        conn.setAutoCommit(false);

        insertStmt = conn.prepareStatement(INSERT_SQL);
        cleanupStmt = conn.prepareStatement(cleanupSql());
    }

    /**
     * Jeden tick: snapshot_time wspólny dla WSZYSTKICH (exchange, symbol).
     */
    public void writeFullSnapshotTick(
            long snapshotTimeMillis,
            OrderBookStore store,
            Exchange[] exchanges,
            List<String> symbols
    ) {
        try {
            ensureConnection();
            insertStmt.clearBatch();

            Timestamp snapshotTs = Timestamp.from(Instant.ofEpochMilli(snapshotTimeMillis));

            for (Exchange ex : exchanges) {
                for (String symbol : symbols) {
                    OrderBookSnapshot snap = store.get(ex, symbol);
                    if (snap == null) {
                        continue; // brak danych dla tej pary
                    }

                    String bidsJson = levelsToJson(snap.getBids());
                    String asksJson = levelsToJson(snap.getAsks());

                    insertStmt.setTimestamp(1, snapshotTs);
                    insertStmt.setString(2, ex.name());
                    insertStmt.setString(3, symbol);

                    long evMillis = snap.getEventTimeMillis();
                    // Always have an event_time in DB:
                    //  - prefer exchange-provided eventTimeMillis when > 0
                    //  - otherwise fall back to the local snapshot timestamp
                    Timestamp eventTs = evMillis > 0L ? new Timestamp(evMillis) : snapshotTs;
                    insertStmt.setTimestamp(4, eventTs);

                    insertStmt.setString(5, bidsJson);
                    insertStmt.setString(6, asksJson);

                    insertStmt.addBatch();
                }
            }

            insertStmt.executeBatch();
            conn.commit();

        } catch (SQLException e) {
            System.err.println("Error while writing orderbook snapshots: " + e.getMessage());
            e.printStackTrace();
            tryRollbackQuiet();
        }
    }

    /**
     * Czyści dane starsze niż retention (domyślnie 15 min, env DENSE_SNAPSHOT_RETENTION_MINUTES).
     */
    public void cleanupOldSnapshots() {
        try {
            ensureConnection();
            int deleted = cleanupStmt.executeUpdate();
            conn.commit();
            if (deleted > 0) {
                System.out.println("orderbook_snapshots cleanup: deleted " + deleted + " rows");
            }
        } catch (SQLException e) {
            System.err.println("Cleanup error: " + e.getMessage());
            e.printStackTrace();
            tryRollbackQuiet();
        }
    }

    private void tryRollbackQuiet() {
        if (conn == null) return;
        try {
            conn.rollback();
        } catch (SQLException ignored) {
        }
    }

    @Override
    public void close() {
        try { if (insertStmt != null) insertStmt.close(); } catch (Exception ignored) {}
        try { if (cleanupStmt != null) cleanupStmt.close(); } catch (Exception ignored) {}
        try { if (conn != null) conn.close(); } catch (Exception ignored) {}
        insertStmt = null;
        cleanupStmt = null;
        conn = null;
    }

    /**
     * Prosty ręczny JSON: [{ "p":12345.6, "s":0.01 }, ...]
     * Zero zależności, minimalny koszt.
     */
    private String levelsToJson(List<OrderBookLevel> levels) {
        if (levels == null || levels.isEmpty()) {
            return "[]";
        }
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        boolean first = true;
        for (OrderBookLevel lvl : levels) {
            double price = lvl.getPrice();
            double size = lvl.getSize();
            if (price <= 0.0 || size <= 0.0) continue;
            if (!first) sb.append(',');
            first = false;
            sb.append('{')
                    .append("\"p\":").append(price).append(',')
                    .append("\"s\":").append(size)
                    .append('}');
        }
        sb.append(']');
        return sb.toString();
    }

    private static int envInt(String name, int defaultValue) {
        String v = System.getenv(name);
        if (v == null || v.isEmpty()) return defaultValue;
        try {
            return Integer.parseInt(v);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }
}
