package manager_client;

import db_pg.PostgresConfig;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class SymbolRepository {

    private final PostgresConfig pg;

    public SymbolRepository(PostgresConfig pg) {
        this.pg = pg;
    }

    private Connection open() throws SQLException {
        return DriverManager.getConnection(pg.url, pg.user, pg.password);
    }

    public List<ManagerSymbol> loadAllActive() {
        String sql =
                "SELECT " +
                        "id, exchange, symbol, is_active, base_curr, quote_curr, " +
                        "external_symbol, external_base, external_quote, " +
                        "price_precision, volume_precision, " +
                        "min_vol, min_vol_type, max_vol, max_vol_type, " +
                        "fee_curr_buy, fee_curr_sell, " +
                        "fee_limit_buy, fee_market_buy, fee_limit_sell, fee_market_sell, " +
                        "quotation, symbol_type, contract_size, max_position, max_position_type, leverage " +
                "FROM manager.symbol " +
                "WHERE is_active = true";

        try (Connection c = open();
             PreparedStatement ps = c.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            List<ManagerSymbol> out = new ArrayList<>();
            while (rs.next()) {
                out.add(mapRow(rs));
            }
            return out;

        } catch (SQLException e) {
            throw new RuntimeException("Failed to load active symbols from manager.symbol: " + e.getMessage(), e);
        }
    }

    private static ManagerSymbol mapRow(ResultSet rs) throws SQLException {
        return new ManagerSymbol(
                rs.getLong("id"),
                rs.getString("exchange"),
                rs.getString("symbol"),
                rs.getBoolean("is_active"),
                rs.getString("base_curr"),
                rs.getString("quote_curr"),
                rs.getString("external_symbol"),
                rs.getString("external_base"),
                rs.getString("external_quote"),
                getNullableInt(rs, "price_precision"),
                getNullableInt(rs, "volume_precision"),
                rs.getBigDecimal("min_vol"),
                rs.getString("min_vol_type"),
                rs.getBigDecimal("max_vol"),
                rs.getString("max_vol_type"),
                rs.getString("fee_curr_buy"),
                rs.getString("fee_curr_sell"),
                rs.getBigDecimal("fee_limit_buy"),
                rs.getBigDecimal("fee_market_buy"),
                rs.getBigDecimal("fee_limit_sell"),
                rs.getBigDecimal("fee_market_sell"),
                rs.getBigDecimal("quotation"),
                rs.getString("symbol_type"),
                rs.getBigDecimal("contract_size"),
                rs.getBigDecimal("max_position"),
                rs.getString("max_position_type"),
                rs.getBigDecimal("leverage")
        );
    }

    private static Integer getNullableInt(ResultSet rs, String col) throws SQLException {
        int v = rs.getInt(col);
        return rs.wasNull() ? null : v;
    }
}
