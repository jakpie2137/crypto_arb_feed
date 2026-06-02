package feed_filter;

import db_pg.PostgresConfig;
import manager_client.ManagerSymbol;
import manager_client.SymbolRegistry;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

/**
 * Resolves reference exchange per symbol (futures priority: BINANCEFUT > GATEFUT > BYBITFUT).
 * Shared by GUI, headless detectors, and web API bootstrap.
 */
public final class ReferenceExchangeMap {

    private ReferenceExchangeMap() {}

    public static Map<String, Exchange> load(SymbolRegistry symbolRegistry, String[] symbols) {
        Map<String, Exchange> map = new HashMap<>();
        PostgresConfig pg = PostgresConfig.fromEnvOrDefaults();

        String sql =
                "SELECT symbol, ref_exchange FROM (" +
                "  SELECT symbol, ref_exchange," +
                "    ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY priority) AS rn" +
                "  FROM (" +
                "    SELECT symbol," +
                "      CASE WHEN UPPER(exchange) IN ('BINANCEFUT','BINANCEFUT8SUBFIN','BINANCEFUT8SUB0') THEN 'BINANCEFUT'" +
                "           WHEN UPPER(exchange) IN ('GATEFUT','GATEFUT8SUB0') THEN 'GATEFUT'" +
                "           WHEN UPPER(exchange) = 'BYBITFUT' THEN 'BYBITFUT'" +
                "      END AS ref_exchange," +
                "      CASE WHEN UPPER(exchange) IN ('BINANCEFUT','BINANCEFUT8SUBFIN','BINANCEFUT8SUB0') THEN 1" +
                "           WHEN UPPER(exchange) IN ('GATEFUT','GATEFUT8SUB0') THEN 2" +
                "           WHEN UPPER(exchange) = 'BYBITFUT' THEN 3 ELSE 4 END AS priority" +
                "    FROM manager.symbol" +
                "    WHERE is_active = true" +
                "      AND UPPER(exchange) IN ('BINANCEFUT','BINANCEFUT8SUBFIN','BINANCEFUT8SUB0','GATEFUT','GATEFUT8SUB0','BYBITFUT')" +
                "  ) t1" +
                "  WHERE ref_exchange IS NOT NULL" +
                ") t2 WHERE rn = 1";

        try (Connection conn = DriverManager.getConnection(pg.url, pg.user, pg.password);
             PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                String sym = rs.getString("symbol");
                String exStr = rs.getString("ref_exchange");
                try {
                    map.put(sym, Exchange.valueOf(exStr));
                } catch (IllegalArgumentException e) {
                    System.err.println("[ReferenceExchangeMap] Unknown ref exchange: " + exStr);
                }
            }
        } catch (Exception e) {
            System.err.println("[ReferenceExchangeMap] DB load failed: " + e.getMessage());
        }

        if (map.isEmpty() && symbolRegistry != null) {
            for (Exchange ex : new Exchange[]{Exchange.BINANCEFUT, Exchange.BINANCEFUT8SUBFIN, Exchange.BINANCEFUT8SUB0}) {
                for (ManagerSymbol ms : symbolRegistry.getActiveSymbols(ex)) {
                    map.putIfAbsent(ms.symbol, Exchange.BINANCEFUT);
                }
            }
            for (Exchange ex : new Exchange[]{Exchange.GATEFUT, Exchange.GATEFUT8SUB0}) {
                for (ManagerSymbol ms : symbolRegistry.getActiveSymbols(ex)) {
                    map.putIfAbsent(ms.symbol, Exchange.GATEFUT);
                }
            }
            for (ManagerSymbol ms : symbolRegistry.getActiveSymbols(Exchange.BYBITFUT)) {
                map.putIfAbsent(ms.symbol, Exchange.BYBITFUT);
            }
        }

        if (map.isEmpty() && symbolRegistry != null && symbols != null) {
            Exchange[] refPriority = {
                    Exchange.BINANCEFUT, Exchange.BINANCEFUT8SUBFIN, Exchange.BINANCEFUT8SUB0,
                    Exchange.GATEFUT, Exchange.GATEFUT8SUB0,
                    Exchange.BYBITFUT,
                    Exchange.GATE, Exchange.BINANCE, Exchange.KUCOIN, Exchange.BITGET, Exchange.MEXC
            };
            for (String sym : symbols) {
                if (sym == null || sym.isBlank()) continue;
                for (Exchange ex : refPriority) {
                    ManagerSymbol ms = symbolRegistry.getByInternalOrMain(ex, sym);
                    if (ms != null) {
                        Exchange refEx = (ex == Exchange.BINANCEFUT8SUBFIN || ex == Exchange.BINANCEFUT8SUB0)
                                ? Exchange.BINANCEFUT
                                : (ex == Exchange.GATEFUT8SUB0 ? Exchange.GATEFUT : ex);
                        map.put(sym, refEx);
                        break;
                    }
                }
            }
        }

        System.out.println("[ReferenceExchangeMap] Loaded " + map.size() + " reference mappings.");
        return map;
    }
}
