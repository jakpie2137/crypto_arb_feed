package manager_client;

import feed_filter.Exchange;

/**
 * Exchange IDs are aligned with manager.symbol.exchange.
 * This mapper keeps normalization / forward-compat in one place.
 */
public final class ExchangeMapper {

    private ExchangeMapper() {}

    /** Returns DB exchange id (same as enum name). */
    public static String toDbExchangeId(Exchange exchange) {
        if (exchange == null) return null;
        return exchange.name();
    }

    /** Converts DB exchange id to feed_filter.Exchange (case-insensitive). Returns null if unknown. */
    public static Exchange toFeedExchange(String dbExchangeId) {
        if (dbExchangeId == null) return null;
        String x = dbExchangeId.trim().toUpperCase();

        // Backward-compat aliases (if any legacy data exists)
        if (x.equals("BINANCE_FUT")) x = "BINANCEFUT";
        if (x.equals("BYBIT_FUT")) x = "BYBITFUT";
        if (x.equals("BITGET_SPOT")) x = "BITGET";
        if (x.equals("KUCOIN_SPOT")) x = "KUCOIN";
        if (x.equals("MEXC_SPOT")) x = "MEXC";
        if (x.equals("KRAKEN_SPOT")) x = "KRAKEN";
        if (x.equals("BITVAVO_SPOT")) x = "BITVAVO";
        if (x.equals("BITVAVO8SUB0_SPOT")) x = "BITVAVO8SUB0";
        if (x.equals("GATE_FUT")) x = "GATEFUT";
        // Note: GATEFUT and GATE are separate exchanges now

        try {
            return Exchange.valueOf(x);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }
}
