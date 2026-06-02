package feed_filter;

/**
 * Exchange IDs aligned with manager.symbol.exchange (DB naming policy).
 * Spot: KUCOIN, MEXC, BITGET, BINANCE, GATE
 * Futures: BINANCEFUT, BYBITFUT, GATEFUT
 * Subaccounts: {EXCHANGE}8SUB{N} - use feedExchange in config to map to main feed
 * Additional implemented feeds: KRAKEN, BITVAVO, BITKUB
 */
public enum Exchange {
    // spot (main accounts)
    KUCOIN,
    MEXC,
    BITGET,
    BINANCE,
    GATE,

    // spot subaccounts (trading only, no feed - use feedExchange)
    KUCOIN8SUB0,
    MEXC8SUB0,
    BITGET8SUB0,
    GATE8SUB0,

    // futures (main accounts)
    BINANCEFUT,
    BYBITFUT,
    GATEFUT,

    // futures subaccounts
    GATEFUT8SUB0,
    BINANCEFUT8SUB0,   // Binance Futures subaccount, feed from BINANCEFUT
    
    // Binance Futures via Finandy broker (uses BINANCEFUT feed)
    BINANCEFUT8SUBFIN,

    // other implemented feeds in repo
    BITVAVO,
    BITVAVO8SUB0,   // trading subaccount, feed from BITVAVO
    KRAKEN,
    BITKUB
}
