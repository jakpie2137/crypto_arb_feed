package feed_filter;

/**
 * Mapping from exchange-native symbol to the hedge/reference symbol used in the app (typically ...USDT).
 * Example:
 *  - Bitvavo:  BTC-EUR  -> BTCUSDT
 *  - Kraken:   BTC/EUR  -> BTCUSDT
 */
public class SymbolMapping {
    public final String exchangeSymbol;
    public final String hedgeSymbol;

    public SymbolMapping(String exchangeSymbol, String hedgeSymbol) {
        this.exchangeSymbol = exchangeSymbol;
        this.hedgeSymbol = hedgeSymbol;
    }
}
