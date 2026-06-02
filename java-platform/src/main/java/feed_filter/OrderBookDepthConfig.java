// OrderBookDepthConfig.java

package feed_filter;

/**
 * Central configuration for target order book depths per exchange/feed.
 *
 * These values control how many levels we *request* from each exchange
 * (if the API supports it). They do NOT limit what the GUI can render –
 * viewer may still cut at its own max rows.
 *
 * Available / typical options per venue (WebSocket depth channels):
 *
 *  - BINANCEFUT:
 *      REST depth: 5, 10, 20, 50, 100, 500, 1000
 *      WS streams: symbol@depth<5/10/20/50/100/500/1000>@interval
 *
 *  - KUCOIN:
 *      level2Depth20, level2Depth100, level2 (full)
 *
 *  - BITGET:
 *      books1, books5, books15, books50
 *
 *  - GATE:
 *      futures.order_book payload: [symbol, limit, interval], where
 *      typical limits are 20, 50, 100 (depends on venue docs)
 *
 *  - MEXC:
 *      spot@public.limit.depth.v3.api(.pb) with "levels" parameter
 *      (commonly 5, 10, 20, 50, 100 depending on symbol/liquidity).
 */
public final class OrderBookDepthConfig {

    private OrderBookDepthConfig() {
        // no instances
    }

    /** Binance futures – we want a compact but useful depth. (default = 10) */
    public static final int BINANCE_FUT_DEPTH = 20;

    /** KuCoin spot – decent local depth without flooding. */
    public static final int KUCOIN_SPOT_DEPTH = 20;

    /** Bitget spot – use full books50, no truncation. */
    public static final int BITGET_SPOT_DEPTH = 20;

    /** Gate futures – request 20 levels from futures.order_book. */
    public static final int GATE_FUT_DEPTH = 20;

    /** MEXC spot – target 50 levels on the protobuf depth feed. */
    public static final int MEXC_SPOT_DEPTH = 20;

    /** Bybit futures (Linear) - API supports 1, 50, 200. We will sub to 50 and trim to 20. */
    public static final int BYBIT_FUT_DEPTH = 20;
}
