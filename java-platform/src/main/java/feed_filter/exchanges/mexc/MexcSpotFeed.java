package feed_filter.exchanges.mexc;

import feed_filter.MarketDataFeed;
import feed_filter.OrderBookDepthConfig;
import feed_filter.OrderBookStore;

import java.util.Collections;
import java.util.List;

/**
 * MEXC Spot Feed (Wrapper)
 * Location: feed_filter.exchanges.mexc
 */
public class MexcSpotFeed implements MarketDataFeed {

    private final MexcSpotBatchFeed delegate;

    /** Backward-compatible single-symbol constructor. */
    public MexcSpotFeed(String symbol, OrderBookStore store) {
        this(Collections.singletonList(symbol), store, OrderBookDepthConfig.MEXC_SPOT_DEPTH, null);
    }

    /** Preferred batch constructor. */
    public MexcSpotFeed(List<String> externalSymbols, OrderBookStore store, int levels, MexcSpotBatchFeed.SymbolMapper symbolMapper) {
        this.delegate = new MexcSpotBatchFeed(externalSymbols, store, levels, symbolMapper);
    }

    @Override
    public void start() {
        delegate.start();
    }

    @Override
    public void stop() {
        delegate.stop();
    }
}