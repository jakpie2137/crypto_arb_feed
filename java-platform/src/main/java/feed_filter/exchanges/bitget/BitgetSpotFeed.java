package feed_filter.exchanges.bitget;

import feed_filter.MarketDataFeed;
import feed_filter.OrderBookStore;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

public class BitgetSpotFeed implements MarketDataFeed {

    public record Sub(String internalSymbol, String externalSymbol) {}

    private final BitgetSpotBatchFeed impl;

    public BitgetSpotFeed(List<Sub> subs,
                          OrderBookStore store,
                          Function<String, String> externalToInternal,
                          Duration staleAfter) {
        Objects.requireNonNull(subs, "subs");
        Objects.requireNonNull(store, "store");
        Objects.requireNonNull(externalToInternal, "externalToInternal");
        this.impl = new BitgetSpotBatchFeed(subs, store, externalToInternal, staleAfter);
        
        // Log symbol list
        System.out.println("[BITGET] init totalSymbols=" + subs.size());
        StringBuilder sb = new StringBuilder("[BITGET] Symbols: ");
        for (int i = 0; i < subs.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(subs.get(i).internalSymbol());
        }
        System.out.println(sb.toString());
    }

    @Override
    public void start() { impl.start(); }

    @Override
    public void stop() { impl.stop(); }
}
