package feed_filter.exchanges.kucoin;

import feed_filter.MarketDataFeed;
import feed_filter.FeedFilterApp;
import feed_filter.OrderBookStore;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * KuCoin Spot feed with WS sharding.
 *
 * Uses global settings from FeedFilterApp:
 * - SYMBOL_LIMIT
 * - FeedFilterApp.STALE_THRESHOLD
 *
 * Shard size is a single constant in this file (edit 1 line): SYMBOLS_PER_WS.
 */
public class KucoinSpotFeed implements MarketDataFeed {

    public record Sub(String internalSymbol, String externalSymbol) {}

    // [UPDATED] Reduced to 30 as requested to prevent connection overload/resets
    private static final int SYMBOLS_PER_WS = 30;

    private final List<KucoinSpotBatchFeed> shards = new ArrayList<>();

    public KucoinSpotFeed(List<Sub> subs,
                          OrderBookStore store,
                          Function<String, String> externalToInternal,
                          Duration healthPrintIntervalIgnored) {
        Objects.requireNonNull(subs, "subs");
        Objects.requireNonNull(store, "store");
        Objects.requireNonNull(externalToInternal, "externalToInternal");

        if (subs.isEmpty()) {
            System.out.println("[KUCOIN] no subs; feed will not start");
            return;
        }

        int limit = Math.max(1, FeedFilterApp.SYMBOL_LIMIT);
        int n = Math.min(limit, subs.size());
        List<Sub> capped = subs.subList(0, n);

        int shardSize = Math.max(1, SYMBOLS_PER_WS);
        int shardId = 0;
        for (int i = 0; i < capped.size(); i += shardSize) {
            int j = Math.min(capped.size(), i + shardSize);
            List<Sub> part = capped.subList(i, j);
            shards.add(new KucoinSpotBatchFeed(
                    shardId++,
                    part,
                    store,
                    externalToInternal,
                    FeedFilterApp.STALE_THRESHOLD
            ));
        }

        System.out.println("[KUCOIN] init shards=" + shards.size()
                + " symbolsPerWs=" + shardSize
                + " totalSymbols=" + capped.size()
                + " (cappedFrom=" + subs.size()
                + " limit=" + limit + ")"
                + " staleMs=" + FeedFilterApp.STALE_THRESHOLD.toMillis());
        
        // Log symbol list
        StringBuilder sb = new StringBuilder("[KUCOIN] Symbols: ");
        for (int i = 0; i < capped.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(capped.get(i).internalSymbol());
        }
        System.out.println(sb.toString());
    }

    @Override
    public void start() {
        for (KucoinSpotBatchFeed shard : shards) {
            shard.start();
        }
    }

    @Override
    public void stop() {
        for (KucoinSpotBatchFeed shard : shards) {
            shard.stop();
        }
    }
}