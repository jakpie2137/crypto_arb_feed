package feed_filter;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Bitvavo Spot feed z podziałem na batche – kilka WebSocket zamiast jednego.
 * Przy >50 symbolach jeden strumień może być przeciążony; batching jak BitgetSpotBatchFeed.
 * Każdy batch = osobne połączenie WS, subscribe do podzbioru rynków.
 *
 * Fault isolation: gdy jeden shard ma problem (stale/silence/gap) – reconnects tylko ten shard,
 * pozostałe ~150 symboli dalej dostają dane. Nie ma full restartu feeda (1–2 min downtime).
 * Logi: [BITVAVO G0], [BITVAVO G1] etc. – widać który shard się restartuje.
 */
public class BitvavoSpotBatchFeed implements MarketDataFeed {

    private final List<BitvavoSpotFeed> shards = new ArrayList<>();

    public BitvavoSpotBatchFeed(OrderBookStore store, Map<String, String> marketToStoreSymbol, int batchSize) {
        if (marketToStoreSymbol == null || marketToStoreSymbol.isEmpty()) {
            System.out.println("[BITVAVO] Batch: empty map, no shards");
            return;
        }
        List<Map.Entry<String, String>> entries = new ArrayList<>(marketToStoreSymbol.entrySet());
        int size = Math.max(1, batchSize);
        for (int i = 0; i < entries.size(); i += size) {
            int end = Math.min(entries.size(), i + size);
            Map<String, String> chunk = new LinkedHashMap<>();
            for (int j = i; j < end; j++) {
                Map.Entry<String, String> e = entries.get(j);
                chunk.put(e.getKey(), e.getValue());
            }
            shards.add(new BitvavoSpotFeed(store, chunk, shards.size()));
        }
        System.out.println("[BITVAVO] Batch: " + shards.size() + " shards, batchSize=" + size + ", total=" + entries.size());
    }

    @Override
    public void start() {
        for (BitvavoSpotFeed shard : shards) {
            shard.start();
        }
    }

    @Override
    public void stop() {
        for (BitvavoSpotFeed shard : shards) {
            shard.stop();
        }
    }
}
