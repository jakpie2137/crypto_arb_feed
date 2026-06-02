package feed_filter;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

public class InMemoryOrderBookStore implements OrderBookStore {

    private static final long SANITY_LOG_EVERY_MS = 3600_000L;
    private static final boolean KEEP_HISTORY =
            Boolean.parseBoolean(System.getenv().getOrDefault("OB_KEEP_HISTORY", "true"));
    private static final long HISTORY_SAMPLE_MS =
            Long.parseLong(System.getenv().getOrDefault("OB_HISTORY_SAMPLE_MS", "1000"));
    public static final long MAX_HISTORY_MILLIS =
            Long.parseLong(System.getenv().getOrDefault("OB_MAX_HISTORY_MILLIS", "30000"));
    private static final int STATE_MAX_LEVELS =
            Integer.parseInt(System.getenv().getOrDefault("OB_STATE_MAX_LEVELS", "200"));
    private static final int OUT_DEPTH =
            Integer.parseInt(System.getenv().getOrDefault("OB_OUT_DEPTH", "50"));

    private final Map<String, OrderBookSnapshot> latest = new ConcurrentHashMap<>();
    private final Map<String, Deque<TimedOrderBookSnapshot>> history =
            KEEP_HISTORY ? new ConcurrentHashMap<>() : null;
    private final Map<String, Long> lastHistorySampleTs =
            KEEP_HISTORY ? new ConcurrentHashMap<>() : null;

    private static final class BookState {
        final NavigableMap<Double, Double> bids = new TreeMap<>(Comparator.reverseOrder());
        final NavigableMap<Double, Double> asks = new TreeMap<>();
        volatile long lastEventTimeMillis;
    }

    private final Map<String, BookState> state = new ConcurrentHashMap<>();
    private final Map<String, Long> nextSanityLogTs = new ConcurrentHashMap<>();

    public static final class TimedOrderBookSnapshot {
        private final long tsMillis;
        private final OrderBookSnapshot snapshot;
        public TimedOrderBookSnapshot(long tsMillis, OrderBookSnapshot snapshot) {
            this.tsMillis = tsMillis;
            this.snapshot = snapshot;
        }
        public long getTsMillis() { return tsMillis; }
        public OrderBookSnapshot getSnapshot() { return snapshot; }
    }

    private static String key(Exchange ex, String symbol) {
        return ex.name() + "|" + symbol.toUpperCase();
    }

    public void reset(Exchange exchange, String symbol) {
        String k = key(exchange, symbol);
        state.remove(k);
        latest.remove(k);
    }

    /**
     * Standard DELTA update (Merge).
     * Used for exchanges sending differential updates (add/remove/update specific levels).
     */
    @Override
    public void update(OrderBookSnapshot snapshot) {
        processUpdate(snapshot, false);
    }

    /**
     * SNAPSHOT overwrite (Replace).
     * Used for exchanges sending full state (e.g. Binance Partial Depth).
     * Clears previous levels to prevent "Ghost Levels" / Crossed Books.
     */
    public void putSnapshot(OrderBookSnapshot snapshot) {
        processUpdate(snapshot, true);
    }

    private void processUpdate(OrderBookSnapshot snapshot, boolean isFullSnapshot) {
        if (snapshot == null) return;
        String k = key(snapshot.getExchange(), snapshot.getSymbol());

        long nowForLog = System.currentTimeMillis();
        long nextAt = nextSanityLogTs.getOrDefault(k, 0L);
        boolean doPeriodicLog = nowForLog >= nextAt;

        BookState st = state.computeIfAbsent(k, kk -> new BookState());
        OrderBookSnapshot full;

        synchronized (st) {
            // CRITICAL FIX: If this is a full snapshot, clear old state first!
            if (isFullSnapshot) {
                st.bids.clear();
                st.asks.clear();
            }

            mergeSide(st.bids, snapshot.getBids());
            mergeSide(st.asks, snapshot.getAsks());

            long et = snapshot.getEventTimeMillis();
            if (et > 0) st.lastEventTimeMillis = et;

            // Optional: Trim to max levels to save RAM, though partial streams are small usually
            trimSide(st.bids, STATE_MAX_LEVELS);
            trimSide(st.asks, STATE_MAX_LEVELS);

            full = new OrderBookSnapshot(
                    snapshot.getExchange(),
                    snapshot.getSymbol(),
                    st.lastEventTimeMillis > 0 ? st.lastEventTimeMillis : System.currentTimeMillis(),
                    toLevels(st.bids, OUT_DEPTH),
                    toLevels(st.asks, OUT_DEPTH)
            );

            if (isCrossed(full)) {
                if (doPeriodicLog) {
                    System.err.println("[OB][CROSSED][MERGED] " + k + " bestBid=" + bestBid(full)
                            + " bestAsk=" + bestAsk(full));
                    nextSanityLogTs.put(k, nowForLog + SANITY_LOG_EVERY_MS);
                }
            }
        }

        latest.put(k, full);
        // Metrics.onSnapshot(); // Uncomment if you have metrics

        if (!KEEP_HISTORY) return;

        long now = System.currentTimeMillis();
        long last = lastHistorySampleTs.getOrDefault(k, 0L);
        if (now - last < HISTORY_SAMPLE_MS) return;
        lastHistorySampleTs.put(k, now);

        Deque<TimedOrderBookSnapshot> deque =
                history.computeIfAbsent(k, kk -> new ConcurrentLinkedDeque<>());
        deque.addLast(new TimedOrderBookSnapshot(now, full));
        trimOld(deque, now);
    }

    private void trimOld(Deque<TimedOrderBookSnapshot> deque, long nowMillis) {
        long cutoff = nowMillis - MAX_HISTORY_MILLIS;
        while (true) {
            TimedOrderBookSnapshot first = deque.peekFirst();
            if (first == null || first.getTsMillis() >= cutoff) break;
            deque.pollFirst();
        }
    }

    @Override
    public OrderBookSnapshot get(Exchange exchange, String symbol) {
        return latest.get(key(exchange, symbol));
    }

    private static void mergeSide(NavigableMap<Double, Double> side, List<OrderBookLevel> updates) {
        if (updates == null) return;
        for (OrderBookLevel lvl : updates) {
            if (lvl == null) continue;
            double p = lvl.getPrice();
            double s = lvl.getSize();
            // In a full snapshot, size is always > 0.
            // In a delta, size=0 means remove.
            if (!(p > 0)) continue;
            if (s <= 0.0) side.remove(p);
            else side.put(p, s);
        }
    }

    private static void trimSide(NavigableMap<Double, Double> side, int max) {
        if (side.size() <= max) return;
        int i = 0;
        Iterator<Map.Entry<Double, Double>> it = side.entrySet().iterator();
        while (it.hasNext()) {
            it.next();
            if (++i > max) it.remove();
        }
    }

    private static List<OrderBookLevel> toLevels(NavigableMap<Double, Double> side, int depth) {
        List<OrderBookLevel> out = new ArrayList<>();
        int i = 0;
        for (Map.Entry<Double, Double> e : side.entrySet()) {
            out.add(new OrderBookLevel(e.getKey(), e.getValue()));
            if (++i >= depth) break;
        }
        return out;
    }

    private static boolean isCrossed(OrderBookSnapshot s) {
        double bb = bestBid(s);
        double ba = bestAsk(s);
        return bb > 0 && ba > 0 && bb > ba;
    }

    private static double bestBid(OrderBookSnapshot s) {
        List<OrderBookLevel> bids = s.getBids();
        if (bids == null || bids.isEmpty() || bids.get(0) == null) return -1;
        return bids.get(0).getPrice();
    }

    private static double bestAsk(OrderBookSnapshot s) {
        List<OrderBookLevel> asks = s.getAsks();
        if (asks == null || asks.isEmpty() || asks.get(0) == null) return -1;
        return asks.get(0).getPrice();
    }

    public List<TimedOrderBookSnapshot> getWindow(Exchange exchange, String symbol, long windowMillis) {
        if (!KEEP_HISTORY) return Collections.emptyList();
        String k = key(exchange, symbol);
        Deque<TimedOrderBookSnapshot> deque = history.get(k);
        if (deque == null || deque.isEmpty()) return Collections.emptyList();
        long now = System.currentTimeMillis();
        long cutoff = now - windowMillis;
        List<TimedOrderBookSnapshot> result = new ArrayList<>();
        for (TimedOrderBookSnapshot ts : deque) {
            if (ts.getTsMillis() >= cutoff) result.add(ts);
        }
        return result;
    }

    public String debugSizes() {
        if (!KEEP_HISTORY) return "latestKeys=" + latest.size() + " history=OFF";
        long items = 0;
        for (Deque<TimedOrderBookSnapshot> d : history.values()) items += d.size();
        return "latestKeys=" + latest.size() + " historyKeys=" + history.size() + " historyItems=" + items;
    }
}