package feed_filter.exchanges.bybitfut;

import feed_filter.Exchange;
import feed_filter.FeedFilterApp;
import feed_filter.FeedLogger;
import feed_filter.FundingInfo;
import feed_filter.FundingRateStore;
import feed_filter.MarketDataFeed;
import feed_filter.OrderBookStore;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class BybitFuturesFeed implements MarketDataFeed {

    public record Sub(String internalSymbol, String externalSymbol) {}

    // Bybit allows generous limits, 50 symbols per connection is safe.
    private static final int SYMBOLS_PER_WS = 50;
    private static final String INSTRUMENTS_URL =
            "https://api.bybit.com/v5/market/instruments-info?category=linear&limit=1000";
    private static final String TICKERS_URL =
            "https://api.bybit.com/v5/market/tickers?category=linear&symbol=";

    private final List<BybitFuturesBatchFeed> shards = new ArrayList<>();
    private final List<Sub> activeSubs = new ArrayList<>();
    private final HttpClient client = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build();
    private final ObjectMapper mapper = new ObjectMapper();
    private final Map<String, Integer> fundingIntervalHoursBySymbol = new ConcurrentHashMap<>();
    private final FundingRateStore fundingStore;
    private final long fundingPollIntervalMs;
    private final ScheduledExecutorService fundingExec;
    private volatile ScheduledFuture<?> fundingTask;

    public BybitFuturesFeed(List<Sub> subs,
                            OrderBookStore store,
                            Function<String, String> externalToInternal,
                            FundingRateStore fundingStore,
                            long fundingPollIntervalMs) {
        Objects.requireNonNull(subs, "subs");
        Objects.requireNonNull(store, "store");
        Objects.requireNonNull(externalToInternal, "externalToInternal");
        this.fundingStore = fundingStore;
        this.fundingPollIntervalMs = fundingPollIntervalMs;
        this.fundingExec = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "bybitfut-funding");
            t.setDaemon(true);
            return t;
        });

        if (subs.isEmpty()) {
            System.out.println("[BYBITFUT] no subs; feed will not start");
            return;
        }

        // Use global limit from App logic
        int limit = Math.max(1, FeedFilterApp.SYMBOL_LIMIT);
        int n = Math.min(limit, subs.size());
        List<Sub> capped = subs.subList(0, n);
        List<Sub> filtered = filterValidSymbols(capped);
        activeSubs.addAll(filtered);

        int shardId = 0;
        for (int i = 0; i < filtered.size(); i += SYMBOLS_PER_WS) {
            int j = Math.min(filtered.size(), i + SYMBOLS_PER_WS);
            List<Sub> part = filtered.subList(i, j);
            shards.add(new BybitFuturesBatchFeed(
                    shardId++,
                    part,
                    store,
                    externalToInternal
            ));
        }

        System.out.println("[BYBITFUT] init shards=" + shards.size()
                + " symbolsPerWs=" + SYMBOLS_PER_WS
                + " totalSymbols=" + filtered.size());
        
        // Log symbol list
        StringBuilder sb = new StringBuilder("[BYBITFUT] Symbols: ");
        for (int i = 0; i < filtered.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(filtered.get(i).internalSymbol());
        }
        System.out.println(sb.toString());
    }

    private List<Sub> filterValidSymbols(List<Sub> subs) {
        Set<String> valid = fetchValidLinearSymbols();
        if (valid == null || valid.isEmpty()) {
            System.out.println("[BYBITFUT] instrument prefilter skipped (empty list)");
            return subs;
        }

        List<Sub> filtered = new ArrayList<>();
        List<String> removed = new ArrayList<>();
        for (Sub s : subs) {
            if (valid.contains(s.externalSymbol())) {
                filtered.add(s);
            } else {
                removed.add(s.internalSymbol());
            }
        }

        if (!removed.isEmpty()) {
            int show = Math.min(10, removed.size());
            System.out.println("[BYBITFUT] filtered invalid symbols: " + removed.size()
                    + " (e.g. " + removed.subList(0, show) +
                    (removed.size() > show ? " ...)" : ")"));
        }

        return filtered.isEmpty() ? subs : filtered;
    }

    private Set<String> fetchValidLinearSymbols() {
        Set<String> symbols = new HashSet<>();
        String cursor = "";

        try {
            while (true) {
                String url = INSTRUMENTS_URL + (cursor.isEmpty() ? "" : "&cursor=" + cursor);
                HttpRequest req = HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .timeout(Duration.ofSeconds(10))
                        .GET()
                        .build();

                HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
                if (resp.statusCode() != 200) break;

                JsonNode root = mapper.readTree(resp.body());
                if (root.get("retCode").asInt() != 0) break;

                JsonNode result = root.get("result");
                if (result == null) break;

                JsonNode list = result.get("list");
                if (list != null && list.isArray()) {
                    for (JsonNode item : list) {
                        JsonNode sym = item.get("symbol");
                        if (sym != null) {
                            String symbol = sym.asText();
                            symbols.add(symbol);
                            Integer interval = parseFundingIntervalHours(item);
                            if (interval != null) {
                                fundingIntervalHoursBySymbol.put(symbol, interval);
                            }
                        }
                    }
                }

                JsonNode next = result.get("nextPageCursor");
                String nextCursor = (next == null) ? "" : next.asText("");
                if (nextCursor == null || nextCursor.isEmpty() || nextCursor.equals(cursor)) break;
                cursor = nextCursor;
            }
        } catch (Exception e) {
            System.out.println("[BYBITFUT] instrument prefilter failed: " + e.getMessage());
        }

        return symbols;
    }

    private Integer parseFundingIntervalHours(JsonNode item) {
        JsonNode intervalNode = item.get("fundingInterval");
        if (intervalNode == null || intervalNode.isNull()) return null;
        String val = intervalNode.asText();
        if (val == null || val.isEmpty()) return null;
        try {
            int minutes = Integer.parseInt(val);
            if (minutes <= 0) return null;
            return (int) Math.round(minutes / 60.0);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    @Override
    public void start() {
        for (BybitFuturesBatchFeed shard : shards) {
            shard.start();
        }
        startFundingPoller();
    }

    @Override
    public void stop() {
        for (BybitFuturesBatchFeed shard : shards) {
            shard.stop();
        }
        stopFundingPoller();
    }

    private void startFundingPoller() {
        if (fundingStore == null || fundingPollIntervalMs <= 0 || activeSubs.isEmpty()) return;
        if (fundingTask != null) return;
        fundingTask = fundingExec.scheduleAtFixedRate(
                this::pollFundingRates,
                1000L,
                fundingPollIntervalMs,
                TimeUnit.MILLISECONDS
        );
    }

    private void stopFundingPoller() {
        ScheduledFuture<?> task = fundingTask;
        fundingTask = null;
        if (task != null) {
            try { task.cancel(false); } catch (Exception ignored) {}
        }
        fundingExec.shutdownNow();
    }

    public void refreshFundingNow() {
        if (fundingStore == null || activeSubs.isEmpty()) return;
        fundingExec.execute(this::pollFundingRates);
    }

    private void pollFundingRates() {
        if (activeSubs.isEmpty() || fundingStore == null) return;
        int ok = 0;
        int fail = 0;
        for (Sub s : activeSubs) {
            FundingInfo info = fetchFundingInfo(s.externalSymbol());
            if (info != null) {
                fundingStore.put(Exchange.BYBITFUT, s.internalSymbol(), info);
                ok++;
            } else {
                fail++;
            }
        }
        FeedLogger.info("BYBITFUT", "Funding poll: ok=" + ok + " fail=" + fail);
    }

    private FundingInfo fetchFundingInfo(String externalSymbol) {
        if (externalSymbol == null || externalSymbol.isEmpty()) return null;
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(TICKERS_URL + externalSymbol))
                    .timeout(Duration.ofSeconds(5))
                    .GET()
                    .build();
            HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) return null;
            JsonNode root = mapper.readTree(resp.body());
            if (root.get("retCode").asInt() != 0) return null;
            JsonNode result = root.get("result");
            if (result == null) return null;
            JsonNode list = result.get("list");
            if (list == null || !list.isArray() || list.size() == 0) return null;
            JsonNode item = list.get(0);
            double rate = item.path("fundingRate").asDouble(Double.NaN);
            if (Double.isNaN(rate)) return null;
            long nextFundingTime = item.path("nextFundingTime").asLong(0L);
            Integer interval = fundingIntervalHoursBySymbol.get(externalSymbol);
            Long nextTs = nextFundingTime > 0 ? nextFundingTime : null;
            long sourceTs = System.currentTimeMillis();
            return new FundingInfo(rate, interval, nextTs, null, sourceTs);
        } catch (Exception e) {
            return null;
        }
    }
}