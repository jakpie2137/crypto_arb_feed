package feed_filter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class FundingRateStore {
    private final Map<String, FundingInfo> latest = new ConcurrentHashMap<>();

    private static String key(Exchange ex, String symbol) {
        return ex.name() + "|" + symbol.toUpperCase();
    }

    public void put(Exchange exchange, String symbol, FundingInfo info) {
        if (exchange == null || symbol == null || info == null) return;
        latest.put(key(exchange, symbol), info);
    }

    public FundingInfo get(Exchange exchange, String symbol) {
        if (exchange == null || symbol == null) return null;
        return latest.get(key(exchange, symbol));
    }
}
