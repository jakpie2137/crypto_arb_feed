package feed_filter;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Minimal FX / stablecoin cross router for theoretical arb sizing.
 *
 * Primary:
 *  - USD/USDT: Kraken (with Binance as secondary)
 *  - EUR/USD : Kraken (dense WS)
 *
 * Emergency fallback for EUR/USD:
 *  - Frankfurter (ECB reference), only after we try to revive Kraken and fail:
 *      - 3 failures
 *      - AND at least 5 minutes since the first failure in the current failure window
 *
 * Note: This is for theoretical arb sizing (not FX execution).
 */
public final class FxRates {

    public static final class Quote {
        public final double bid;
        public final double ask;
        public final long tsMs;
        public final String source;

        public Quote(double bid, double ask, long tsMs, String source) {
            this.bid = bid;
            this.ask = ask;
            this.tsMs = tsMs;
            this.source = source;
        }

        public double mid() {
            return (bid + ask) * 0.5;
        }

        public boolean isValid() {
            return bid > 0 && ask > 0 && Double.isFinite(bid) && Double.isFinite(ask);
        }
    }

    // USD/USDT (how many USDT for 1 USD)
    private static final AtomicReference<Quote> USDUSDT_BINANCE = new AtomicReference<>();
    private static final AtomicReference<Quote> USDUSDT_KRAKEN  = new AtomicReference<>();

    // EUR/USD (how many USD for 1 EUR)
    private static final AtomicReference<Quote> EURUSD_KRAKEN   = new AtomicReference<>();
    private static final AtomicReference<Quote> EURUSD_REF      = new AtomicReference<>();

    // Staleness thresholds
    private static final long STALE_USDUSDT_MS =
            Long.parseLong(System.getenv().getOrDefault("FX_USDUSDT_STALE_MS", "60000")); // 60s

    private static final long STALE_EURUSD_KRAKEN_MS =
            Long.parseLong(System.getenv().getOrDefault("FX_EURUSD_KRAKEN_STALE_MS", "60000")); // 60s

    // reference can be "stale" for a long time - it's an emergency anchor
    private static final long STALE_EURUSD_REF_MS  =
            Long.parseLong(System.getenv().getOrDefault("FX_EURUSD_REF_STALE_MS",  "86400000")); // 24h

    // Emergency switching policy
    private static final int  EURUSD_FAILS_TO_FORCE_REF =
            Integer.parseInt(System.getenv().getOrDefault("FX_EURUSD_FAILS_TO_FORCE_REF", "3"));
    private static final long EURUSD_FAIL_WINDOW_MS =
            Long.parseLong(System.getenv().getOrDefault("FX_EURUSD_FAIL_WINDOW_MS", "300000")); // 5 min

    private static final AtomicLong eurUsdFirstFailTs = new AtomicLong(0L);
    private static final AtomicLong eurUsdFailCount   = new AtomicLong(0L);
    private static final AtomicLong eurUsdForcedRefTs = new AtomicLong(0L); // 0 => not forced

    private FxRates() {}

    // -------- USD/USDT --------

    public static void updateUsdUsdtFromBinance(double usdPerUsdtBid, double usdPerUsdtAsk, long tsMs) {
        // Incoming is USDT/USD (USD per 1 USDT). We store USD/USDT (USDT per 1 USD).
        if (!(usdPerUsdtBid > 0 && usdPerUsdtAsk > 0)) return;

        // Convert USDT/USD -> USD/USDT conservatively:
        // USD/USDT bid = 1 / (USDT/USD ask)
        // USD/USDT ask = 1 / (USDT/USD bid)
        double bid = 1.0 / usdPerUsdtAsk;
        double ask = 1.0 / usdPerUsdtBid;

        if (!(bid > 0 && ask > 0)) return;
        USDUSDT_BINANCE.set(new Quote(bid, ask, tsMs, "BINANCE"));
    }

    public static void updateUsdUsdtFromKraken(double usdPerUsdtBid, double usdPerUsdtAsk, long tsMs) {
        if (!(usdPerUsdtBid > 0 && usdPerUsdtAsk > 0)) return;

        double bid = 1.0 / usdPerUsdtAsk;
        double ask = 1.0 / usdPerUsdtBid;

        if (!(bid > 0 && ask > 0)) return;
        USDUSDT_KRAKEN.set(new Quote(bid, ask, tsMs, "KRAKEN"));
    }

    private static Quote bestUsdUsdt() {
        long now = System.currentTimeMillis();
        Quote k = USDUSDT_KRAKEN.get();
        Quote b = USDUSDT_BINANCE.get();

        Quote best = null;

        if (k != null && k.isValid() && (now - k.tsMs) <= STALE_USDUSDT_MS) best = k;
        if (best == null && b != null && b.isValid() && (now - b.tsMs) <= STALE_USDUSDT_MS) best = b;

        // if both stale, return freshest valid (still lets caller decide)
        if (best == null) {
            Quote cand1 = (k != null && k.isValid()) ? k : null;
            Quote cand2 = (b != null && b.isValid()) ? b : null;
            if (cand1 == null) return cand2;
            if (cand2 == null) return cand1;
            return (cand1.tsMs >= cand2.tsMs) ? cand1 : cand2;
        }
        return best;
    }

    // -------- EUR/USD --------

    public static void updateEurUsdFromKraken(double eurUsdBid, double eurUsdAsk, long tsMs) {
        if (!(eurUsdBid > 0 && eurUsdAsk > 0)) return;
        EURUSD_KRAKEN.set(new Quote(eurUsdBid, eurUsdAsk, tsMs, "KRAKEN"));
        reportEurUsdKrakenSuccess();
    }

    public static void updateEurUsdRef(double eurUsdMid, long tsMs, String source) {
        if (!(eurUsdMid > 0) || !Double.isFinite(eurUsdMid)) return;
        // No real bid/ask here; treat as tiny spread around mid.
        double bid = eurUsdMid * 0.99995;
        double ask = eurUsdMid * 1.00005;
        EURUSD_REF.set(new Quote(bid, ask, tsMs, source == null ? "REF" : source));
    }

    public static void reportEurUsdKrakenFailure(String why) {
        long now = System.currentTimeMillis();
        long first = eurUsdFirstFailTs.get();
        if (first == 0L || (now - first) > EURUSD_FAIL_WINDOW_MS) {
            // start a new failure window
            eurUsdFirstFailTs.set(now);
            eurUsdFailCount.set(1L);
        } else {
            eurUsdFailCount.incrementAndGet();
        }

        long fails = eurUsdFailCount.get();
        long age   = now - eurUsdFirstFailTs.get();

        if (fails >= EURUSD_FAILS_TO_FORCE_REF && age >= EURUSD_FAIL_WINDOW_MS) {
            // Force fallback to reference until Kraken recovers with a valid update.
            if (eurUsdForcedRefTs.get() == 0L) {
                eurUsdForcedRefTs.set(now);
                System.err.println("[FX] EUR/USD forcing REF fallback (fails=" + fails + ", windowAgeMs=" + age + ") reason=" + why);
            }
        }
    }

    public static void reportEurUsdKrakenSuccess() {
        // If Kraken provides fresh data, clear forced fallback and failure window.
        eurUsdFirstFailTs.set(0L);
        eurUsdFailCount.set(0L);
        if (eurUsdForcedRefTs.getAndSet(0L) != 0L) {
            System.out.println("[FX] EUR/USD Kraken recovered -> back to KRAKEN");
        }
    }

    public static boolean isEurUsdRefForced() {
        return eurUsdForcedRefTs.get() != 0L;
    }

    private static Quote bestEurUsd() {
        long now = System.currentTimeMillis();
        Quote k = EURUSD_KRAKEN.get();
        Quote r = EURUSD_REF.get();

        boolean kFresh = (k != null && k.isValid() && (now - k.tsMs) <= STALE_EURUSD_KRAKEN_MS);
        boolean rFresh = (r != null && r.isValid() && (now - r.tsMs) <= STALE_EURUSD_REF_MS);

        if (!isEurUsdRefForced() && kFresh) return k;
        if (rFresh) return r;

        // if ref missing, allow Kraken even if forced but it's the only thing we have
        if (k != null && k.isValid()) return k;
        return r;
    }

    /**
     * Conversion factor to USDT for a price quoted in given currency.
     * Example:
     *  - price in USD -> multiply by USD/USDT
     *  - price in EUR -> multiply by EUR/USD * USD/USDT
     */
    public static double toUsdtFactor(String quoteCcy) {
        if (quoteCcy == null) return Double.NaN;
        String c = quoteCcy.trim().toUpperCase();

        if ("USDT".equals(c)) return 1.0;

        Quote usdUsdt = bestUsdUsdt();
        if (usdUsdt == null || !usdUsdt.isValid()) return Double.NaN;

        if ("USD".equals(c)) {
            return usdUsdt.mid();
        }

        if ("EUR".equals(c)) {
            Quote eurUsd = bestEurUsd();
            if (eurUsd == null || !eurUsd.isValid()) return Double.NaN;
            return eurUsd.mid() * usdUsdt.mid();
        }

        return Double.NaN;
    }

    public static String diagnostics() {
        long now = System.currentTimeMillis();
        Quote uB = USDUSDT_BINANCE.get();
        Quote uK = USDUSDT_KRAKEN.get();
        Quote eK = EURUSD_KRAKEN.get();
        Quote eR = EURUSD_REF.get();

        return "USD/USDT kraken=" + fmt(uK, now) +
                " binance=" + fmt(uB, now) +
                " EUR/USD kraken=" + fmt(eK, now) +
                " ref=" + fmt(eR, now) +
                " eurUsdForcedRef=" + isEurUsdRefForced() +
                " eurUsdFailCount=" + eurUsdFailCount.get();
    }

    private static String fmt(Quote q, long now) {
        if (q == null) return "null";
        return String.format("%.6f (ageMs=%d src=%s)", q.mid(), (now - q.tsMs), q.source);
    }
}
