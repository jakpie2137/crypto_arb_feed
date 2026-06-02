package feed_filter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import feed_filter.config.CrossFeedConfig;
import okhttp3.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Cross feed: Binance spot EUR/USDT bookTicker.
 * Updates CrossRateStore for (feedId, symbol).
 *
 * Healthcheck:
 * - Reconnect when no message for silenceReconnectMs
 * - Reconnect when mid_price unchanged for midPriceStaleReconnectMs
 */
public class EurUsdtBinanceCrossFeed implements MarketDataFeed {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String WS_BASE = "wss://stream.binance.com:9443/ws/";

    private final String feedId;
    private final String symbol;
    private final CrossRateStore store;
    private final long silenceReconnectMs;
    private final long midPriceStaleReconnectMs;

    private final OkHttpClient client;
    private final ScheduledExecutorService ses;
    private volatile WebSocket ws;
    private final AtomicBoolean running = new AtomicBoolean(false);

    private final AtomicLong lastMsgTs = new AtomicLong(0);
    private final AtomicLong lastMidPriceChangeTs = new AtomicLong(0);
    private volatile double lastMidPrice = Double.NaN;

    public EurUsdtBinanceCrossFeed(String feedId, String symbol, CrossRateStore store, CrossFeedConfig config) {
        this.feedId = feedId;
        this.symbol = symbol;
        this.store = store;
        CrossFeedConfig.CrossFeedDefaults def = config.getDefaults();
        this.silenceReconnectMs = def.getSilenceReconnectMs();
        this.midPriceStaleReconnectMs = def.getMidPriceStaleReconnectMs();

        this.client = new OkHttpClient.Builder()
                .pingInterval(java.time.Duration.ofSeconds(20))
                .build();
        this.ses = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "cross-" + feedId + "-" + symbol);
            t.setDaemon(true);
            return t;
        });
    }

    @Override
    public void start() {
        if (!running.compareAndSet(false, true)) return;
        connect();
        ses.scheduleAtFixedRate(this::watchdog, 1, 1, TimeUnit.SECONDS);
    }

    @Override
    public void stop() {
        running.set(false);
        tryClose("stop");
        ses.shutdownNow();
    }

    private void watchdog() {
        if (!running.get()) return;
        long now = System.currentTimeMillis();

        long silence = now - lastMsgTs.get();
        if (lastMsgTs.get() > 0 && silence > silenceReconnectMs) {
            System.err.println("[CROSS " + feedId + " " + symbol + "] silence " + (silence / 1000) + "s -> reconnect");
            reconnect();
            return;
        }

        long noChange = now - lastMidPriceChangeTs.get();
        if (lastMidPriceChangeTs.get() > 0 && noChange > midPriceStaleReconnectMs) {
            System.err.println("[CROSS " + feedId + " " + symbol + "] mid_price unchanged " + (noChange / 1000) + "s -> reconnect");
            reconnect();
        }
    }

    private void reconnect() {
        tryClose("reconnect");
        lastMidPriceChangeTs.set(System.currentTimeMillis());  // reset – avoid immediate re-trigger
        connect();
    }

    private void connect() {
        String stream = symbol.toLowerCase() + "@bookTicker";
        String url = System.getenv().getOrDefault("CROSS_BINANCE_WS_BASE", WS_BASE) + stream;
        Request req = new Request.Builder().url(url).build();
        lastMsgTs.set(System.currentTimeMillis());

        ws = client.newWebSocket(req, new WebSocketListener() {
            @Override
            public void onOpen(WebSocket webSocket, Response response) {
                lastMsgTs.set(System.currentTimeMillis());
                System.out.println("[CROSS " + feedId + "] connected to " + symbol + " @" + url);
            }

            @Override
            public void onMessage(WebSocket webSocket, String text) {
                long now = System.currentTimeMillis();
                lastMsgTs.set(now);

                try {
                    JsonNode root = MAPPER.readTree(text);
                    double bid = Double.parseDouble(root.path("b").asText("0"));
                    double ask = Double.parseDouble(root.path("a").asText("0"));

                    if (bid > 0 && ask > 0) {
                        double mid = (bid + ask) / 2.0;
                        if (Double.isFinite(lastMidPrice) && Math.abs(mid - lastMidPrice) > 1e-10) {
                            lastMidPriceChangeTs.set(now);
                        } else if (!Double.isFinite(lastMidPrice)) {
                            lastMidPriceChangeTs.set(now);
                        }
                        lastMidPrice = mid;

                        store.update(feedId, symbol, bid, ask, now);
                    }
                } catch (Exception ignore) {
                    // no spam
                }
            }

            @Override
            public void onClosing(WebSocket webSocket, int code, String reason) {
                webSocket.close(1000, null);
            }

            @Override
            public void onFailure(WebSocket webSocket, Throwable t, Response response) {
                String msg = (t == null ? "" : String.valueOf(t.getMessage())).toLowerCase();
                if (msg.contains("canceled") || msg.contains("cancelled") || msg.contains("socket closed")) {
                    return;
                }
                if (running.get()) {
                    System.err.println("[CROSS " + feedId + "] failure: " + (t != null ? t.getMessage() : "null") + " -> reconnect");
                    reconnect();
                }
            }
        });
    }

    private void tryClose(String why) {
        try {
            WebSocket s = ws;
            ws = null;
            if (s != null) {
                s.cancel();
                s.close(1000, why);
            }
        } catch (Exception ignore) {
        }
    }
}
