package feed_filter;

import okhttp3.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Minimal Binance WS feed to keep USD/USDT fresh.
 *
 * Uses Spot bookTicker for USDTUSD (USDT priced in USD).
 * We invert to store USD/USDT in FxRates.
 *
 * Endpoint doc: Binance WebSocket Streams (spot) and bookTicker stream.
 */
public class BinanceUsdUsdtFeed implements MarketDataFeed {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final OkHttpClient client;
    private final ScheduledExecutorService ses;

    private volatile WebSocket ws;
    private final AtomicBoolean running = new AtomicBoolean(false);

    private final AtomicLong lastMsgTs = new AtomicLong(0);

    private final long globalSilenceMs =
            Long.parseLong(System.getenv().getOrDefault("FX_BINANCE_WATCHDOG_MS", "45000"));

    public BinanceUsdUsdtFeed() {
        this.client = new OkHttpClient.Builder()
                .pingInterval(java.time.Duration.ofSeconds(20))
                .build();
        this.ses = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "fx-binance-usdtusd");
            t.setDaemon(true);
            return t;
        });
    }

    @Override
    public void start() {
        if (!running.compareAndSet(false, true)) return;
        connect();
        ses.scheduleAtFixedRate(this::watchdog, 5, 5, TimeUnit.SECONDS);
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
        long last = lastMsgTs.get();
        if (last > 0 && (now - last) > globalSilenceMs) {
            System.err.println("[FX BINANCE] global silence " + (now - last) + "ms -> reconnect");
            reconnect();
        }
    }

    private void reconnect() {
        tryClose("reconnect");
        connect();
    }

    private void connect() {
        // Spot base endpoint per docs:
        String url = System.getenv().getOrDefault("FX_BINANCE_WS_URL", "wss://stream.binance.com:9443/ws/usdtusd@bookTicker");
        Request req = new Request.Builder().url(url).build();
        lastMsgTs.set(System.currentTimeMillis());
        ws = client.newWebSocket(req, new WebSocketListener() {

            @Override
            public void onOpen(WebSocket webSocket, Response response) {
                lastMsgTs.set(System.currentTimeMillis());
                System.out.println("[FX BINANCE] connected");
            }

            @Override
            public void onMessage(WebSocket webSocket, String text) {
                lastMsgTs.set(System.currentTimeMillis());
                try {
                    JsonNode root = MAPPER.readTree(text);
                    // bookTicker fields: b (bid price), a (ask price)
                    double bid = Double.parseDouble(root.path("b").asText("0"));
                    double ask = Double.parseDouble(root.path("a").asText("0"));
                    if (bid > 0 && ask > 0) {
                        FxRates.updateUsdUsdtFromBinance(bid, ask, System.currentTimeMillis());
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
                // Ignore our own closes/cancels
                String msg = (t == null ? "" : String.valueOf(t.getMessage())).toLowerCase();
                if (msg.contains("canceled") || msg.contains("cancelled") || msg.contains("socket closed")) {
                    return;
                }
                if (running.get()) {
                    System.err.println("[FX BINANCE] failure: " + (t == null ? "null" : t.getMessage()) + " -> reconnect");
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
        } catch (Exception ignore) {}
    }
}
