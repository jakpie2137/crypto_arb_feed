package feed_filter;

import okhttp3.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Kraken Spot WS v2 feed for EUR/USD (dense FX proxy).
 *
 * Important: Kraken will rate-limit / 429 if you reconnect too aggressively.
 * This feed is intentionally conservative:
 *  - It does NOT reconnect on every "stale" check.
 *  - It REST-verifies (rate-limited) and reconnects ONLY when REST shows movement while WS is silent.
 *  - Reconnect attempts use exponential backoff + cooldown on 429.
 *
 * Failover policy to ECB/Frankfurter is handled by FxRates:
 *  - record Kraken failures
 *  - after 3 fails in 5 minutes -> force reference for a while
 */
public class KrakenEurUsdFeed implements MarketDataFeed {

    private static final ObjectMapper M = new ObjectMapper();

    private final OkHttpClient http;
    private final OkHttpClient wsClient;
    private volatile WebSocket ws;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ScheduledExecutorService ses =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "fx-kraken-eurusd");
                t.setDaemon(true);
                return t;
            });

    private final AtomicLong lastAnyMsgTs = new AtomicLong(0);
    private final AtomicLong lastBookUpdateTs = new AtomicLong(0);
    private volatile double lastBestBid = Double.NaN;
    private volatile double lastBestAsk = Double.NaN;

    private final long globalSilenceMs =
            Long.parseLong(System.getenv().getOrDefault("FX_KRAKEN_EURUSD_GLOBAL_SILENCE_MS", "90000")); // 90s
    private final long perStaleMs =
            Long.parseLong(System.getenv().getOrDefault("FX_KRAKEN_EURUSD_STALE_MS", "90000")); // 90s (avoid reconnect storms)
    private final long restVerifyMinMs =
            Long.parseLong(System.getenv().getOrDefault("FX_KRAKEN_EURUSD_REST_VERIFY_MIN_MS", "30000")); // 30s
    private final AtomicLong lastRestVerifyTs = new AtomicLong(0);

    private final int depth =
            Integer.parseInt(System.getenv().getOrDefault("FX_KRAKEN_EURUSD_DEPTH", "10"));

    // reconnect backoff
    private final AtomicLong nextAllowedReconnectTs = new AtomicLong(0);
    private final AtomicLong backoffMs = new AtomicLong(1000); // grows on failures
    private final long maxBackoffMs =
            Long.parseLong(System.getenv().getOrDefault("FX_KRAKEN_EURUSD_MAX_BACKOFF_MS", "60000"));

    public KrakenEurUsdFeed() {
        this.http = new OkHttpClient.Builder()
                .callTimeout(Duration.ofSeconds(10))
                .connectTimeout(Duration.ofSeconds(10))
                .readTimeout(Duration.ofSeconds(10))
                .build();

        this.wsClient = new OkHttpClient.Builder()
                .pingInterval(Duration.ofSeconds(20))
                .retryOnConnectionFailure(true)
                .build();
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

    private void connect() {
        if (!running.get()) return;

        String url = System.getenv().getOrDefault("KRAKEN_WS_URL", "wss://ws.kraken.com/v2");
        Request req = new Request.Builder().url(url).build();

        lastAnyMsgTs.set(System.currentTimeMillis());
        ws = wsClient.newWebSocket(req, new WebSocketListener() {

            @Override
            public void onOpen(WebSocket webSocket, Response response) {
                lastAnyMsgTs.set(System.currentTimeMillis());
                backoffMs.set(1000); // reset on success
                System.out.println("[FX KRAKEN EURUSD] connected");

                String sub = "{"
                        + "\"method\":\"subscribe\","
                        + "\"params\":{"
                        + "\"channel\":\"book\","
                        + "\"symbol\":[\"EUR/USD\"],"
                        + "\"depth\":" + Math.max(10, depth) + ","
                        + "\"snapshot\":true"
                        + "}"
                        + "}";
                webSocket.send(sub);
            }

            @Override
            public void onMessage(WebSocket webSocket, String text) {
                long now = System.currentTimeMillis();
                lastAnyMsgTs.set(now);

                try {
                    JsonNode root = M.readTree(text);
                    if (!"book".equals(root.path("channel").asText())) return;

                    JsonNode data = root.get("data");
                    if (data == null || !data.isArray() || data.size() == 0) return;

                    for (JsonNode d : data) {
                        if (!"EUR/USD".equals(d.path("symbol").asText())) continue;

                        JsonNode bids = d.get("bids");
                        JsonNode asks = d.get("asks");
                        if (bids == null || !bids.isArray() || bids.size() == 0) continue;
                        if (asks == null || !asks.isArray() || asks.size() == 0) continue;

                        double bid = Double.parseDouble(bids.get(0).get(0).asText("0"));
                        double ask = Double.parseDouble(asks.get(0).get(0).asText("0"));
                        if (!(bid > 0) || !(ask > 0)) continue;

                        lastBestBid = bid;
                        lastBestAsk = ask;
                        lastBookUpdateTs.set(now);

                        FxRates.updateEurUsdFromKraken(bid, ask, now);
                    }

                } catch (Exception ignored) {
                    // ignore; watchdog will handle staleness
                }
            }

            @Override
            public void onFailure(WebSocket webSocket, Throwable t, Response response) {
                String msg = (t == null ? "" : String.valueOf(t.getMessage())).toLowerCase();
                if (msg.contains("canceled") || msg.contains("cancelled") || msg.contains("socket closed")) return;

                int code = response != null ? response.code() : -1;
                if (code == 429 || msg.contains("429")) {
                    // hard cooldown: Kraken is telling us to stop reconnecting
                    long now = System.currentTimeMillis();
                    long cooldown = Math.min(maxBackoffMs, Math.max(30000, backoffMs.get()));
                    nextAllowedReconnectTs.set(now + cooldown);
                    backoffMs.set(Math.min(maxBackoffMs, cooldown * 2));
                    FxRates.reportEurUsdKrakenFailure("failure@"+now);
                    System.err.println("[FX KRAKEN EURUSD] 429 Too Many Requests -> cooldown " + cooldown + "ms (no reconnect storm)");
                    return;
                }

                if (running.get()) {
                    long now = System.currentTimeMillis();
                    FxRates.reportEurUsdKrakenFailure("failure@"+now);
                    scheduleReconnect("failure: " + (t == null ? "null" : t.getMessage()));
                }
            }

            @Override
            public void onClosed(WebSocket webSocket, int code, String reason) {
                if (running.get()) {
                    long now = System.currentTimeMillis();
                    FxRates.reportEurUsdKrakenFailure("failure@"+now);
                    scheduleReconnect("closed code=" + code + " reason=" + reason);
                }
            }
        });
    }

    private void scheduleReconnect(String why) {
        long now = System.currentTimeMillis();
        long notBefore = nextAllowedReconnectTs.get();
        if (now < notBefore) {
            // still cooling down
            return;
        }

        long delay = Math.min(maxBackoffMs, backoffMs.get());
        backoffMs.set(Math.min(maxBackoffMs, delay * 2));
        nextAllowedReconnectTs.set(now + delay);

        System.err.println("[FX KRAKEN EURUSD] " + why + " -> reconnect in " + delay + "ms");
        tryClose("reconnect");
        ses.schedule(() -> {
            if (running.get()) connect();
        }, delay, TimeUnit.MILLISECONDS);
    }

    private void tryClose(String why) {
        try {
            WebSocket s = ws;
            ws = null;
            if (s != null) s.close(1000, why);
        } catch (Exception ignored) {}
    }

    private void watchdog() {
        if (!running.get()) return;

        long now = System.currentTimeMillis();

        long anySilence = now - lastAnyMsgTs.get();
        if (anySilence > globalSilenceMs) {
            FxRates.reportEurUsdKrakenFailure("failure@"+now);
            scheduleReconnect("GLOBAL silence " + anySilence + "ms");
            return;
        }

        long last = lastBookUpdateTs.get();
        if (last == 0) return;

        long silence = now - last;
        if (silence <= perStaleMs) return;

        // Rate-limit REST verify
        long lastRest = lastRestVerifyTs.get();
        if (now - lastRest < restVerifyMinMs) return;
        lastRestVerifyTs.set(now);

        Best best;
        try {
            best = fetchBestFromRest();
        } catch (Exception e) {
            // REST failing doesn't mean WS is dead; avoid storms
            return;
        }
        if (best == null) return;

        boolean moved = Double.isFinite(lastBestBid) && Double.isFinite(lastBestAsk)
                && ((best.bid != lastBestBid) || (best.ask != lastBestAsk));

        if (moved) {
            System.err.println("[FX KRAKEN EURUSD] stale " + silence + "ms, REST moved (bid=" + best.bid + " ask=" + best.ask + ") -> reconnect (backoff guarded)");
            FxRates.reportEurUsdKrakenFailure("failure@"+now);
            scheduleReconnect("REST moved while WS silent");
        }
    }

    private static final class Best {
        final double bid;
        final double ask;
        Best(double bid, double ask) { this.bid = bid; this.ask = ask; }
    }

    private Best fetchBestFromRest() throws IOException {
        // Kraken REST Depth EURUSD count=1
        String pair = "EURUSD";
        String url = "https://api.kraken.com/0/public/Depth?pair=" +
                URLEncoder.encode(pair, StandardCharsets.UTF_8) + "&count=1";
        Request req = new Request.Builder().url(url).get().build();
        try (Response resp = http.newCall(req).execute()) {
            if (!resp.isSuccessful() || resp.body() == null) throw new IOException("HTTP " + resp.code());
            JsonNode root = M.readTree(resp.body().string());
            JsonNode result = root.get("result");
            if (result == null || !result.fields().hasNext()) return null;
            JsonNode pairObj = result.fields().next().getValue();

            JsonNode bids = pairObj.get("bids");
            JsonNode asks = pairObj.get("asks");
            double bid = (bids != null && bids.isArray() && bids.size() > 0) ? Double.parseDouble(bids.get(0).get(0).asText("0")) : Double.NaN;
            double ask = (asks != null && asks.isArray() && asks.size() > 0) ? Double.parseDouble(asks.get(0).get(0).asText("0")) : Double.NaN;
            if (!(bid > 0) || !(ask > 0)) return null;
            return new Best(bid, ask);
        }
    }
}
