package feed_filter.exchanges.gate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import feed_filter.*;
import okhttp3.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * Gate.io Spot orderbook feed.
 * Subscribes to spot.order_book channel for multiple pairs.
 */
public class GateSpotFeed implements MarketDataFeed {

    private static final String WS_URL = "wss://api.gateio.ws/ws/v4/";
    private static final long PING_INTERVAL_MS = 20_000L;  // Send ping every 20s to keep connection alive
    private static final long RECONNECT_BASE_DELAY_MS = 1_000L;
    private static final long RECONNECT_MAX_DELAY_MS = 15_000L;

    public record Sub(String internalSymbol, String externalSymbol) {}

    private final List<Sub> subs;
    private final OrderBookStore store;
    private final Function<String, String> externalToInternal;
    private final int depth;
    private final long globalSilenceTimeoutMs;

    private final OkHttpClient client;
    private final ObjectMapper mapper = new ObjectMapper();
    private final ScheduledExecutorService exec;

    private volatile WebSocket webSocket;
    private volatile boolean running = false;
    private volatile ScheduledFuture<?> pingTask;
    private volatile ScheduledFuture<?> watchdogTask;

    private final AtomicLong lastAnyMessageMs = new AtomicLong(0L);
    private final AtomicBoolean reconnectInFlight = new AtomicBoolean(false);
    private volatile int reconnectStreak = 0;

    public GateSpotFeed(List<Sub> subs, OrderBookStore store,
                        Function<String, String> externalToInternal, int depth,
                        long globalSilenceTimeoutMs) {
        this.subs = subs;
        this.store = store;
        this.externalToInternal = externalToInternal;
        this.depth = depth;
        this.globalSilenceTimeoutMs = globalSilenceTimeoutMs > 0 ? globalSilenceTimeoutMs : 60_000L;

        this.client = new OkHttpClient.Builder()
                .readTimeout(0, TimeUnit.MILLISECONDS)
                .pingInterval(0, TimeUnit.MILLISECONDS)
                .build();

        this.exec = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "gate-spot-feed");
            t.setDaemon(true);
            return t;
        });
    }

    @Override
    public void start() {
        running = true;
        lastAnyMessageMs.set(System.currentTimeMillis());

        FeedLogger.info("GATE", "Starting spot feed for " + subs.size() + " pairs");
        startWatchdog();
        connect();
    }

    @Override
    public void stop() {
        running = false;
        stopPingLoop();
        stopWatchdog();

        WebSocket ws = this.webSocket;
        this.webSocket = null;
        if (ws != null) {
            try { ws.close(1000, "shutdown"); } catch (Exception ignored) {}
            try { ws.cancel(); } catch (Exception ignored) {}
        }

        exec.shutdownNow();
    }

    private void connect() {
        if (!running) return;

        WebSocket prev = this.webSocket;
        this.webSocket = null;
        if (prev != null) {
            try { prev.close(1000, "reconnect"); } catch (Exception ignored) {}
            try { prev.cancel(); } catch (Exception ignored) {}
        }

        Request request = new Request.Builder().url(WS_URL).build();
        this.webSocket = client.newWebSocket(request, new WebSocketListener() {

            @Override
            public void onOpen(WebSocket ws, Response response) {
                FeedLogger.info("GATE", "Spot WS connected");
                reconnectStreak = 0;
                reconnectInFlight.set(false);
                lastAnyMessageMs.set(System.currentTimeMillis());

                // Subscribe to all pairs
                // Gate spot uses: {"time": ..., "channel": "spot.order_book", "event": "subscribe", "payload": ["BTC_USDT", "20", "100ms"]}
                for (Sub s : subs) {
                    long nowSec = System.currentTimeMillis() / 1000L;
                    String sub = String.format(
                            "{\"time\":%d,\"channel\":\"spot.order_book\",\"event\":\"subscribe\",\"payload\":[\"%s\",\"%d\",\"100ms\"]}",
                            nowSec, s.externalSymbol, depth
                    );
                    ws.send(sub);
                }
                FeedLogger.info("GATE", "Subscribed to " + subs.size() + " spot pairs (depth=" + depth + ")");

                startPingLoop(ws);
            }

            @Override
            public void onMessage(WebSocket ws, String text) {
                if (text == null) return;
                lastAnyMessageMs.set(System.currentTimeMillis());

                // Parse orderbook
                if (text.contains("\"channel\":\"spot.order_book\"")) {
                    try {
                        OrderBookSnapshot snap = GateOrderBookParser.parse(text);
                        if (snap != null && store instanceof InMemoryOrderBookStore) {
                            ((InMemoryOrderBookStore) store).putSnapshot(snap);
                        }
                    } catch (Exception e) {
                        // Ignore parse errors
                    }
                }
            }

            @Override
            public void onFailure(WebSocket ws, Throwable t, Response r) {
                if (!running) return;
                String msg = t.getClass().getSimpleName() + (t.getMessage() != null ? ": " + t.getMessage() : "");
                FeedLogger.error("GATE", "Spot WS failure: " + msg);
                cleanupAndReconnect(ws, "failure");
            }

            @Override
            public void onClosed(WebSocket ws, int code, String reason) {
                if (!running) return;
                FeedLogger.info("GATE", "Spot WS closed: " + code + " " + reason);
                cleanupAndReconnect(ws, "closed");
            }
        });
    }

    private void cleanupAndReconnect(WebSocket ws, String reason) {
        stopPingLoop();
        if (this.webSocket == ws) {
            this.webSocket = null;
        }
        try { ws.cancel(); } catch (Exception ignored) {}
        scheduleReconnect(reason);
    }

    private void startPingLoop(WebSocket ws) {
        stopPingLoop();
        pingTask = exec.scheduleAtFixedRate(() -> {
            if (!running || ws != webSocket) return;
            try {
                long nowSec = System.currentTimeMillis() / 1000L;
                // Gate spot uses "spot.ping" channel
                String ping = String.format("{\"time\":%d,\"channel\":\"spot.ping\"}", nowSec);
                ws.send(ping);
            } catch (Exception ignored) {}
        }, PING_INTERVAL_MS, PING_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    private void stopPingLoop() {
        ScheduledFuture<?> p = pingTask;
        pingTask = null;
        if (p != null) {
            try { p.cancel(false); } catch (Exception ignored) {}
        }
    }

    private void startWatchdog() {
        stopWatchdog();
        watchdogTask = exec.scheduleAtFixedRate(() -> {
            if (!running) return;

            // Global silence check - if no data for configured timeout, reconnect
            long silenceDiff = System.currentTimeMillis() - lastAnyMessageMs.get();
            if (silenceDiff > globalSilenceTimeoutMs) {
                if (reconnectInFlight.compareAndSet(false, true)) {
                    FeedLogger.warn("GATE", "Spot global silence (" + silenceDiff + "ms) -> reconnecting");
                    forceReconnect();
                }
            }
        }, 5000L, 5000L, TimeUnit.MILLISECONDS);  // check every 5s
    }

    private void stopWatchdog() {
        ScheduledFuture<?> w = watchdogTask;
        watchdogTask = null;
        if (w != null) {
            try { w.cancel(false); } catch (Exception ignored) {}
        }
    }

    private void forceReconnect() {
        exec.execute(() -> {
            if (!running) {
                reconnectInFlight.set(false);
                return;
            }
            stopPingLoop();
            WebSocket ws = this.webSocket;
            this.webSocket = null;
            if (ws != null) {
                try { ws.close(1000, "reconnect"); } catch (Exception ignored) {}
                try { ws.cancel(); } catch (Exception ignored) {}
            }
            connect();
        });
    }

    private void scheduleReconnect(String reason) {
        if (!running) return;
        if (!reconnectInFlight.compareAndSet(false, true)) return;

        reconnectStreak++;
        long delay = Math.min(
                RECONNECT_MAX_DELAY_MS,
                RECONNECT_BASE_DELAY_MS * (1L << Math.min(10, reconnectStreak - 1))
        );

        FeedLogger.info("GATE", "Spot reconnect scheduled in " + delay + "ms (" + reason + ")");

        exec.schedule(() -> {
            if (!running) {
                reconnectInFlight.set(false);
                return;
            }
            WebSocket ws = this.webSocket;
            this.webSocket = null;
            if (ws != null) {
                try { ws.close(1000, "reconnect"); } catch (Exception ignored) {}
                try { ws.cancel(); } catch (Exception ignored) {}
            }
            connect();
        }, delay, TimeUnit.MILLISECONDS);
    }
}
