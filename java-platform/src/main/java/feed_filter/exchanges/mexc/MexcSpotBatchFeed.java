package feed_filter.exchanges.mexc;

import com.mxc.push.common.protobuf.PublicLimitDepthV3ApiItem;
import com.mxc.push.common.protobuf.PublicLimitDepthsV3Api;
import com.mxc.push.common.protobuf.PushDataV3ApiWrapper;
import feed_filter.Exchange;
import feed_filter.FeedLogger;
import feed_filter.InMemoryOrderBookStore;
import feed_filter.MarketDataFeed;
import feed_filter.OrderBookDepthConfig;
import feed_filter.OrderBookLevel;
import feed_filter.OrderBookSnapshot;
import feed_filter.OrderBookStore;
import okhttp3.*;
import okio.ByteString;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class MexcSpotBatchFeed implements MarketDataFeed {

    public interface SymbolMapper {
        String toInternal(String externalSymbol);
    }

    private static final String WS_URL = "wss://wbs-api.mexc.com/ws";
    private static final int MAX_SUBS_PER_WS = 30;

    // Ping interval for Application-Level Pings
    private static final long PING_INTERVAL_MS = 10_000L;

    private static final long RECONNECT_BASE_DELAY_MS = 1_000L;
    private static final long RECONNECT_MAX_DELAY_MS  = 30_000L;

    // [FIX] Disabled OkHttp protocol pings (set to 0) to avoid conflict with manual JSON pings
    private static final OkHttpClient CLIENT = new OkHttpClient.Builder()
            .readTimeout(0, TimeUnit.MILLISECONDS)
            .pingInterval(0, TimeUnit.SECONDS)
            .retryOnConnectionFailure(true)
            .build();

    private static final ScheduledExecutorService SCHED = Executors.newScheduledThreadPool(
            Math.max(2, Runtime.getRuntime().availableProcessors() / 2),
            r -> {
                Thread t = new Thread(r, "mexc-batch-scheduler");
                t.setDaemon(true);
                return t;
            }
    );

    private final OrderBookStore store;
    private final int levels;
    private final SymbolMapper symbolMapper;
    private final List<String> symbolsUpper;

    private volatile boolean running = false;
    private final List<Conn> conns = new ArrayList<>();

    public MexcSpotBatchFeed(List<String> symbols, OrderBookStore store) {
        this(symbols, store, OrderBookDepthConfig.MEXC_SPOT_DEPTH);
    }

    public MexcSpotBatchFeed(List<String> symbols, OrderBookStore store, int levels) {
        this(symbols, store, levels, s -> s);
    }

    public MexcSpotBatchFeed(List<String> symbols, OrderBookStore store, int levels, SymbolMapper symbolMapper) {
        if (store == null) throw new IllegalArgumentException("store cannot be null");
        if (symbols == null) throw new IllegalArgumentException("symbols cannot be null");
        this.store = store;
        this.levels = levels;
        this.symbolMapper = (symbolMapper != null) ? symbolMapper : (s -> s);

        List<String> tmp = new ArrayList<>(symbols.size());
        for (String s : symbols) {
            if (s == null) continue;
            String up = s.trim().toUpperCase(Locale.ROOT);
            if (!up.isEmpty()) tmp.add(up);
        }
        this.symbolsUpper = Collections.unmodifiableList(tmp);
    }

    @Override
    public synchronized void start() {
        if (running) return;
        running = true;

        List<List<String>> chunks = chunk(symbolsUpper, MAX_SUBS_PER_WS);
        int idx = 0;
        for (List<String> ch : chunks) {
            Conn c = new Conn(idx++, ch);
            conns.add(c);
            c.connectNow("startup");
        }

        FeedLogger.info("MEXC", "Batch feed started: symbols=" + symbolsUpper.size() +
                " conns=" + conns.size() + " levels=" + levels);
        
        // Log symbol list
        StringBuilder sb = new StringBuilder("[MEXC] Symbols: ");
        for (int i = 0; i < symbolsUpper.size(); i++) {
            if (i > 0) sb.append(", ");
            String internal = symbolMapper.toInternal(symbolsUpper.get(i));
            sb.append(internal != null ? internal : symbolsUpper.get(i));
        }
        System.out.println(sb.toString());
    }

    @Override
    public synchronized void stop() {
        running = false;
        for (Conn c : conns) {
            c.stop();
        }
        conns.clear();
        FeedLogger.info("MEXC", "Batch feed stopped");
    }

    private static List<List<String>> chunk(List<String> list, int size) {
        List<List<String>> out = new ArrayList<>();
        for (int i = 0; i < list.size(); i += size) {
            out.add(list.subList(i, Math.min(list.size(), i + size)));
        }
        return out;
    }

    private final class Conn {
        private final int connId;
        private final List<String> symbols;
        private volatile WebSocket ws;

        private final AtomicBoolean reconnectScheduled = new AtomicBoolean(false);
        private final AtomicInteger backoffExp = new AtomicInteger(0);
        private volatile ScheduledFuture<?> pingTask;

        // Track if this is the first successful connection to change log message
        private boolean firstConnect = true;

        Conn(int connId, List<String> symbols) {
            this.connId = connId;
            this.symbols = new ArrayList<>(symbols);
        }

        void connectNow(String reason) {
            if (!running) return;

            Request request = new Request.Builder()
                    .url(WS_URL)
                    .header("User-Agent", "Mozilla/5.0")
                    .build();

            WebSocket old = ws;
            if (old != null) {
                try { old.cancel(); } catch (Exception ignored) {}
                ws = null;
            }

            ws = CLIENT.newWebSocket(request, new WebSocketListener() {
                @Override
                public void onOpen(WebSocket webSocket, Response response) {
                    backoffExp.set(0);
                    reconnectScheduled.set(false);

                    List<String> topics = new ArrayList<>(symbols.size());
                    for (String s : symbols) {
                        topics.add("spot@public.limit.depth.v3.api.pb@" + s + "@" + levels);
                    }

                    String subJson = buildSubscriptionJson(topics);
                    boolean ok = webSocket.send(subJson);
                    if (!ok) {
                        FeedLogger.error("MEXC", "WS send SUB failed conn#" + connId);
                    }

                    startPing(webSocket);

                    // [UPDATED LOGGING]
                    if (firstConnect) {
                        FeedLogger.info("MEXC", "WS connected conn#" + connId + " subs=" + symbols.size());
                        firstConnect = false;
                    } else {
                        // Suppressed message as requested by user
                        FeedLogger.info("MEXC", "MEXC connection checked for shard " + connId + " - OK");
                    }
                }

                @Override
                public void onMessage(WebSocket webSocket, String text) {
                }

                @Override
                public void onMessage(WebSocket webSocket, ByteString bytes) {
                    try {
                        ByteBuffer buf = bytes.asByteBuffer();
                        PushDataV3ApiWrapper wrapper = PushDataV3ApiWrapper.parseFrom(buf);
                        handleWrapper(wrapper);
                    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                        // Enhanced error handling for protobuf parsing
                        byte[] byteArray = bytes.toByteArray();
                        String firstByteStr = byteArray.length > 0 ? String.valueOf(byteArray[0] & 0xFF) : "empty";
                        FeedLogger.error("MEXC", "Protobuf parse error conn#" + connId + ": " + e.getMessage() 
                                + " (bytes=" + byteArray.length + ", firstByte=" + firstByteStr + ")");
                        // Log first few bytes for debugging
                        if (byteArray.length > 0) {
                            StringBuilder hex = new StringBuilder();
                            int len = Math.min(20, byteArray.length);
                            for (int i = 0; i < len; i++) {
                                hex.append(String.format("%02x ", byteArray[i] & 0xFF));
                            }
                            FeedLogger.error("MEXC", "First " + len + " bytes (hex): " + hex.toString());
                        }
                    } catch (Exception e) {
                        // Enhanced error handling for other exceptions
                        FeedLogger.error("MEXC", "Unexpected error parsing protobuf conn#" + connId + ": " + e.getClass().getSimpleName() 
                                + ": " + e.getMessage());
                        if (e.getCause() != null) {
                            FeedLogger.error("MEXC", "Caused by: " + e.getCause().getClass().getSimpleName() + ": " + e.getCause().getMessage());
                        }
                    }
                }

                @Override
                public void onClosing(WebSocket webSocket, int code, String reason) {
                    try { webSocket.close(code, reason); } catch (Exception ignored) {}
                }

                @Override
                public void onClosed(WebSocket webSocket, int code, String reason) {
                    stopPing();
                    if (running) scheduleReconnect("onClosed-" + code);
                }

                @Override
                public void onFailure(WebSocket webSocket, Throwable t, Response response) {
                    stopPing();
                    if (running) scheduleReconnect("onFailure");
                }
            });
        }

        private void handleWrapper(PushDataV3ApiWrapper wrapper) {
            if (wrapper == null) return;
            if (!wrapper.hasPublicLimitDepths()) return;

            PublicLimitDepthsV3Api depth = wrapper.getPublicLimitDepths();
            String sym = wrapper.hasSymbol() ? wrapper.getSymbol() : null;
            if (sym == null) return;

            long ts = wrapper.hasSendTime() ? wrapper.getSendTime() : System.currentTimeMillis();

            List<OrderBookLevel> bids = new ArrayList<>();
            for (PublicLimitDepthV3ApiItem b : depth.getBidsList()) {
                double p = safeDouble(b.getPrice());
                double q = safeDouble(b.getQuantity());
                if (!Double.isNaN(p) && q > 0d) bids.add(new OrderBookLevel(p, q));
            }

            List<OrderBookLevel> asks = new ArrayList<>();
            for (PublicLimitDepthV3ApiItem a : depth.getAsksList()) {
                double p = safeDouble(a.getPrice());
                double q = safeDouble(a.getQuantity());
                if (!Double.isNaN(p) && q > 0d) asks.add(new OrderBookLevel(p, q));
            }

            OrderBookSnapshot snap = new OrderBookSnapshot(
                    Exchange.MEXC,
                    symbolMapper.toInternal(sym),
                    ts,
                    bids,
                    asks
            );

            if (store instanceof InMemoryOrderBookStore) {
                ((InMemoryOrderBookStore) store).putSnapshot(snap);
            } else {
                store.update(snap);
            }
        }

        private double safeDouble(String s) {
            if (s == null) return Double.NaN;
            try { return Double.parseDouble(s); }
            catch (Exception e) { return Double.NaN; }
        }

        private void startPing(WebSocket webSocket) {
            stopPing();
            pingTask = SCHED.scheduleAtFixedRate(() -> {
                try {
                    webSocket.send("{\"method\":\"PING\"}");
                } catch (Exception ignored) {}
            }, PING_INTERVAL_MS, PING_INTERVAL_MS, TimeUnit.MILLISECONDS);
        }

        private void stopPing() {
            ScheduledFuture<?> t = pingTask;
            if (t != null) {
                try { t.cancel(false); } catch (Exception ignored) {}
            }
            pingTask = null;
        }

        private void scheduleReconnect(String reason) {
            if (!running) return;
            if (!reconnectScheduled.compareAndSet(false, true)) return;

            int exp = Math.min(10, backoffExp.getAndIncrement());
            long delay = Math.min(RECONNECT_MAX_DELAY_MS, RECONNECT_BASE_DELAY_MS * (1L << exp));

            SCHED.schedule(() -> {
                if (!running) {
                    reconnectScheduled.set(false);
                    return;
                }
                connectNow("reconnect-" + reason);
            }, delay, TimeUnit.MILLISECONDS);
        }

        void stop() {
            stopPing();
            WebSocket w = ws;
            ws = null;
            if (w != null) {
                try { w.close(1000, "shutdown"); } catch (Exception ignored) {}
                try { w.cancel(); } catch (Exception ignored) {}
            }
            reconnectScheduled.set(false);
        }
    }

    private static String buildSubscriptionJson(List<String> topics) {
        StringBuilder sb = new StringBuilder(64 + topics.size() * 64);
        sb.append("{\"method\":\"SUBSCRIPTION\",\"params\":[");
        for (int i = 0; i < topics.size(); i++) {
            if (i > 0) sb.append(',');
            sb.append('"').append(topics.get(i)).append('"');
        }
        sb.append("]}");
        return sb.toString();
    }
}