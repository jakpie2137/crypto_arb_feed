package feed_filter;

import feed.FeedServiceGrpc;
import feed.Level;
import feed.OrderbookUpdate;
import io.grpc.Server;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * gRPC server that streams all orderbook updates to connected clients (e.g. Feed Proxy).
 * StreamObserver is not thread-safe - each subscriber uses a single-thread executor to serialize writes.
 */
public class FeedGrpcServer {

    private final OrderBookUpdateBroadcaster broadcaster;
    private final FundingRateStore fundingStore;
    private final int port;
    private Server server;
    private final AtomicInteger subscriberCount = new AtomicInteger(0);

    public FeedGrpcServer(OrderBookUpdateBroadcaster broadcaster, FundingRateStore fundingStore, int port) {
        this.broadcaster = broadcaster;
        this.fundingStore = fundingStore;
        this.port = port;
    }

    public void start() throws IOException {
        server = io.grpc.ServerBuilder.forPort(port)
                .addService(new FeedServiceImpl())
                .build();
        server.start();
        System.out.println("[FEED-GRPC] Server started on port " + port);
    }

    public void shutdown() {
        if (server != null && !server.isShutdown()) {
            server.shutdown();
            System.out.println("[FEED-GRPC] Server shutdown");
        }
    }

    public int getSubscriberCount() {
        return subscriberCount.get();
    }

    private class FeedServiceImpl extends FeedServiceGrpc.FeedServiceImplBase {

        @Override
        public void streamAllOrderbooks(com.google.protobuf.Empty request,
                                       StreamObserver<OrderbookUpdate> responseObserver) {
            subscriberCount.incrementAndGet();
            System.out.println("[FEED-GRPC] Proxy connected (subscribers: " + subscriberCount.get() + ")");

            ExecutorService executor = Executors.newSingleThreadExecutor(r -> {
                Thread t = new Thread(r, "feed-grpc-sub-" + subscriberCount.get());
                t.setDaemon(true);
                return t;
            });

            OrderBookUpdateBroadcaster.OrderBookUpdateListener listener = snapshot -> {
                executor.submit(() -> {
                    try {
                        if (responseObserver instanceof ServerCallStreamObserver<?> serverObs) {
                            if (serverObs.isCancelled() || !serverObs.isReady()) return;
                        }
                        OrderbookUpdate msg = toProto(snapshot, fundingStore);
                        responseObserver.onNext(msg);
                    } catch (Exception e) {
                        String msg = e.getMessage() != null ? e.getMessage().toLowerCase() : "";
                        Throwable cause = e.getCause();
                        String causeMsg = (cause != null && cause.getMessage() != null) ? cause.getMessage().toLowerCase() : "";
                        boolean streamClosed = msg.contains("closed") || msg.contains("cancel") || msg.contains("stream")
                                || causeMsg.contains("closed") || causeMsg.contains("stream");
                        if (!streamClosed) {
                            System.err.println("[FEED-GRPC] Send error: " + e.getMessage());
                        }
                    }
                });
            };

            broadcaster.addListener(listener);

            if (responseObserver instanceof ServerCallStreamObserver<?> serverObs) {
                serverObs.setOnCancelHandler(() -> {
                    broadcaster.removeListener(listener);
                    executor.shutdown();
                    subscriberCount.decrementAndGet();
                    System.out.println("[FEED-GRPC] Proxy disconnected (subscribers: " + subscriberCount.get() + ")");
                });
            }
        }
    }

    private static OrderbookUpdate toProto(OrderBookSnapshot snap, FundingRateStore fundingStore) {
        OrderbookUpdate.Builder builder = OrderbookUpdate.newBuilder()
                .setExchange(snap.getExchange().name())
                .setSymbol(snap.getSymbol())
                .setTimestampMs(snap.getEventTimeMillis());

        List<OrderBookLevel> bids = snap.getBids();
        if (bids != null) {
            for (OrderBookLevel lvl : bids) {
                builder.addBids(Level.newBuilder().setPrice(lvl.getPrice()).setSize(lvl.getSize()).build());
            }
        }
        List<OrderBookLevel> asks = snap.getAsks();
        if (asks != null) {
            for (OrderBookLevel lvl : asks) {
                builder.addAsks(Level.newBuilder().setPrice(lvl.getPrice()).setSize(lvl.getSize()).build());
            }
        }

        if (fundingStore != null) {
            FundingInfo info = fundingStore.get(snap.getExchange(), snap.getSymbol());
            if (info != null) {
                builder.setFundingRate(info.getFundingRate());
                if (info.getFundingIntervalHours() != null) {
                    builder.setFundingIntervalHours(info.getFundingIntervalHours());
                }
                if (info.getNextFundingTimeMillis() != null) {
                    builder.setNextFundingTimeMs(info.getNextFundingTimeMillis());
                }
                builder.setFundingSourceTsMs(info.getSourceTsMillis());
            }
        }
        return builder.build();
    }
}
