package feed_filter;

public interface MarketDataFeed {
    /**
     * Zwraca aktualną cenę (np. MidPrice) dla danego symbolu.
     * Używane przez Router do weryfikacji poślizgu cenowego.
     * * Default: 0.0 - dzięki temu nie musimy implementować tej metody
     * w klasach technicznych feedów (BinanceFeed, KucoinFeed itp.),
     * które służą tylko do pobierania danych, a nie ich odczytu.
     */
    default double getPrice(String exchange, String symbol) {
        return 0.0;
    }

    void start();
    void stop();
}