package feed_filter;

import db_pg.PostgresConfig;
import db_pg.MarketDataArbStatsWriter;
import db_pg.MarketDataArbRollingGainWriter;
import feed_filter.config.CrossFeedConfig;
import feed_filter.config.CrossFeedConfigLoader;
import feed_filter.config.FeedConfig;
import feed_filter.config.FeedConfigLoader;
import feed_filter.exchanges.binance.BinanceSpotFeed;
import feed_filter.exchanges.binancefut.BinanceFuturesFeed;
import feed_filter.exchanges.bitget.BitgetSpotFeed;
import feed_filter.exchanges.gate.GateSpotFeed;
import feed_filter.exchanges.gatefut.GateFuturesFeed;
import feed_filter.exchanges.kucoin.KucoinSpotFeed;
import feed_filter.exchanges.mexc.MexcSpotFeed;
import feed_filter.exchanges.bybitfut.BybitFuturesFeed;

/* [DISABLED TEMPORARILY]
import feed_filter.exchanges.kraken.KrakenSpotFeed;
import feed_filter.exchanges.bitkub.BitkubSpotFeed;
*/

import manager_client.ManagerSymbol;
import manager_client.SymbolRegistry;
import manager_client.SymbolRepository;
import feed_filter.OrderBookLevel;
import feed_filter.OrderBookSnapshot;

import javax.swing.SwingUtilities;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Główny punkt startowy aplikacji (bootstrap).
 * Inicjalizuje połączenia z giełdami, magazyn danych (Store) oraz GUI.
 * 
 * Konfiguracja feedów: conf/feeds/{exchange}.yaml
 */
public class FeedFilterApp {

    public static final int SYMBOL_LIMIT =
            Integer.parseInt(System.getenv().getOrDefault("OB_GUI_SYMBOL_LIMIT", "600"));

    public static final Duration STALE_THRESHOLD = Duration.ofSeconds(30);

    public static void main(String[] args) {
        System.out.println("=== FeedFilterApp Starting ===");
        System.out.println("[CONFIG] Loading feed configurations from: conf/feeds/");
        
        // Load all feed configs
        Map<String, FeedConfig> feedConfigs = FeedConfigLoader.loadAll();
        System.out.println("[CONFIG] Loaded " + feedConfigs.size() + " feed configurations");
        
        PostgresConfig pg = PostgresConfig.fromEnvOrDefaults();
        SymbolRepository repo = new SymbolRepository(pg);
        SymbolRegistry registry = new SymbolRegistry(repo);
        registry.refresh();

        // --- BROADCASTER (dla gRPC stream) ---
        OrderBookUpdateBroadcaster broadcaster = new OrderBookUpdateBroadcaster();

        // --- INTELLIGENT STORE WRAPPER ---
        // Wrapper dla InMemoryOrderBookStore, który normalizuje snapshoty (mnożniki)
        // i ujednolica czas (timestamp) dla wszystkich giełd.
        InMemoryOrderBookStore store = new InMemoryOrderBookStore() {
            @Override
            public void update(OrderBookSnapshot incomingSnapshot) {
                if (incomingSnapshot == null) return;
                OrderBookSnapshot effectiveSnapshot = normalizeSnapshot(
                        registry,
                        incomingSnapshot.getExchange(),
                        incomingSnapshot.getSymbol(),
                        incomingSnapshot
                );
                // Ustawienie czasu lokalnego w celu synchronizacji opóźnień
                effectiveSnapshot = new OrderBookSnapshot(
                        effectiveSnapshot.getExchange(),
                        effectiveSnapshot.getSymbol(),
                        System.currentTimeMillis(),
                        effectiveSnapshot.getBids(),
                        effectiveSnapshot.getAsks()
                );
                super.update(effectiveSnapshot);
                broadcaster.onUpdate(effectiveSnapshot);
            }

            @Override
            public void putSnapshot(OrderBookSnapshot incomingSnapshot) {
                if (incomingSnapshot == null) return;
                OrderBookSnapshot effectiveSnapshot = normalizeSnapshot(
                        registry,
                        incomingSnapshot.getExchange(),
                        incomingSnapshot.getSymbol(),
                        incomingSnapshot
                );
                effectiveSnapshot = new OrderBookSnapshot(
                        effectiveSnapshot.getExchange(),
                        effectiveSnapshot.getSymbol(),
                        System.currentTimeMillis(),
                        effectiveSnapshot.getBids(),
                        effectiveSnapshot.getAsks()
                );
                super.putSnapshot(effectiveSnapshot);
                broadcaster.onUpdate(effectiveSnapshot);
            }
        };
        FundingRateService fundingService = FundingRateService.getInstance();
        FundingRateStore fundingStore = fundingService.getStore();

        Set<String> symbolsForGui = new LinkedHashSet<>();
        Map<Exchange, Set<String>> subscribedByExchange = new HashMap<>();
        int totalFeeds = 0;
        java.util.concurrent.atomic.AtomicReference<BinanceFuturesFeed> binanceFutFeedRef =
                new java.util.concurrent.atomic.AtomicReference<>();
        java.util.concurrent.atomic.AtomicReference<BybitFuturesFeed> bybitFutFeedRef =
                new java.util.concurrent.atomic.AtomicReference<>();
        java.util.concurrent.atomic.AtomicReference<GateFuturesFeed> gateFutFeedRef =
                new java.util.concurrent.atomic.AtomicReference<>();

        // --- BINANCEFUT ---
        FeedConfig binanceFutConfig = feedConfigs.get("binancefut");
        if (binanceFutConfig != null && binanceFutConfig.isEnabled()) {
            Set<String> configSymbols = new HashSet<>(binanceFutConfig.getSymbols());
            // Load symbols from both BINANCEFUT and BINANCEFUT8SUBFIN (same orderbook feed)
            List<ManagerSymbol> binanceFut = new ArrayList<>();
            binanceFut.addAll(registry.getActiveSymbols(Exchange.BINANCEFUT));
            binanceFut.addAll(registry.getActiveSymbols(Exchange.BINANCEFUT8SUBFIN));
            // Remove duplicates (by symbol), filter by config, and limit
            binanceFut = binanceFut.stream()
                    .filter(s -> configSymbols.contains(s.symbol))
                    .collect(Collectors.toMap(
                            s -> s.symbol,
                            s -> s,
                            (s1, s2) -> s1  // Keep first if duplicate
                    ))
                    .values()
                    .stream()
                    .limit(SYMBOL_LIMIT)
                    .toList();
            
            System.out.println("[BINANCEFUT] Config symbols: " + configSymbols.size() + 
                    ", matched from DB (BINANCEFUT + BINANCEFUT8SUBFIN): " + binanceFut.size());
            logUnmatchedSymbols("BINANCEFUT", configSymbols, binanceFut);
            
            if (!binanceFut.isEmpty()) {
                List<BinanceFuturesFeed.Sub> subs = binanceFut.stream()
                        .map(s -> new BinanceFuturesFeed.Sub(s.symbol, s.externalSymbol))
                        .collect(Collectors.toList());
                long fundingPollIntervalMs = binanceFutConfig.getDefaults().getFundingPollIntervalMs();
                BinanceFuturesFeed feed = new BinanceFuturesFeed(
                        subs,
                        store,
                        external -> registry.mapExternalToInternal(Exchange.BINANCEFUT, external),
                        fundingStore,
                        fundingPollIntervalMs
                );
                binanceFutFeedRef.set(feed);
                fundingService.registerRefresher(Exchange.BINANCEFUT, feed::refreshFundingNow);
                feed.start();
                binanceFut.forEach(s -> symbolsForGui.add(s.symbol));
                subscribedByExchange.put(Exchange.BINANCEFUT, 
                        binanceFut.stream().map(s -> s.symbol).collect(Collectors.toSet()));
                totalFeeds += binanceFut.size();
            }
        } else {
            System.out.println("[BINANCEFUT] Disabled or no config");
        }

        // --- BYBITFUT ---
        FeedConfig bybitFutConfig = feedConfigs.get("bybitfut");
        if (bybitFutConfig != null && bybitFutConfig.isEnabled()) {
            Set<String> configSymbols = new HashSet<>(bybitFutConfig.getSymbols());
            List<ManagerSymbol> bybitFut = registry.getActiveSymbols(Exchange.BYBITFUT)
                    .stream()
                    .filter(s -> configSymbols.contains(s.symbol))
                    .limit(SYMBOL_LIMIT)
                    .toList();
            
            System.out.println("[BYBITFUT] Config symbols: " + configSymbols.size() + 
                    ", matched from DB: " + bybitFut.size());
            logUnmatchedSymbols("BYBITFUT", configSymbols, bybitFut);
            
            if (!bybitFut.isEmpty()) {
                List<BybitFuturesFeed.Sub> subs = bybitFut.stream()
                        .map(s -> new BybitFuturesFeed.Sub(s.symbol, s.externalSymbol))
                        .collect(Collectors.toList());
                long fundingPollIntervalMs = bybitFutConfig.getDefaults().getFundingPollIntervalMs();
                BybitFuturesFeed feed = new BybitFuturesFeed(
                        subs,
                        store,
                        external -> registry.mapExternalToInternal(Exchange.BYBITFUT, external),
                        fundingStore,
                        fundingPollIntervalMs
                );
                bybitFutFeedRef.set(feed);
                fundingService.registerRefresher(Exchange.BYBITFUT, feed::refreshFundingNow);
                feed.start();
                bybitFut.forEach(s -> symbolsForGui.add(s.symbol));
                subscribedByExchange.put(Exchange.BYBITFUT, 
                        bybitFut.stream().map(s -> s.symbol).collect(Collectors.toSet()));
                totalFeeds += bybitFut.size();
            }
        } else {
            System.out.println("[BYBITFUT] Disabled or no config");
        }

        // --- BITGET ---
        FeedConfig bitgetConfig = feedConfigs.get("bitget");
        if (bitgetConfig != null && bitgetConfig.isEnabled()) {
            Set<String> configSymbols = new HashSet<>(bitgetConfig.getSymbols());
            Duration staleThreshold = Duration.ofMillis(
                    bitgetConfig.getDefaults().getOrderbookStaleThresholdMs()
            );
            
            List<ManagerSymbol> bitget = registry.getActiveSymbols(Exchange.BITGET)
                    .stream()
                    .filter(s -> configSymbols.contains(s.symbol))
                    .limit(SYMBOL_LIMIT)
                    .toList();
            
            System.out.println("[BITGET] Config symbols: " + configSymbols.size() + 
                    ", matched from DB: " + bitget.size() +
                    ", staleThreshold: " + staleThreshold.toMillis() + "ms");
            logUnmatchedSymbols("BITGET", configSymbols, bitget);
            
            if (!bitget.isEmpty()) {
                List<BitgetSpotFeed.Sub> subs = bitget.stream()
                        .map(s -> new BitgetSpotFeed.Sub(s.symbol, s.externalSymbol))
                        .collect(Collectors.toList());
                new BitgetSpotFeed(
                        subs,
                        store,
                        external -> registry.mapExternalToInternal(Exchange.BITGET, external),
                        staleThreshold
                ).start();
                bitget.forEach(s -> symbolsForGui.add(s.symbol));
                subscribedByExchange.put(Exchange.BITGET, 
                        bitget.stream().map(s -> s.symbol).collect(Collectors.toSet()));
                totalFeeds += bitget.size();
            }
        } else {
            System.out.println("[BITGET] Disabled or no config");
        }

        // --- KUCOIN ---
        FeedConfig kucoinConfig = feedConfigs.get("kucoin");
        if (kucoinConfig != null && kucoinConfig.isEnabled()) {
            Set<String> configSymbols = new HashSet<>(kucoinConfig.getSymbols());
            Duration staleThreshold = Duration.ofMillis(
                    kucoinConfig.getDefaults().getOrderbookStaleThresholdMs()
            );
            
            // Kucoin uses external symbols in config (DOGE-USDT), need to match
            List<ManagerSymbol> kucoin = registry.getActiveSymbols(Exchange.KUCOIN)
                    .stream()
                    .filter(s -> configSymbols.contains(s.symbol) || 
                                 configSymbols.contains(s.externalSymbol))
                    .limit(SYMBOL_LIMIT)
                    .toList();
            
            System.out.println("[KUCOIN] Config symbols: " + configSymbols.size() + 
                    ", matched from DB: " + kucoin.size());
            logUnmatchedSymbols("KUCOIN", configSymbols, kucoin);
            
            if (!kucoin.isEmpty()) {
                List<KucoinSpotFeed.Sub> subs = kucoin.stream()
                        .map(s -> new KucoinSpotFeed.Sub(s.symbol, s.externalSymbol))
                        .collect(Collectors.toList());
                new KucoinSpotFeed(
                        subs,
                        store,
                        external -> registry.mapExternalToInternal(Exchange.KUCOIN, external),
                        staleThreshold
                ).start();
                kucoin.forEach(s -> symbolsForGui.add(s.symbol));
                subscribedByExchange.put(Exchange.KUCOIN, 
                        kucoin.stream().map(s -> s.symbol).collect(Collectors.toSet()));
                totalFeeds += kucoin.size();
            }
        } else {
            System.out.println("[KUCOIN] Disabled or no config");
        }

        // --- MEXC ---
        FeedConfig mexcConfig = feedConfigs.get("mexc");
        if (mexcConfig != null && mexcConfig.isEnabled()) {
            Set<String> configSymbols = new HashSet<>(mexcConfig.getSymbols());
            int depth = mexcConfig.getDefaults().getDepth();
            
            List<ManagerSymbol> mexc = registry.getActiveSymbols(Exchange.MEXC)
                    .stream()
                    .filter(s -> configSymbols.contains(s.symbol))
                    .limit(SYMBOL_LIMIT)
                    .toList();
            
            System.out.println("[MEXC] Config symbols: " + configSymbols.size() + 
                    ", matched from DB: " + mexc.size() + ", depth: " + depth);
            logUnmatchedSymbols("MEXC", configSymbols, mexc);
            
            if (!mexc.isEmpty()) {
                List<String> externalSymbols = mexc.stream()
                        .map(s -> s.externalSymbol)
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
                
                MexcSpotFeed mexcFeed = new MexcSpotFeed(
                        externalSymbols,
                        store,
                        depth,
                        external -> registry.mapExternalToInternal(Exchange.MEXC, external)
                );
                mexcFeed.start();
                mexc.forEach(s -> symbolsForGui.add(s.symbol));
                subscribedByExchange.put(Exchange.MEXC, 
                        mexc.stream().map(s -> s.symbol).collect(Collectors.toSet()));
                totalFeeds += mexc.size();
            }
        } else {
            System.out.println("[MEXC] Disabled or no config");
        }

        // --- BINANCE (Spot) ---
        FeedConfig binanceConfig = feedConfigs.get("binance");
        if (binanceConfig != null && binanceConfig.isEnabled()) {
            Set<String> configSymbols = new HashSet<>(binanceConfig.getSymbols());
            
            List<ManagerSymbol> binance = registry.getActiveSymbols(Exchange.BINANCE)
                    .stream()
                    .filter(s -> configSymbols.contains(s.symbol))
                    .limit(SYMBOL_LIMIT)
                    .toList();
            
            System.out.println("[BINANCE] Config symbols: " + configSymbols.size() + 
                    ", matched from DB: " + binance.size());
            logUnmatchedSymbols("BINANCE", configSymbols, binance);
            
            if (!binance.isEmpty()) {
                List<BinanceSpotFeed.Sub> subs = binance.stream()
                        .map(s -> new BinanceSpotFeed.Sub(s.symbol, s.externalSymbol))
                        .collect(Collectors.toList());
                new BinanceSpotFeed(
                        subs,
                        store,
                        external -> registry.mapExternalToInternal(Exchange.BINANCE, external)
                ).start();
                binance.forEach(s -> symbolsForGui.add(s.symbol));
                subscribedByExchange.put(Exchange.BINANCE, 
                        binance.stream().map(s -> s.symbol).collect(Collectors.toSet()));
                totalFeeds += binance.size();
            }
        } else {
            System.out.println("[BINANCE] Disabled or no config");
        }

        // --- GATEFUT ---
        FeedConfig gatefutConfig = feedConfigs.get("gatefut");
        if (gatefutConfig != null && gatefutConfig.isEnabled()) {
            Set<String> configSymbols = new HashSet<>(gatefutConfig.getSymbols());
            int depth = gatefutConfig.getDefaults().getDepth();
            
            List<ManagerSymbol> gatefut = registry.getActiveSymbols(Exchange.GATEFUT)
                    .stream()
                    .filter(s -> configSymbols.contains(s.symbol))
                    .limit(SYMBOL_LIMIT)
                    .toList();
            
            System.out.println("[GATEFUT] Config symbols: " + configSymbols.size() + 
                    ", matched from DB: " + gatefut.size() + ", depth: " + depth);
            logUnmatchedSymbols("GATEFUT", configSymbols, gatefut);
            
            if (!gatefut.isEmpty()) {
                List<GateFuturesFeed.Sub> subs = gatefut.stream()
                        .map(s -> new GateFuturesFeed.Sub(
                                s.symbol, 
                                s.externalSymbol,
                                s.contractSize != null ? s.contractSize : BigDecimal.ONE
                        ))
                        .collect(Collectors.toList());
                long silenceTimeout = gatefutConfig.getDefaults().getGlobalSilenceTimeoutMs();
                long fundingPollIntervalMs = gatefutConfig.getDefaults().getFundingPollIntervalMs();
                GateFuturesFeed feed = new GateFuturesFeed(
                        subs,
                        store,
                        external -> registry.mapExternalToInternal(Exchange.GATEFUT, external),
                        depth,
                        silenceTimeout,
                        fundingStore,
                        fundingPollIntervalMs
                );
                gateFutFeedRef.set(feed);
                fundingService.registerRefresher(Exchange.GATEFUT, feed::refreshFundingNow);
                feed.start();
                gatefut.forEach(s -> symbolsForGui.add(s.symbol));
                subscribedByExchange.put(Exchange.GATEFUT, 
                        gatefut.stream().map(s -> s.symbol).collect(Collectors.toSet()));
                totalFeeds += gatefut.size();
            }
        } else {
            System.out.println("[GATEFUT] Disabled or no config");
        }

        // --- GATE (Spot) ---
        FeedConfig gateConfig = feedConfigs.get("gatespot");
        if (gateConfig != null && gateConfig.isEnabled()) {
            Set<String> configSymbols = new HashSet<>(gateConfig.getSymbols());
            int depth = gateConfig.getDefaults().getDepth();
            
            List<ManagerSymbol> gate = registry.getActiveSymbols(Exchange.GATE)
                    .stream()
                    .filter(s -> configSymbols.contains(s.symbol))
                    .limit(SYMBOL_LIMIT)
                    .toList();
            
            System.out.println("[GATE] Config symbols: " + configSymbols.size() + 
                    ", matched from DB: " + gate.size() + ", depth: " + depth);
            logUnmatchedSymbols("GATE", configSymbols, gate);
            
            if (!gate.isEmpty()) {
                List<GateSpotFeed.Sub> subs = gate.stream()
                        .map(s -> new GateSpotFeed.Sub(s.symbol, s.externalSymbol))
                        .collect(Collectors.toList());
                long silenceTimeout = gateConfig.getDefaults().getGlobalSilenceTimeoutMs();
                new GateSpotFeed(
                        subs,
                        store,
                        external -> registry.mapExternalToInternal(Exchange.GATE, external),
                        depth,
                        silenceTimeout
                ).start();
                gate.forEach(s -> symbolsForGui.add(s.symbol));
                subscribedByExchange.put(Exchange.GATE, 
                        gate.stream().map(s -> s.symbol).collect(Collectors.toSet()));
                totalFeeds += gate.size();
            }
        } else {
            System.out.println("[GATE] Disabled or no config");
        }

        // --- CROSS / FX RATE FEEDS (kilka streamów, niski overhead) ---
        CrossRateStore crossRateStore = null;
        List<MarketDataFeed> crossFeeds = new ArrayList<>();
        boolean crossFeedsEnabled = Boolean.parseBoolean(System.getenv().getOrDefault("CROSS_FEEDS_ENABLED", "true"));
        if (!crossFeedsEnabled) {
            System.out.println("[CROSS] Cross feeds disabled (CROSS_FEEDS_ENABLED=false)");
        }
        Map<String, CrossFeedConfig> crossFeedConfigs = crossFeedsEnabled ? CrossFeedConfigLoader.loadAll() : Map.of();
        for (Map.Entry<String, CrossFeedConfig> entry : crossFeedConfigs.entrySet()) {
            String feedId = entry.getKey();
            CrossFeedConfig cfg = entry.getValue();
            if (!cfg.isEnabled() || cfg.getSymbols().isEmpty()) continue;

            if (crossRateStore == null) crossRateStore = new CrossRateStore();

            for (Map.Entry<String, CrossFeedConfig.SymbolConfig> symEntry : cfg.getSymbols().entrySet()) {
                String symbol = symEntry.getKey();
                CrossFeedConfig.SymbolConfig symCfg = symEntry.getValue();
                if (!"BINANCE".equalsIgnoreCase(symCfg.getSource())) {
                    System.out.println("[CROSS] Skipping " + feedId + "/" + symbol + " - source " + symCfg.getSource() + " not yet supported");
                    continue;
                }
                EurUsdtBinanceCrossFeed cf = new EurUsdtBinanceCrossFeed(feedId, symbol, crossRateStore, cfg);
                cf.start();
                crossFeeds.add(cf);
                System.out.println("[CROSS] Started " + feedId + "/" + symbol);
            }
        }
        CrossRateStore finalCrossRateStore = crossRateStore;

        // --- BITVAVO (native EUR, cross in MM) ---
        FeedConfig bitvavoConfig = feedConfigs.get("bitvavo");
        if (bitvavoConfig != null && bitvavoConfig.isEnabled()) {
            Set<String> configSymbols = new HashSet<>(bitvavoConfig.getSymbols());
            List<ManagerSymbol> bitvavo = registry.getActiveSymbols(Exchange.BITVAVO)
                    .stream()
                    .filter(s -> configSymbols.contains(s.symbol) || 
                                 configSymbols.contains(s.externalSymbol) ||
                                 (s.externalSymbol != null && configSymbols.contains(s.externalSymbol.replace("_", "-"))))
                    .limit(SYMBOL_LIMIT)
                    .toList();
            
            System.out.println("[BITVAVO] Config symbols: " + configSymbols.size() + 
                    ", matched from DB: " + bitvavo.size());
            logUnmatchedSymbols("BITVAVO", configSymbols, bitvavo);
            
            if (!bitvavo.isEmpty()) {
                Map<String, String> bitvavoMap = new HashMap<>();
                for (ManagerSymbol s : bitvavo) {
                    if (s.externalSymbol != null && !s.externalSymbol.isBlank()) {
                        bitvavoMap.put(s.externalSymbol.trim(), s.symbol);
                        String dash = s.externalSymbol.replace("_", "-").trim();
                        if (!dash.equals(s.externalSymbol.trim())) bitvavoMap.put(dash, s.symbol);
                    }
                }
                boolean useMdPro = Boolean.parseBoolean(System.getenv().getOrDefault("BITVAVO_USE_MARKET_DATA_PRO", "false"));
                String apiKey = router.SecretsLoader.loadSecret("bitvavo_feed_key");
                if (apiKey == null) apiKey = System.getenv("BITVAVO_FEED_KEY");
                String apiSecret = router.SecretsLoader.loadSecret("bitvavo_feed_secret");
                if (apiSecret == null) apiSecret = System.getenv("BITVAVO_FEED_SECRET");
                if (useMdPro && apiKey != null && !apiKey.isBlank() && apiSecret != null && !apiSecret.isBlank()) {
                    new BitvavoMarketDataProFeed(store, bitvavoMap).start();
                    System.out.println("[BITVAVO] Using WS Market Data Pro (BITVAVO_USE_MARKET_DATA_PRO=true)");
                } else {
                    int batchSize = bitvavoConfig.getDefaults() != null ? bitvavoConfig.getDefaults().getBatchSize() : 50;
                    new BitvavoSpotBatchFeed(store, bitvavoMap, batchSize).start();
                }
                bitvavo.forEach(s -> symbolsForGui.add(s.symbol));
                subscribedByExchange.put(Exchange.BITVAVO, 
                        bitvavo.stream().map(s -> s.symbol).collect(Collectors.toSet()));
                totalFeeds += bitvavo.size();
            }
        } else {
            System.out.println("[BITVAVO] Disabled or no config");
        }

        /* [DISABLED - Kraken, Bitkub]
        // --- KRAKEN ---
        FeedConfig krakenConfig = feedConfigs.get("kraken");
        if (krakenConfig != null && krakenConfig.isEnabled()) {
            Set<String> configSymbols = new HashSet<>(krakenConfig.getSymbols());
            List<ManagerSymbol> kraken = registry.getActiveSymbols(Exchange.KRAKEN)
                    .stream()
                    .filter(s -> configSymbols.contains(s.symbol) || 
                                 configSymbols.contains(s.externalSymbol))
                    .limit(SYMBOL_LIMIT)
                    .toList();
            
            System.out.println("[KRAKEN] Config symbols: " + configSymbols.size() + 
                    ", matched from DB: " + kraken.size());
            logUnmatchedSymbols("KRAKEN", configSymbols, kraken);
            
            if (!kraken.isEmpty()) {
                Map<String, String> krakenMap = new HashMap<>();
                for (ManagerSymbol s : kraken) {
                    if (s.externalSymbol != null) {
                        krakenMap.put(s.externalSymbol, s.symbol);
                    }
                }
                new KrakenSpotFeed(store, krakenMap).connect();
                kraken.forEach(s -> symbolsForGui.add(s.symbol));
                subscribedByExchange.put(Exchange.KRAKEN, 
                        kraken.stream().map(s -> s.symbol).collect(Collectors.toSet()));
                totalFeeds += kraken.size();
            }
        }

        // --- BITKUB ---
        FeedConfig bitkubConfig = feedConfigs.get("bitkub");
        if (bitkubConfig != null && bitkubConfig.isEnabled()) {
            Set<String> configSymbols = new HashSet<>(bitkubConfig.getSymbols());
            int batchSize = bitkubConfig.getDefaults().getBatchSize();
            
            List<ManagerSymbol> bitkub = registry.getActiveSymbols(Exchange.BITKUB)
                    .stream()
                    .filter(s -> configSymbols.contains(s.symbol))
                    .limit(SYMBOL_LIMIT)
                    .toList();
            
            System.out.println("[BITKUB] Config symbols: " + configSymbols.size() + 
                    ", matched from DB: " + bitkub.size() + ", batchSize: " + batchSize);
            logUnmatchedSymbols("BITKUB", configSymbols, bitkub);
            
            if (!bitkub.isEmpty()) {
                List<ManagerSymbol> remaining = new ArrayList<>(bitkub);
                int batchId = 1;
                while (!remaining.isEmpty()) {
                    int take = Math.min(remaining.size(), batchSize);
                    List<ManagerSymbol> batch = remaining.subList(0, take);
                    
                    Map<String, String> bitkubMap = new HashMap<>();
                    for (ManagerSymbol s : batch) {
                        if (s.externalSymbol != null) {
                            bitkubMap.put(s.externalSymbol, s.symbol);
                        }
                    }
                    
                    System.out.println("[BITKUB] Starting batch #" + batchId + " with " + bitkubMap.size() + " symbols");
                    BitkubSpotFeed.create(store, bitkubMap).connect();
                    
                    remaining = remaining.subList(take, remaining.size());
                    batchId++;
                }
                bitkub.forEach(s -> symbolsForGui.add(s.symbol));
                subscribedByExchange.put(Exchange.BITKUB, 
                        bitkub.stream().map(s -> s.symbol).collect(Collectors.toSet()));
                totalFeeds += bitkub.size();
            }
        }
        */

        System.out.println("=== Feeds started. Total symbols: " + totalFeeds + " ===");

        // Start watchdog for missing/stale symbols
        startStaleSymbolWatchdog(
                (InMemoryOrderBookStore) store, 
                subscribedByExchange,
                Duration.ofMinutes(2),   // Stale threshold: 2 minutes without update
                Duration.ofMinutes(1),   // Grace period: wait 1 min after startup before warning about missing data
                Duration.ofSeconds(60)   // Check every 60 seconds
        );

        // ------------------------------------------------------------
        // GUI symbols list - include ALL active symbols from ALL exchanges
        // ------------------------------------------------------------
        Set<String> allSymbolsForGui = new LinkedHashSet<>(symbolsForGui);
        
        // Add all active symbols from all exchanges in the registry
        // (getActiveSymbols already filters by is_active = true, but we double-check)
        for (Exchange ex : Exchange.values()) {
            List<ManagerSymbol> allActive = registry.getActiveSymbols(ex);
            for (ManagerSymbol ms : allActive) {
                if (ms.isActive) {  // Explicit check: only is_active = true
                    allSymbolsForGui.add(ms.symbol);
                }
            }
        }
        
        String[] guiSymbols = allSymbolsForGui.stream()
                .sorted()
                .toArray(String[]::new);
        
        System.out.println("[GUI] Total symbols available: " + guiSymbols.length + 
                " (subscribed: " + symbolsForGui.size() + ")");

        // ------------------------------------------------------------
        // Serwisy pomocnicze (Snapshot, Stats, Writers)
        // ------------------------------------------------------------
        OrderBookSnapshotService snapshotService = null;
        try {
            snapshotService = new OrderBookSnapshotService(
                    store,
                    Exchange.values(),
                    java.util.List.of(guiSymbols),
                    pg
            );
        } catch (Throwable t) {
            System.err.println("[BOOT] snapshot service init failed: " + t);
        }
        OrderBookSnapshotService finalSnapshotService = snapshotService;

        MarketDataArbStatsWriter arbStatsWriter = null;
        try {
            arbStatsWriter = new MarketDataArbStatsWriter(pg);
        } catch (Throwable t) {
            System.err.println("[BOOT] arbStatsWriter init failed");
        }
        MarketDataArbStatsWriter finalArbStatsWriter = arbStatsWriter;

        MarketDataArbRollingGainWriter rollingGainWriter = null;
        try {
            rollingGainWriter = new MarketDataArbRollingGainWriter(pg);
        } catch (Throwable t) {
            System.err.println("[BOOT] rollingGainWriter init failed");
        }
        MarketDataArbRollingGainWriter finalRollingGainWriter = rollingGainWriter;


        // ------------------------------------------------------------
        // Uruchomienie GUI lub tryb HEADLESS
        // ------------------------------------------------------------
        boolean startGui = Boolean.parseBoolean(System.getenv().getOrDefault("OB_GUI", "true"));
        final int finalTotalFeeds = totalFeeds;
        final Map<Exchange, Set<String>> finalSubscribedByExchange = subscribedByExchange;
        
        if (startGui) {
            // ========== TRYB GUI (Windows) ==========
            SwingUtilities.invokeLater(() -> {
                try {
                    System.out.println("[BOOT] launching GUI...");
                    MultiExchangeOrderBookViewer gui = new MultiExchangeOrderBookViewer(
                            store,
                            guiSymbols,
                            finalArbStatsWriter,
                            finalRollingGainWriter,
                            registry,
                            fundingService
                    );

                    if (finalSnapshotService != null) {
                        gui.setSnapshotService(finalSnapshotService);
                    }
                    gui.show();
                } catch (Throwable t) {
                    System.err.println("[BOOT] GUI failed: " + t);
                    t.printStackTrace();
                }
            });
        } else {
            // ========== TRYB HEADLESS (Linux/Docker) ==========
            System.out.println("[BOOT] Running in HEADLESS mode (no GUI)");
            System.out.println("[BOOT] Total feeds started: " + finalTotalFeeds);
            
            // gRPC server (stream all orderbooks + funding rate to Proxy)
            int grpcPort = Integer.parseInt(System.getenv().getOrDefault("FEED_GRPC_PORT", "50050"));
            startFeedGrpcServer(broadcaster, fundingService.getStore(), grpcPort);
            
            // Health HTTP server
            int healthPort = Integer.parseInt(System.getenv().getOrDefault("HEALTH_PORT", "8080"));
            startHealthServer(healthPort, store, finalSubscribedByExchange, finalCrossRateStore);

            // Headless analytics (controlled by env from demo-config)
            startHeadlessAnalytics(
                    store,
                    registry,
                    guiSymbols,
                    finalSnapshotService,
                    finalRollingGainWriter
            );
            
            // Shutdown hook for graceful shutdown
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("[SHUTDOWN] Received shutdown signal, cleaning up...");
                System.out.println("[SHUTDOWN] Bye!");
            }, "shutdown-hook"));
            
            // Blocking loop - keeps JVM alive
            // Responds to SIGTERM/SIGINT (Ctrl+C, docker stop)
            System.out.println("[BOOT] Feed running. Send SIGTERM or Ctrl+C to stop.");
            final Object lock = new Object();
            synchronized (lock) {
                try {
                    lock.wait();
                } catch (InterruptedException e) {
                    System.out.println("[BOOT] Main thread interrupted, shutting down...");
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private static void startFeedGrpcServer(OrderBookUpdateBroadcaster broadcaster, FundingRateStore fundingStore, int port) {
        try {
            FeedGrpcServer grpcServer = new FeedGrpcServer(broadcaster, fundingStore, port);
            grpcServer.start();
            Runtime.getRuntime().addShutdownHook(new Thread(grpcServer::shutdown, "feed-grpc-shutdown"));
        } catch (Exception e) {
            System.err.println("[BOOT] Feed gRPC server failed to start: " + e.getMessage());
        }
    }

    /**
     * Minimalist health endpoint for Docker/Kubernetes healthchecks.
     * Endpoints:
     * - /health - returns status based on feed data availability
     * - /ready - always returns 200 (readiness probe)
     */
    private static void startHealthServer(int port, InMemoryOrderBookStore store, 
                                          Map<Exchange, Set<String>> subscribed,
                                          CrossRateStore crossRateStore) {
        try {
            com.sun.net.httpserver.HttpServer server = 
                com.sun.net.httpserver.HttpServer.create(new java.net.InetSocketAddress(port), 0);
            
            server.createContext("/health", exchange -> {
                int totalSymbols = subscribed.values().stream().mapToInt(Set::size).sum();
                int withData = 0;
                long now = System.currentTimeMillis();
                int staleCount = 0;
                
                for (var entry : subscribed.entrySet()) {
                    for (String sym : entry.getValue()) {
                        OrderBookSnapshot snap = store.get(entry.getKey(), sym);
                        if (snap != null) {
                            withData++;
                            if (now - snap.getEventTimeMillis() > 60000) staleCount++;
                        }
                    }
                }
                
                String status = withData > 0 ? "UP" : "STARTING";
                int httpCode = withData > 0 ? 200 : 503;
                
                String body = String.format(
                    "{\"status\":\"%s\",\"totalSymbols\":%d,\"withData\":%d,\"stale\":%d,\"uptimeMs\":%d}", 
                    status, totalSymbols, withData, staleCount, 
                    java.lang.management.ManagementFactory.getRuntimeMXBean().getUptime()
                );
                
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(httpCode, body.length());
                try (var os = exchange.getResponseBody()) {
                    os.write(body.getBytes());
                }
            });
            
            server.createContext("/ready", exchange -> {
                String body = "{\"ready\":true}";
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, body.length());
                try (var os = exchange.getResponseBody()) {
                    os.write(body.getBytes());
                }
            });

            // --- CROSS RATES (kilka streamów, endpoint do weryfikacji) ---
            server.createContext("/cross", exchange -> {
                if (!"GET".equals(exchange.getRequestMethod())) {
                    exchange.sendResponseHeaders(405, 0);
                    exchange.close();
                    return;
                }
                if (crossRateStore == null) {
                    String body = "{\"cross\":{},\"message\":\"no cross feeds configured\"}";
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    exchange.sendResponseHeaders(200, body.length());
                    try (var os = exchange.getResponseBody()) { os.write(body.getBytes()); }
                    return;
                }
                CrossRateStore.Quote q = crossRateStore.getQuote("feed_forex", "EURUSDT");
                long now = System.currentTimeMillis();
                String body = q != null && q.isValid()
                        ? String.format("{\"feed_forex\":{\"EURUSDT\":{\"mid\":%.6f,\"bid\":%.6f,\"ask\":%.6f,\"ageMs\":%d}}}",
                                q.mid, q.bid, q.ask, now - q.tsMs)
                        : "{\"feed_forex\":{\"EURUSDT\":null,\"message\":\"no data or stale\"}}";
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, body.length());
                try (var os = exchange.getResponseBody()) { os.write(body.getBytes()); }
            });
            
            // --- PRICE VALIDATION ENDPOINT (non-invasive, read-only) ---
            // Returns current prices for all subscribed symbols, used for pre-MM validation
            server.createContext("/prices", exchange -> {
                if (!"GET".equals(exchange.getRequestMethod())) {
                    exchange.sendResponseHeaders(405, 0);
                    exchange.close();
                    return;
                }
                
                try {
                    Map<String, Map<String, Object>> prices = new HashMap<>();
                    long now = System.currentTimeMillis();
                    
                    // Iterate through all subscribed symbols
                    for (var entry : subscribed.entrySet()) {
                        Exchange ex = entry.getKey();
                        Map<String, Object> exchangePrices = new HashMap<>();
                        
                        for (String symbol : entry.getValue()) {
                            OrderBookSnapshot snap = store.get(ex, symbol);
                            if (snap != null) {
                                List<OrderBookLevel> bids = snap.getBids();
                                List<OrderBookLevel> asks = snap.getAsks();
                                
                                double bestBid = (bids != null && !bids.isEmpty() && bids.get(0) != null) 
                                    ? bids.get(0).getPrice() : 0.0;
                                double bestAsk = (asks != null && !asks.isEmpty() && asks.get(0) != null) 
                                    ? asks.get(0).getPrice() : 0.0;
                                
                                double midPrice = 0.0;
                                if (bestBid > 0 && bestAsk > 0) {
                                    midPrice = (bestBid + bestAsk) / 2.0;
                                } else if (bestBid > 0) {
                                    midPrice = bestBid;
                                } else if (bestAsk > 0) {
                                    midPrice = bestAsk;
                                }
                                
                                long ageMs = now - snap.getEventTimeMillis();
                                
                                Map<String, Object> symbolData = new HashMap<>();
                                symbolData.put("midPrice", midPrice);
                                symbolData.put("bestBid", bestBid);
                                symbolData.put("bestAsk", bestAsk);
                                symbolData.put("ageMs", ageMs);
                                symbolData.put("hasData", midPrice > 0);
                                
                                exchangePrices.put(symbol, symbolData);
                            }
                        }
                        
                        if (!exchangePrices.isEmpty()) {
                            prices.put(ex.name(), exchangePrices);
                        }
                    }
                    
                    // Simple JSON serialization (avoid external dependencies)
                    StringBuilder json = new StringBuilder();
                    json.append("{\n");
                    boolean firstExchange = true;
                    for (var exEntry : prices.entrySet()) {
                        if (!firstExchange) json.append(",\n");
                        firstExchange = false;
                        json.append("  \"").append(exEntry.getKey()).append("\": {\n");
                        boolean firstSymbol = true;
                        for (var symEntry : exEntry.getValue().entrySet()) {
                            if (!firstSymbol) json.append(",\n");
                            firstSymbol = false;
                            @SuppressWarnings("unchecked")
                            Map<String, Object> data = (Map<String, Object>) symEntry.getValue();
                            json.append("    \"").append(symEntry.getKey()).append("\": {\n");
                            json.append("      \"midPrice\": ").append(data.get("midPrice")).append(",\n");
                            json.append("      \"bestBid\": ").append(data.get("bestBid")).append(",\n");
                            json.append("      \"bestAsk\": ").append(data.get("bestAsk")).append(",\n");
                            json.append("      \"ageMs\": ").append(data.get("ageMs")).append(",\n");
                            json.append("      \"hasData\": ").append(data.get("hasData"));
                            json.append("\n    }");
                        }
                        json.append("\n  }");
                    }
                    json.append("\n}");
                    
                    String body = json.toString();
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    exchange.sendResponseHeaders(200, body.length());
                    try (var os = exchange.getResponseBody()) {
                        os.write(body.getBytes());
                    }
                } catch (Exception e) {
                    String error = "{\"error\":\"" + e.getMessage().replace("\"", "\\\"") + "\"}";
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    exchange.sendResponseHeaders(500, error.length());
                    try (var os = exchange.getResponseBody()) {
                        os.write(error.getBytes());
                    }
                }
            });
            
            // --- ORDERBOOK SNAPSHOT ENDPOINT (read-only) ---
            // GET /orderbook?ex=BITGET&sym=QUSDT&levels=20
            server.createContext("/orderbook", exchange -> {
                if (!"GET".equals(exchange.getRequestMethod())) {
                    exchange.sendResponseHeaders(405, 0);
                    exchange.close();
                    return;
                }
                try {
                    Map<String, String> params = parseQuery(exchange.getRequestURI().getQuery());
                    String exStr = params.get("ex");
                    String sym = params.get("sym");
                    int levels = params.containsKey("levels") ? Integer.parseInt(params.get("levels")) : 20;
                    
                    if (exStr == null || sym == null) {
                        String error = "{\"error\":\"Missing ex or sym\"}";
                        exchange.getResponseHeaders().set("Content-Type", "application/json");
                        exchange.sendResponseHeaders(400, error.length());
                        try (var os = exchange.getResponseBody()) {
                            os.write(error.getBytes());
                        }
                        return;
                    }
                    
                    Exchange ex = Exchange.valueOf(exStr.toUpperCase());
                    OrderBookSnapshot snap = store.get(ex, sym);
                    
                    StringBuilder json = new StringBuilder();
                    json.append("{\"exchange\":\"").append(exStr).append("\",\"symbol\":\"").append(sym).append("\",");
                    if (snap == null) {
                        json.append("\"hasData\":false}");
                    } else {
                        long ageMs = System.currentTimeMillis() - snap.getEventTimeMillis();
                        json.append("\"hasData\":true,");
                        json.append("\"ts\":").append(snap.getEventTimeMillis()).append(",");
                        json.append("\"ageMs\":").append(ageMs).append(",");
                        json.append("\"bids\":[");
                        appendLevels(json, snap.getBids(), levels);
                        json.append("],\"asks\":[");
                        appendLevels(json, snap.getAsks(), levels);
                        json.append("]}");
                    }
                    
                    String body = json.toString();
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    exchange.sendResponseHeaders(200, body.length());
                    try (var os = exchange.getResponseBody()) {
                        os.write(body.getBytes());
                    }
                } catch (Exception e) {
                    String error = "{\"error\":\"" + e.getMessage().replace("\"", "\\\"") + "\"}";
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    exchange.sendResponseHeaders(500, error.length());
                    try (var os = exchange.getResponseBody()) {
                        os.write(error.getBytes());
                    }
                }
            });
            
            server.setExecutor(java.util.concurrent.Executors.newFixedThreadPool(3));
            server.start();
            System.out.println("[HEALTH] HTTP server listening on port " + port);
            System.out.println("[HEALTH] Endpoints: GET /health, GET /ready, GET /prices, GET /orderbook");
        } catch (Exception e) {
            System.err.println("[HEALTH] Failed to start health server: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private static Map<String, String> parseQuery(String query) {
        Map<String, String> out = new HashMap<>();
        if (query == null || query.isBlank()) return out;
        String[] parts = query.split("&");
        for (String part : parts) {
            int idx = part.indexOf('=');
            if (idx <= 0) continue;
            String key = part.substring(0, idx);
            String val = part.substring(idx + 1);
            out.put(key, val);
        }
        return out;
    }
    
    private static void appendLevels(StringBuilder json, List<OrderBookLevel> levels, int limit) {
        if (levels == null || levels.isEmpty() || limit <= 0) {
            return;
        }
        int max = Math.min(limit, levels.size());
        for (int i = 0; i < max; i++) {
            OrderBookLevel lvl = levels.get(i);
            if (lvl == null) continue;
            if (i > 0) json.append(",");
            json.append("{\"p\":").append(lvl.getPrice()).append(",\"s\":").append(lvl.getSize()).append("}");
        }
    }

    /**
     * Normalizuje snapshot uwzględniając mnożniki (np. dla par 1000SHIB).
     */
    private static OrderBookSnapshot normalizeSnapshot(SymbolRegistry registry, Exchange exchange, String symbol, OrderBookSnapshot raw) {
        if (raw == null) return null;
        ManagerSymbol ms = registry.getByInternal(exchange, symbol);
        if (ms == null || ms.quotation == null || ms.quotation.compareTo(BigDecimal.ONE) == 0) {
            return raw;
        }
        double multiplier = ms.quotation.doubleValue();
        if (multiplier == 1.0) return raw;

        return new OrderBookSnapshot(
                raw.getExchange(),
                raw.getSymbol(),
                raw.getEventTimeMillis(),
                multiplyLevels(raw.getBids(), multiplier),
                multiplyLevels(raw.getAsks(), multiplier)
        );
    }

    private static List<OrderBookLevel> multiplyLevels(List<OrderBookLevel> levels, double multiplier) {
        if (levels == null) return new ArrayList<>();
        List<OrderBookLevel> result = new ArrayList<>(levels.size());
        for (OrderBookLevel lvl : levels) {
            result.add(new OrderBookLevel(lvl.getPrice() / multiplier, lvl.getSize()));
        }
        return result;
    }

    /**
     * Logs warning for symbols in YAML config that don't exist in DB (or are inactive).
     */
    private static void logUnmatchedSymbols(String exchange, Set<String> configSymbols, List<ManagerSymbol> matched) {
        Set<String> matchedSymbols = matched.stream()
                .map(s -> s.symbol)
                .collect(Collectors.toSet());
        
        List<String> unmatched = configSymbols.stream()
                .filter(s -> !matchedSymbols.contains(s))
                .sorted()
                .toList();
        
        if (!unmatched.isEmpty()) {
            FeedLogger.warn(exchange, "Symbols in config but NOT in DB (or inactive): " + 
                    unmatched.size() + " symbols");
            // Log first 10 for brevity
            int show = Math.min(10, unmatched.size());
            FeedLogger.warn(exchange, "Unmatched examples: " + unmatched.subList(0, show) +
                    (unmatched.size() > show ? " ... and " + (unmatched.size() - show) + " more" : ""));
        }
    }

    /**
     * Starts a watchdog that monitors for:
     * 1. Missing data - symbols that never received any orderbook since startup
     * 2. Stale data - symbols that stopped receiving updates
     */
    private static void startStaleSymbolWatchdog(InMemoryOrderBookStore store, 
                                                  Map<Exchange, Set<String>> subscribedByExchange,
                                                  Duration staleThreshold, 
                                                  Duration initialGracePeriod,
                                                  Duration checkInterval) {
        final long startTime = System.currentTimeMillis();
        final Set<String> alreadyWarnedNoData = ConcurrentHashMap.newKeySet();
        final Set<String> alreadyWarnedStale = ConcurrentHashMap.newKeySet();
        
        java.util.concurrent.Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "stale-symbol-watchdog");
            t.setDaemon(true);
            return t;
        }).scheduleAtFixedRate(() -> {
            try {
                long now = System.currentTimeMillis();
                long thresholdMs = staleThreshold.toMillis();
                long graceMs = initialGracePeriod.toMillis();
                boolean pastGracePeriod = (now - startTime) > graceMs;
                
                for (Map.Entry<Exchange, Set<String>> entry : subscribedByExchange.entrySet()) {
                    Exchange ex = entry.getKey();
                    Set<String> symbols = entry.getValue();
                    
                    for (String symbol : symbols) {
                        String key = ex.name() + ":" + symbol;
                        OrderBookSnapshot snap = store.get(ex, symbol);
                        
                        if (snap == null) {
                            // No data ever received
                            if (pastGracePeriod && !alreadyWarnedNoData.contains(key)) {
                                FeedLogger.warn(ex.name(), "NO DATA for " + symbol + 
                                        " - never received orderbook since startup (" + 
                                        ((now - startTime) / 1000) + "s ago)");
                                alreadyWarnedNoData.add(key);
                            }
                        } else {
                            // Has data - check if stale
                            alreadyWarnedNoData.remove(key); // Got data, clear no-data warning
                            
                            long age = now - snap.getEventTimeMillis();
                            if (age > thresholdMs) {
                                if (!alreadyWarnedStale.contains(key)) {
                                    FeedLogger.warn(ex.name(), "STALE orderbook for " + symbol + 
                                            " - last update " + (age / 1000) + "s ago");
                                    alreadyWarnedStale.add(key);
                                }
                            } else {
                                alreadyWarnedStale.remove(key); // Data fresh again, clear stale warning
                            }
                        }
                    }
                }
            } catch (Exception e) {
                FeedLogger.error("WATCHDOG", "Symbol check failed: " + e.getMessage());
            }
        }, checkInterval.toMillis(), checkInterval.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
        
        int totalSymbols = subscribedByExchange.values().stream().mapToInt(Set::size).sum();
        FeedLogger.info("WATCHDOG", "Started monitoring " + totalSymbols + " symbols across " + 
                subscribedByExchange.size() + " exchanges (stale=" + staleThreshold.toSeconds() + 
                "s, grace=" + initialGracePeriod.toSeconds() + "s, check=" + checkInterval.toSeconds() + "s)");
    }

    private static void startHeadlessAnalytics(
            OrderBookStore store,
            SymbolRegistry registry,
            String[] guiSymbols,
            OrderBookSnapshotService snapshotService,
            MarketDataArbRollingGainWriter rollingGainWriter
    ) {
        boolean saveDense = Boolean.parseBoolean(System.getenv().getOrDefault("SAVE_DENSE_SNAPSHOTS", "false"));
        boolean saveRolled = Boolean.parseBoolean(System.getenv().getOrDefault("SAVE_ROLLED_GAINS", "true"));

        if (saveDense && snapshotService != null) {
            long intervalMs = Long.parseLong(System.getenv().getOrDefault("DENSE_SNAPSHOT_INTERVAL_MS", "2000"));
            snapshotService.start(intervalMs);
            System.out.println("[BOOT] Headless dense snapshots started (interval=" + intervalMs + "ms)");
        }

        if (saveRolled && rollingGainWriter != null) {
            Map<String, Exchange> refMap = ReferenceExchangeMap.load(registry, guiSymbols);
            List<String> symbolList = Arrays.asList(guiSymbols);
            RollingGainDetector detector = new RollingGainDetector(
                    store,
                    Exchange.values(),
                    refMap,
                    symbolList,
                    rollingGainWriter,
                    registry
            );
            Thread t = new Thread(detector, "rolling-gain-detector");
            t.setDaemon(true);
            t.start();
            System.out.println("[BOOT] Headless RollingGainDetector started");
        }
    }
}
