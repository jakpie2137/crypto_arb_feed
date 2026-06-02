// MultiExchangeOrderBookViewer.java

package feed_filter;

import javax.swing.*;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import java.awt.*;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.time.Instant;
import java.util.Comparator;

import db_pg.MarketDataArbStatsWriter;
import db_pg.MarketDataArbRollingGainWriter;
import db_pg.ArbStatRecord;
import db_pg.PostgresConfig;
import manager_client.SymbolRegistry;
import manager_client.ManagerSymbol;


public class MultiExchangeOrderBookViewer {

    // UI-only: how many levels per side to render.
    private static final int DISPLAY_DEPTH = 20;

    private final OrderBookStore store;
    private final String[] symbols;
    private final MarketDataArbStatsWriter arbStatsWriter;
    private final MarketDataArbRollingGainWriter rollingGainWriter;
    private final Exchange[] exchanges;
    private final SymbolRegistry symbolRegistry;
    private final FundingRateStore fundingStore;
    private final FundingRateService fundingService;


    private OrderBookSnapshotService snapshotService;

    // Interval for DB snapshots (ms), controlled from GUI. Default 3s.
    private long snapshotIntervalMillis = 3000L;

    // --- Big gain detector config ---
    private static final long DETECTOR_WINDOW_MILLIS = 30_000L; // 30s
    private static final double DETECTOR_TOTAL_FEES_PCT = 0.0008;
    private static final double DETECTOR_TARGET_VOLUME_QUOTE =
            envDouble("ARB_DETECTOR_TARGET_VOLUME_USD", 5_000.0);
    private static final double DETECTOR_TARGET_GAIN_USD =
            envDouble("ARB_DETECTOR_TARGET_GAIN_USD", 20.0);

    private static double envDouble(String envName, double defaultValue) {
        String v = System.getenv(envName);
        if (v == null || v.isEmpty()) return defaultValue;
        try {
            return Double.parseDouble(v);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private static String fmtFeePair(BigDecimal buy, BigDecimal sell) {
        String b = (buy != null && buy.doubleValue() >= 0) ? String.format("%.4f", buy.doubleValue()) : "—";
        String s = (sell != null && sell.doubleValue() >= 0) ? String.format("%.4f", sell.doubleValue()) : "—";
        return b + " / " + s;
    }

    private static double toFee(BigDecimal fee) {
        if (fee == null) return 0.0004;
        double d = fee.doubleValue();
        return (d >= 0.0 && d <= 1.0) ? d : 0.0004;
    }

    private volatile boolean detectorRunning = false;
    private Thread detectorThread;
    private Thread rollingDetectorThread;
    private BigGainDetectorWindow detectorWindow;

    private enum OrderType { LIMIT, MARKET }


    public MultiExchangeOrderBookViewer(OrderBookStore store, String[] symbols) {
        this(store, symbols, null, null, null, null);
    }

    public MultiExchangeOrderBookViewer(OrderBookStore store, String[] symbols, MarketDataArbStatsWriter arbStatsWriter) {
        this(store, symbols, arbStatsWriter, null, null, null);
    }

    public MultiExchangeOrderBookViewer(OrderBookStore store,
                                        String[] symbols,
                                        MarketDataArbStatsWriter arbStatsWriter,
                                        MarketDataArbRollingGainWriter rollingGainWriter,
                                        SymbolRegistry registry) {
        this(store, symbols, arbStatsWriter, rollingGainWriter, registry, null);
    }

    public MultiExchangeOrderBookViewer(OrderBookStore store,
                                        String[] symbols,
                                        MarketDataArbStatsWriter arbStatsWriter,
                                        MarketDataArbRollingGainWriter rollingGainWriter,
                                        SymbolRegistry registry,
                                        FundingRateService fundingService) {
        this.store = store;
        this.symbols = symbols;
        this.arbStatsWriter = arbStatsWriter;
        this.rollingGainWriter = rollingGainWriter;
        this.exchanges = Exchange.values();
        this.symbolRegistry = registry;
        this.fundingService = fundingService;
        this.fundingStore = fundingService != null ? fundingService.getStore() : null;
    }


    public void setSnapshotService(OrderBookSnapshotService snapshotService) {
        this.snapshotService = snapshotService;
    }

    /**
     * Load reference exchange per symbol from manager.symbol (futures only).
     * Priority: BINANCEFUT > GATEFUT > BYBITFUT. If symbol on both BINANCEFUT and GATEFUT, use BINANCEFUT.
     * Includes subaccounts (BINANCEFUT8SUBFIN, GATEFUT8SUB0) mapped to their main feed.
     */
    private Map<String, Exchange> loadReferenceMap() {
        Map<String, Exchange> map = new HashMap<>();
        PostgresConfig pg = PostgresConfig.fromEnvOrDefaults();

        String sql =
                "SELECT symbol, ref_exchange FROM (" +
                "  SELECT symbol, ref_exchange," +
                "    ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY priority) AS rn" +
                "  FROM (" +
                "    SELECT symbol," +
                "      CASE WHEN UPPER(exchange) IN ('BINANCEFUT','BINANCEFUT8SUBFIN','BINANCEFUT8SUB0') THEN 'BINANCEFUT'" +
                "           WHEN UPPER(exchange) IN ('GATEFUT','GATEFUT8SUB0') THEN 'GATEFUT'" +
                "           WHEN UPPER(exchange) = 'BYBITFUT' THEN 'BYBITFUT'" +
                "      END AS ref_exchange," +
                "      CASE WHEN UPPER(exchange) IN ('BINANCEFUT','BINANCEFUT8SUBFIN','BINANCEFUT8SUB0') THEN 1" +
                "           WHEN UPPER(exchange) IN ('GATEFUT','GATEFUT8SUB0') THEN 2" +
                "           WHEN UPPER(exchange) = 'BYBITFUT' THEN 3 ELSE 4 END AS priority" +
                "    FROM manager.symbol" +
                "    WHERE is_active = true" +
                "      AND UPPER(exchange) IN ('BINANCEFUT','BINANCEFUT8SUBFIN','BINANCEFUT8SUB0','GATEFUT','GATEFUT8SUB0','BYBITFUT')" +
                "  ) t1" +
                "  WHERE ref_exchange IS NOT NULL" +
                ") t2 WHERE rn = 1";

        System.out.println("[GUI] Loading reference symbols from DB: " + pg.url);

        try (Connection conn = DriverManager.getConnection(pg.url, pg.user, pg.password);
             PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                String sym = rs.getString("symbol");
                String exStr = rs.getString("ref_exchange");
                try {
                    Exchange ex = Exchange.valueOf(exStr);
                    map.put(sym, ex);
                } catch (IllegalArgumentException e) {
                    System.err.println("[GUI] Warning: Unknown ref exchange: " + exStr);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            JOptionPane.showMessageDialog(null, "Error loading references: " + e.getMessage());
        }

        // Fallback 1: by exchange (registry has futures symbols)
        if (map.isEmpty() && symbolRegistry != null) {
            System.out.println("[GUI] Reference map empty from DB, trying SymbolRegistry by exchange...");
            for (Exchange ex : new Exchange[]{Exchange.BINANCEFUT, Exchange.BINANCEFUT8SUBFIN, Exchange.BINANCEFUT8SUB0}) {
                for (ManagerSymbol ms : symbolRegistry.getActiveSymbols(ex)) {
                    map.putIfAbsent(ms.symbol, Exchange.BINANCEFUT);
                }
            }
            for (Exchange ex : new Exchange[]{Exchange.GATEFUT, Exchange.GATEFUT8SUB0}) {
                for (ManagerSymbol ms : symbolRegistry.getActiveSymbols(ex)) {
                    map.putIfAbsent(ms.symbol, Exchange.GATEFUT);
                }
            }
            for (ManagerSymbol ms : symbolRegistry.getActiveSymbols(Exchange.BYBITFUT)) {
                map.putIfAbsent(ms.symbol, Exchange.BYBITFUT);
            }
        }

        // Fallback 2: per symbol – for each symbol in feed, pick best ref exchange from registry
        if (map.isEmpty() && symbolRegistry != null && symbols != null) {
            int symCount = symbols.length;
            int bf = symbolRegistry.getActiveSymbols(Exchange.BINANCEFUT).size();
            int gf = symbolRegistry.getActiveSymbols(Exchange.GATEFUT).size();
            System.out.println("[GUI] Building reference map per symbol. Symbols in feed: " + symCount +
                    ", BINANCEFUT in registry: " + bf + ", GATEFUT: " + gf);
            Exchange[] refPriority = {
                    Exchange.BINANCEFUT, Exchange.BINANCEFUT8SUBFIN, Exchange.BINANCEFUT8SUB0,
                    Exchange.GATEFUT, Exchange.GATEFUT8SUB0,
                    Exchange.BYBITFUT,
                    Exchange.GATE, Exchange.BINANCE, Exchange.KUCOIN, Exchange.BITGET, Exchange.MEXC
            };
            for (String sym : symbols) {
                if (sym == null || sym.isBlank()) continue;
                for (Exchange ex : refPriority) {
                    ManagerSymbol ms = symbolRegistry.getByInternalOrMain(ex, sym);
                    if (ms != null) {
                        Exchange refEx = (ex == Exchange.BINANCEFUT8SUBFIN || ex == Exchange.BINANCEFUT8SUB0)
                                ? Exchange.BINANCEFUT
                                : (ex == Exchange.GATEFUT8SUB0 ? Exchange.GATEFUT : ex);
                        map.put(sym, refEx);
                        break;
                    }
                }
            }
            System.out.println("[GUI] Fallback per-symbol: " + map.size() + " reference mappings.");
        }
        System.out.println("[GUI] Loaded " + map.size() + " reference mappings.");
        return map;
    }

    public void show() {
        SwingUtilities.invokeLater(() -> {
            JFrame frame = new JFrame("ARB-TEST OB Viewer (heatmap)");
            frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

            Color bg = new Color(20, 22, 28);
            Color panelBg = new Color(28, 30, 38);
            Color textFg = new Color(230, 230, 230);

            JPanel root = new JPanel(new BorderLayout());
            root.setBackground(bg);

            // --- top controls --- (two-row layout)
            JPanel topPanel = new JPanel(new BorderLayout());
            topPanel.setBackground(panelBg);
            
            // First row panel
            JPanel topRow = new JPanel(new FlowLayout(FlowLayout.LEFT));
            topRow.setBackground(panelBg);
            
            // Second row panel
            JPanel bottomRow = new JPanel(new FlowLayout(FlowLayout.LEFT));
            bottomRow.setBackground(panelBg);

            JTextField symbolFilterField = new JTextField(8);
            JComboBox<String> symbolBox = new JComboBox<>();
            configureSymbolFilter(symbolFilterField, symbolBox);
            JComboBox<Exchange> leftExBox = new JComboBox<>(exchanges);
            JComboBox<Exchange> rightExBox = new JComboBox<>(exchanges);
            JButton refreshButton = new JButton("Refresh");
            refreshButton.setFocusable(false);
            JButton refreshFundingButton = new JButton("Refresh FR");
            refreshFundingButton.setFocusable(false);
            refreshFundingButton.setEnabled(fundingService != null);
            refreshFundingButton.addActionListener(ev -> {
                if (fundingService != null) {
                    fundingService.refreshAll();
                }
            });

            // Snapshot interval + start button
            String[] snapshotIntervalLabels = { "100 ms", "200 ms", "500 ms", "1 s", "2 s", "3 s", "5 s" };
            final long[] snapshotIntervalValues = { 100L, 200L, 500L, 1000L, 2000L, 3000L, 5000L };
            JComboBox<String> snapshotIntervalBox = new JComboBox<>(snapshotIntervalLabels);
            snapshotIntervalBox.setSelectedIndex(5); // 3s default
            snapshotIntervalBox.addActionListener(ev -> {
                int idx = snapshotIntervalBox.getSelectedIndex();
                if (idx >= 0 && idx < snapshotIntervalValues.length) {
                    snapshotIntervalMillis = snapshotIntervalValues[idx];
                }
            });

            JButton startSnapshotsBtn = new JButton("Start snapshots -> DB");
            startSnapshotsBtn.setFocusable(false);
            startSnapshotsBtn.addActionListener(ev -> {
                if (snapshotService != null) {
                    snapshotService.start(snapshotIntervalMillis);
                    startSnapshotsBtn.setEnabled(false);
                } else {
                    JOptionPane.showMessageDialog(frame,
                            "Snapshot service is not configured",
                            "Error",
                            JOptionPane.ERROR_MESSAGE);
                }
            });

            JButton startDetectorBtn = new JButton("Start big gain detector");
            startDetectorBtn.setFocusable(false);
            startDetectorBtn.addActionListener(ev -> {
                if (!detectorRunning) {

                    // 1. Load Reference Map
                    Map<String, Exchange> refMap = loadReferenceMap();
                    if (refMap.isEmpty()) {
                        int confirm = JOptionPane.showConfirmDialog(frame,
                                "Reference map is empty! Detector will find nothing. Continue?",
                                "Warning", JOptionPane.YES_NO_OPTION);
                        if (confirm != JOptionPane.YES_OPTION) return;
                    }

                    detectorRunning = true;
                    if (detectorWindow == null) {
                        detectorWindow = new BigGainDetectorWindow();
                        detectorWindow.setVisible(true);
                    }

                    // 2. Start Ranking Detector
                    detectorThread = new Thread(
                            new BigGainDetector(
                                    store,
                                    symbols,
                                    exchanges,
                                    refMap,
                                    DETECTOR_WINDOW_MILLIS,
                                    DETECTOR_TOTAL_FEES_PCT,
                                    DETECTOR_TARGET_VOLUME_QUOTE,
                                    DETECTOR_TARGET_GAIN_USD,
                                    detectorWindow,
                                    arbStatsWriter
                            ),
                            "big-gain-detector"
                    );
                    detectorThread.setDaemon(true);
                    detectorThread.start();

                    // 3. Start Rolling Gain Detector
                    // Only start if we have the writer configured
                    if (rollingGainWriter != null) {
                        RollingGainDetector rollingDetector = new RollingGainDetector(
                                store,
                                exchanges,          // All exchanges (detector filters based on map)
                                refMap,
                                Arrays.asList(symbols),
                                rollingGainWriter,
                                symbolRegistry
                        );
                        rollingDetectorThread = new Thread(rollingDetector, "rolling-gain-detector");
                        rollingDetectorThread.setDaemon(true);
                        rollingDetectorThread.start();
                        System.out.println("[GUI] Started RollingGainDetector thread.");
                        detectorWindow.appendAlert("Rolling gain detector started.");
                    } else {
                        System.err.println("[GUI] Cannot start RollingGainDetector: writer is null");
                        detectorWindow.appendAlert("WARNING: Rolling gain detector NOT started (missing writer).");
                    }

                    startDetectorBtn.setEnabled(false);
                }
            });

            JCheckBox autoRefreshCheck = new JCheckBox("Auto");
            autoRefreshCheck.setBackground(panelBg);
            autoRefreshCheck.setForeground(textFg);

            JCheckBox totalPositiveCheck = new JCheckBox("Total positive");
            totalPositiveCheck.setBackground(panelBg);
            totalPositiveCheck.setForeground(textFg);

            String[] intervalLabels = { "20 ms", "50 ms", "100 ms", "200 ms", "500 ms", "1 s", "2 s" };
            final int[] intervalValues = { 20, 50, 100, 200, 500, 1000, 2000 };
            JComboBox<String> intervalBox = new JComboBox<>(intervalLabels);
            intervalBox.setSelectedIndex(3);

            JLabel volumeLabel = new JLabel("Vol(Q):");
            JTextField volumeField = new JTextField("100.0", 5);

            totalPositiveCheck.addActionListener(e -> volumeField.setEnabled(!totalPositiveCheck.isSelected()));

            // --- FEES & ORDER TYPES ---
            // Source (Left)
            JLabel srcOrderTypeLabel = new JLabel("Src:");
            JComboBox<OrderType> srcOrderTypeBox = new JComboBox<>(OrderType.values());
            srcOrderTypeBox.setSelectedItem(OrderType.MARKET); // Default Taker

            JLabel feeSourceLabel = new JLabel("Fee (B/S):");
            JTextField feeSourceField = new JTextField("—", 12);

            // Ref (Right)
            JLabel refOrderTypeLabel = new JLabel("Ref:");
            JComboBox<OrderType> refOrderTypeBox = new JComboBox<>(OrderType.values());
            refOrderTypeBox.setSelectedItem(OrderType.LIMIT); // Default Maker

            JLabel feeRefLabel = new JLabel("Fee (B/S):");
            JTextField feeRefField = new JTextField("—", 12);

            // UI Styling
            srcOrderTypeLabel.setForeground(textFg);
            refOrderTypeLabel.setForeground(textFg);
            feeSourceLabel.setForeground(textFg);
            feeRefLabel.setForeground(textFg);
            volumeLabel.setForeground(textFg);
            JLabel symbolLabel = new JLabel("Sym:");
            symbolLabel.setForeground(textFg);
            JLabel perfLabel = new JLabel("ms: -");
            perfLabel.setForeground(new Color(180, 180, 180));

            symbolBox.setBackground(bg); symbolBox.setForeground(textFg);
            leftExBox.setBackground(bg); leftExBox.setForeground(textFg);
            rightExBox.setBackground(bg); rightExBox.setForeground(textFg);
            intervalBox.setBackground(bg); intervalBox.setForeground(textFg);
            snapshotIntervalBox.setBackground(bg); snapshotIntervalBox.setForeground(textFg);

            volumeField.setBackground(bg); volumeField.setForeground(textFg);
            feeSourceField.setBackground(bg); feeSourceField.setForeground(textFg);
            feeRefField.setBackground(bg); feeRefField.setForeground(textFg);

            refreshButton.setBackground(new Color(55, 60, 70));
            refreshButton.setForeground(textFg);

            // Add components to first row
            topRow.add(symbolLabel);
            topRow.add(symbolFilterField);
            topRow.add(symbolBox);

            topRow.add(new JLabel(" | "));
            topRow.add(leftExBox);
            topRow.add(srcOrderTypeLabel);
            topRow.add(srcOrderTypeBox);
            topRow.add(feeSourceLabel);
            topRow.add(feeSourceField);

            topRow.add(new JLabel(" | "));
            topRow.add(rightExBox);
            topRow.add(refOrderTypeLabel);
            topRow.add(refOrderTypeBox);
            topRow.add(feeRefLabel);
            topRow.add(feeRefField);

            topRow.add(new JLabel(" | "));
            topRow.add(volumeLabel);
            topRow.add(volumeField);
            topRow.add(totalPositiveCheck);

            topRow.add(refreshButton);
            topRow.add(autoRefreshCheck);
            topRow.add(intervalBox);
            topRow.add(refreshFundingButton);

            // Add components to second row
            bottomRow.add(new JLabel("DB Snapshots:"));
            bottomRow.add(snapshotIntervalBox);
            bottomRow.add(startSnapshotsBtn);
            
            bottomRow.add(new JLabel(" | "));
            bottomRow.add(startDetectorBtn);
            
            bottomRow.add(new JLabel(" | "));
            bottomRow.add(perfLabel);
            
            // Combine rows
            topPanel.add(topRow, BorderLayout.NORTH);
            topPanel.add(bottomRow, BorderLayout.SOUTH);


            // --- order book heatmap panels ---
            Font obFont = new Font(Font.MONOSPACED, Font.PLAIN, 13);

            OrderBookPanel leftPanel = new OrderBookPanel("SOURCE", true);
            OrderBookPanel rightPanel = new OrderBookPanel("REF", false);
            leftPanel.setBackground(bg);
            rightPanel.setBackground(bg);
            leftPanel.setFont(obFont);
            rightPanel.setFont(obFont);

            JSplitPane split = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT, leftPanel, rightPanel);
            split.setResizeWeight(0.5);
            split.setDividerSize(2);
            split.setContinuousLayout(true);
            split.setBorder(null);

            // --- global VWAP gains (moved ABOVE orderbooks) ---
            JLabel buyGainLabel = new JLabel("BUY_GAIN: n/a");
            JLabel sellGainLabel = new JLabel("SELL_GAIN: n/a");
            buyGainLabel.setForeground(new Color(200, 200, 200));
            sellGainLabel.setForeground(new Color(200, 200, 200));
            Font gainFont = new Font(Font.MONOSPACED, Font.BOLD, 16);
            buyGainLabel.setFont(gainFont);
            sellGainLabel.setFont(gainFont);

            JPanel gainsPanel = new JPanel(new FlowLayout(FlowLayout.CENTER));
            gainsPanel.setBackground(panelBg);
            gainsPanel.add(buyGainLabel);
            gainsPanel.add(Box.createHorizontalStrut(24));
            gainsPanel.add(sellGainLabel);

            JPanel northWrapper = new JPanel(new BorderLayout());
            northWrapper.setBackground(panelBg);
            northWrapper.add(topPanel, BorderLayout.NORTH);
            northWrapper.add(gainsPanel, BorderLayout.SOUTH);

            root.add(northWrapper, BorderLayout.NORTH);
            root.add(split, BorderLayout.CENTER);

            frame.setContentPane(root);
            frame.setSize(1600, 900);
            frame.setLocationRelativeTo(null);
            frame.setVisible(true);

            SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            fmt.setTimeZone(TimeZone.getTimeZone("UTC"));

            // Auto-update fees logic – show B (buy) and S (sell) from manager.symbol
            Runnable updateFeesFromRegistry = () -> {
                if (symbolRegistry == null) {
                    feeSourceField.setText("—");
                    feeRefField.setText("—");
                    return;
                }

                String symbol = (String) symbolBox.getSelectedItem();
                Exchange leftEx = (Exchange) leftExBox.getSelectedItem();
                Exchange rightEx = (Exchange) rightExBox.getSelectedItem();

                if (symbol == null || leftEx == null || rightEx == null) return;

                boolean leftMarket = ((OrderType) srcOrderTypeBox.getSelectedItem()) == OrderType.MARKET;
                boolean rightMarket = ((OrderType) refOrderTypeBox.getSelectedItem()) == OrderType.MARKET;

                ManagerSymbol msSrc = symbolRegistry.getByInternalOrMain(leftEx, symbol);
                if (msSrc != null) {
                    BigDecimal buy = leftMarket ? msSrc.feeMarketBuy : msSrc.feeLimitBuy;
                    BigDecimal sell = leftMarket ? msSrc.feeMarketSell : msSrc.feeLimitSell;
                    feeSourceField.setText(fmtFeePair(buy, sell));
                } else {
                    feeSourceField.setText("— (no DB)");
                }

                ManagerSymbol msRef = symbolRegistry.getByInternalOrMain(rightEx, symbol);
                if (msRef != null) {
                    BigDecimal buy = rightMarket ? msRef.feeMarketBuy : msRef.feeLimitBuy;
                    BigDecimal sell = rightMarket ? msRef.feeMarketSell : msRef.feeLimitSell;
                    feeRefField.setText(fmtFeePair(buy, sell));
                } else {
                    feeRefField.setText("— (no DB)");
                }
            };

            // Hook listeners for fee updates
            symbolBox.addActionListener(e -> updateFeesFromRegistry.run());
            leftExBox.addActionListener(e -> updateFeesFromRegistry.run());
            rightExBox.addActionListener(e -> updateFeesFromRegistry.run());
            srcOrderTypeBox.addActionListener(e -> updateFeesFromRegistry.run());
            refOrderTypeBox.addActionListener(e -> updateFeesFromRegistry.run());


            Runnable doRefresh = () -> {
                long startNs = System.nanoTime();

                String symbol = (String) symbolBox.getSelectedItem();
                Exchange leftEx = (Exchange) leftExBox.getSelectedItem();
                Exchange rightEx = (Exchange) rightExBox.getSelectedItem();
                if (symbol == null || leftEx == null || rightEx == null) {
                    return;
                }

                OrderBookSnapshot leftSnap = store.get(leftEx, symbol);
                OrderBookSnapshot rightSnap = store.get(rightEx, symbol);

                boolean useTotalPositive = totalPositiveCheck.isSelected();
                double quoteVolume = 0.0;
                if (!useTotalPositive) {
                    try {
                        quoteVolume = Double.parseDouble(volumeField.getText().trim());
                    } catch (NumberFormatException e) {
                        buyGainLabel.setText("BUY_GAIN: invalid volume");
                        buyGainLabel.setForeground(Color.ORANGE);
                        sellGainLabel.setText("SELL_GAIN: invalid volume");
                        sellGainLabel.setForeground(Color.ORANGE);
                        double durMs = (System.nanoTime() - startNs) / 1_000_000.0;
                        perfLabel.setText(String.format("refresh: %.2f ms", durMs));
                        return;
                    }
                }

                // Resolve fees from manager.symbol (4 values: left B/S, right B/S)
                boolean leftMarket = ((OrderType) srcOrderTypeBox.getSelectedItem()) == OrderType.MARKET;
                boolean rightMarket = ((OrderType) refOrderTypeBox.getSelectedItem()) == OrderType.MARKET;
                double leftBuy, leftSell, rightBuy, rightSell;
                if (symbolRegistry != null) {
                    ManagerSymbol msLeft = symbolRegistry.getByInternalOrMain(leftEx, symbol);
                    ManagerSymbol msRight = symbolRegistry.getByInternalOrMain(rightEx, symbol);
                    leftBuy = toFee(msLeft != null ? (leftMarket ? msLeft.feeMarketBuy : msLeft.feeLimitBuy) : null);
                    leftSell = toFee(msLeft != null ? (leftMarket ? msLeft.feeMarketSell : msLeft.feeLimitSell) : null);
                    rightBuy = toFee(msRight != null ? (rightMarket ? msRight.feeMarketBuy : msRight.feeLimitBuy) : null);
                    rightSell = toFee(msRight != null ? (rightMarket ? msRight.feeMarketSell : msRight.feeLimitSell) : null);
                } else {
                    leftBuy = leftSell = rightBuy = rightSell = 0.0004;
                }

                leftPanel.setSnapshot(leftEx, symbol, leftSnap, rightSnap, fmt, leftBuy, leftSell, rightBuy, rightSell);
                rightPanel.setSnapshot(rightEx, symbol, rightSnap, leftSnap, fmt, rightBuy, rightSell, leftBuy, leftSell);

                updateGains(symbol, quoteVolume, leftBuy, leftSell, rightBuy, rightSell,
                        leftEx, rightEx,
                        leftSnap, rightSnap,
                        buyGainLabel, sellGainLabel,
                        useTotalPositive);

                double durMs = (System.nanoTime() - startNs) / 1_000_000.0;

                long now = System.currentTimeMillis();
                String lagPart = "";
                if (leftSnap != null && leftSnap.getEventTimeMillis() > 0) {
                    long lagLeft = now - leftSnap.getEventTimeMillis();
                    if (lagLeft >= 0 && lagLeft < 600_000) {
                        lagPart += " | lagL=" + lagLeft + " ms";
                    }
                }
                if (rightSnap != null && rightSnap.getEventTimeMillis() > 0) {
                    long lagRight = now - rightSnap.getEventTimeMillis();
                    if (lagRight >= 0 && lagRight < 600_000) {
                        lagPart += " | lagR=" + lagRight + " ms";
                    }
                }
                perfLabel.setText(String.format("ms: %.1f", durMs) + lagPart);
            };

            refreshButton.addActionListener(e -> doRefresh.run());
            volumeField.addActionListener(e -> doRefresh.run());
            feeSourceField.addActionListener(e -> doRefresh.run());
            feeRefField.addActionListener(e -> doRefresh.run());

            final Timer[] timerHolder = new Timer[1];

            autoRefreshCheck.addActionListener(e -> {
                if (autoRefreshCheck.isSelected()) {
                    int idx = intervalBox.getSelectedIndex();
                    if (idx < 0) idx = 3;
                    int delay = intervalValues[idx];
                    Timer t = new Timer(delay, ev -> doRefresh.run());
                    t.start();
                    timerHolder[0] = t;
                } else {
                    if (timerHolder[0] != null) {
                        timerHolder[0].stop();
                        timerHolder[0] = null;
                    }
                }
            });

            intervalBox.addActionListener(e -> {
                if (timerHolder[0] != null) {
                    int idx = intervalBox.getSelectedIndex();
                    if (idx < 0) idx = 3;
                    int delay = intervalValues[idx];
                    timerHolder[0].setDelay(delay);
                }
            });

            updateFeesFromRegistry.run();
            doRefresh.run();
        });
    }

    private static class VwapResult {
        final double vwapPrice;
        final double baseVolume;
        final double quoteVolume;

        VwapResult(double vwapPrice, double baseVolume, double quoteVolume) {
            this.vwapPrice = vwapPrice;
            this.baseVolume = baseVolume;
            this.quoteVolume = quoteVolume;
        }
    }

    private void configureSymbolFilter(JTextField filterField, JComboBox<String> symbolBox) {
        symbolBox.setMaximumRowCount(20);
        symbolBox.setFocusable(true);

        final String[] allSymbols = symbols.clone();

        DefaultComboBoxModel<String> initial = new DefaultComboBoxModel<>(allSymbols);
        symbolBox.setModel(initial);
        if (initial.getSize() > 0) {
            symbolBox.setSelectedIndex(0);
        }

        filterField.getDocument().addDocumentListener(new DocumentListener() {
            private void refilter() {
                SwingUtilities.invokeLater(() -> {
                    String text = filterField.getText();
                    String lower = text == null ? "" : text.toLowerCase();

                    DefaultComboBoxModel<String> model = new DefaultComboBoxModel<>();
                    for (String s : allSymbols) {
                        if (lower.isEmpty() || s.toLowerCase().contains(lower)) {
                            model.addElement(s);
                        }
                    }
                    symbolBox.setModel(model);
                    if (model.getSize() > 0) {
                        symbolBox.setSelectedIndex(0);
                    }
                });
            }

            @Override
            public void insertUpdate(DocumentEvent e) {
                refilter();
            }

            @Override
            public void removeUpdate(DocumentEvent e) {
                refilter();
            }

            @Override
            public void changedUpdate(DocumentEvent e) {
                refilter();
            }
        });
    }

    private VwapResult computeVwapForQuote(java.util.List<OrderBookLevel> levels, double targetQuoteVolume) {
        if (levels == null || levels.isEmpty() || targetQuoteVolume <= 0.0) {
            return new VwapResult(Double.NaN, 0.0, 0.0);
        }
        double remainingQuote = targetQuoteVolume;
        double sumQuote = 0.0;
        double sumBase = 0.0;

        for (OrderBookLevel lvl : levels) {
            double price = lvl.getPrice();
            double amount = lvl.getSize();
            if (price <= 0.0 || amount <= 0.0) {
                continue;
            }

            double levelQuote = price * amount;
            if (levelQuote <= remainingQuote) {
                sumQuote += levelQuote;
                sumBase += amount;
                remainingQuote -= levelQuote;
            } else {
                double takeBase = remainingQuote / price;
                sumQuote += remainingQuote;
                sumBase += takeBase;
                remainingQuote = 0.0;
                break;
            }
        }

        if (sumBase <= 0.0) {
            return new VwapResult(Double.NaN, 0.0, 0.0);
        }
        double vwap = sumQuote / sumBase;
        return new VwapResult(vwap, sumBase, sumQuote);
    }

    private void updateGains(String symbol,
                             double quoteVolume,
                             double leftBuy, double leftSell,
                             double rightBuy, double rightSell,
                             Exchange leftEx, Exchange rightEx,
                             OrderBookSnapshot leftSnap, OrderBookSnapshot rightSnap,
                             JLabel buyGainLabel, JLabel sellGainLabel,
                             boolean useTotalPositive) {

        if (leftSnap == null || rightSnap == null) {
            buyGainLabel.setText("BUY_GAIN: n/a (missing book)");
            buyGainLabel.setForeground(Color.LIGHT_GRAY);
            sellGainLabel.setText("SELL_GAIN: n/a (missing book)");
            sellGainLabel.setForeground(Color.LIGHT_GRAY);
            return;
        }

        List<OrderBookLevel> leftAsks = leftSnap.getAsks();
        List<OrderBookLevel> leftBids = leftSnap.getBids();
        List<OrderBookLevel> rightAsks = rightSnap.getAsks();
        List<OrderBookLevel> rightBids = rightSnap.getBids();

        if (leftAsks.isEmpty() || leftBids.isEmpty() || rightAsks.isEmpty() || rightBids.isEmpty()) {
            buyGainLabel.setText("BUY_GAIN: n/a (empty side)");
            buyGainLabel.setForeground(Color.LIGHT_GRAY);
            sellGainLabel.setText("SELL_GAIN: n/a (empty side)");
            sellGainLabel.setForeground(Color.LIGHT_GRAY);
            return;
        }

        double buyGainAbs;
        double buyGainPct;
        double buyVolQuote = Double.NaN;
        double sellGainAbs;
        double sellGainPct;
        double sellVolQuote = Double.NaN;

        if (!useTotalPositive) {
            VwapResult buyVwap = computeVwapForQuote(leftAsks, quoteVolume);
            buyGainAbs = Double.NaN;
            buyGainPct = Double.NaN;
            if (!Double.isNaN(buyVwap.vwapPrice) && !rightBids.isEmpty()) {
                double hedgePrice = rightBids.get(0).getPrice();

                double effectiveBuyPrice = buyVwap.vwapPrice * (1.0 + leftBuy);
                double effectiveSellPrice = hedgePrice * (1.0 - rightSell);

                double perUnit = effectiveSellPrice - effectiveBuyPrice;
                buyGainAbs = perUnit * buyVwap.baseVolume;
                buyGainPct = perUnit / effectiveBuyPrice * 100.0;
            }

            VwapResult sellVwap = computeVwapForQuote(leftBids, quoteVolume);
            sellGainAbs = Double.NaN;
            sellGainPct = Double.NaN;
            if (!Double.isNaN(sellVwap.vwapPrice) && !rightAsks.isEmpty()) {
                double hedgePrice = rightAsks.get(0).getPrice();

                double effectiveSellPrice = sellVwap.vwapPrice * (1.0 - leftSell);
                double effectiveBuyPrice = hedgePrice * (1.0 + rightBuy);

                double perUnit = effectiveSellPrice - effectiveBuyPrice;
                sellGainAbs = perUnit * sellVwap.baseVolume;
                sellGainPct = perUnit / effectiveSellPrice * 100.0;
            }
        } else {
            double[] buyTotal = computeTotalPositiveGains(leftAsks, rightBids, true, leftBuy, rightSell);
            buyGainAbs = buyTotal[0];
            buyGainPct = buyTotal[1];
            buyVolQuote = buyTotal[2];

            double[] sellTotal = computeTotalPositiveGains(leftBids, rightAsks, false, leftSell, rightBuy);
            sellGainAbs = sellTotal[0];
            sellGainPct = sellTotal[1];
            sellVolQuote = sellTotal[2];
        }

        String buyTitle = useTotalPositive ? "BUY_TOTAL_POS" : "BUY_GAIN";
        String sellTitle = useTotalPositive ? "SELL_TOTAL_POS" : "SELL_GAIN";

        updateGainLabel(buyGainLabel, buyTitle, buyGainAbs, buyGainPct, buyVolQuote);
        updateGainLabel(sellGainLabel, sellTitle, sellGainAbs, sellGainPct, sellVolQuote);
    }

    private String formatFundingHeader(Exchange exchange, String symbol) {
        if (fundingStore == null || exchange == null || symbol == null) return "";
        if (exchange != Exchange.BINANCEFUT && exchange != Exchange.BYBITFUT && exchange != Exchange.GATEFUT) {
            return "";
        }
        FundingInfo info = fundingStore.get(exchange, symbol);
        if (info == null) return "";
        double rate = info.getFundingRate();
        if (Double.isNaN(rate)) return "";
        double ratePct = rate * 100.0;
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("FR=%.6f (%.3f%%)", rate, ratePct));
        Integer interval = info.getFundingIntervalHours();
        if (interval != null && interval > 0) {
            sb.append(String.format(" | int=%dh", interval));
        }
        Long nextTs = info.getNextFundingTimeMillis();
        if (nextTs != null && nextTs > 0) {
            SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            fmt.setTimeZone(TimeZone.getTimeZone("UTC"));
            sb.append(" | next=").append(fmt.format(new Date(nextTs))).append(" UTC");
        }
        return sb.toString();
    }


    private double[] computeTotalPositiveGains(
            java.util.List<OrderBookLevel> srcLevels,
            java.util.List<OrderBookLevel> refLevels,
            boolean isAskSide,
            double feeSource,
            double feeRef
    ) {
        if (srcLevels == null || refLevels == null || srcLevels.isEmpty() || refLevels.isEmpty()) {
            return new double[] { Double.NaN, Double.NaN, Double.NaN };
        }

        int i = 0;
        int j = 0;

        double remainingSrcQty = srcLevels.get(0).getSize();
        double remainingRefQty = refLevels.get(0).getSize();

        double totalPositiveGainUsd = 0.0;
        double totalVolumeQuote = 0.0;

        while (i < srcLevels.size() && j < refLevels.size()) {
            if (remainingSrcQty <= 0.0) {
                i++;
                if (i >= srcLevels.size()) {
                    break;
                }
                remainingSrcQty = srcLevels.get(i).getSize();
                continue;
            }
            if (remainingRefQty <= 0.0) {
                j++;
                if (j >= refLevels.size()) {
                    break;
                }
                remainingRefQty = refLevels.get(j).getSize();
                continue;
            }

            OrderBookLevel srcLevel = srcLevels.get(i);
            OrderBookLevel refLevel = refLevels.get(j);

            double srcPrice = srcLevel.getPrice();
            double refPrice = refLevel.getPrice();
            if (srcPrice <= 0.0 || refPrice <= 0.0) {
                break;
            }

            double effSrc;
            double effRef;
            double costPrice;
            double proceedsPrice;

            if (isAskSide) {
                // BUY on src asks, SELL on ref bids
                effSrc = srcPrice * (1.0 + feeSource);
                effRef = refPrice * (1.0 - feeRef);
                costPrice = effSrc;
                proceedsPrice = effRef;
            } else {
                // SELL on src bids, BUY on ref asks
                effSrc = srcPrice * (1.0 - feeSource);
                effRef = refPrice * (1.0 + feeRef);
                costPrice = effRef;
                proceedsPrice = effSrc;
            }

            double perUnit = proceedsPrice - costPrice;
            if (perUnit <= 0.0) {
                break;
            }

            double tradableQty = Math.min(remainingSrcQty, remainingRefQty);
            if (tradableQty <= 0.0) {
                break;
            }

            double levelCostQuote = costPrice * tradableQty;
            double levelGainUsd = perUnit * tradableQty;

            totalPositiveGainUsd += levelGainUsd;
            totalVolumeQuote += levelCostQuote;

            remainingSrcQty -= tradableQty;
            remainingRefQty -= tradableQty;

            if (remainingSrcQty <= 0.0) {
                i++;
                if (i < srcLevels.size()) {
                    remainingSrcQty = srcLevels.get(i).getSize();
                }
            }
            if (remainingRefQty <= 0.0) {
                j++;
                if (j < refLevels.size()) {
                    remainingRefQty = refLevels.get(j).getSize();
                }
            }
        }

        if (totalVolumeQuote <= 0.0) {
            return new double[] { Double.NaN, Double.NaN, Double.NaN };
        }

        double avgGainPct = (totalPositiveGainUsd / totalVolumeQuote) * 100.0;
        return new double[] { totalPositiveGainUsd, avgGainPct, totalVolumeQuote };
    }


    private void updateGainLabel(JLabel label, String title, double gainAbs, double gainPct, double volumeQuote) {
        if (Double.isNaN(gainAbs) || Double.isNaN(gainPct)) {
            label.setText(title + ": n/a");
            label.setForeground(Color.LIGHT_GRAY);
            return;
        }
        String color;
        if (gainAbs > 0) {
            color = "#00FF00";
        } else if (gainAbs < 0) {
            color = "#FF0000";
        } else {
            color = "#FFFFFF";
        }

        String txt;
        if (Double.isNaN(volumeQuote)) {
            txt = String.format(
                    "<html>%s: <span style='color:%s'>%+.4f USDT (%.3f%%)</span></html>",
                    title, color, gainAbs, gainPct
            );
        } else {
            txt = String.format(
                    "<html>%s: <span style='color:%s'>%+.4f USDT (%.3f%%) (vol: %.2f)</span></html>",
                    title, color, gainAbs, gainPct, volumeQuote
            );
        }

        label.setText(txt);
        label.setForeground(new Color(230, 230, 230));
    }

    private class OrderBookPanel extends JComponent {
        private final String title;
        private final boolean isSource;
        private Exchange exchange;
        private String symbol;
        private OrderBookSnapshot snapshot;
        private OrderBookSnapshot refSnapshot;
        private String eventTimeStr = "";
        private double feeBuy = Double.NaN;
        private double feeSell = Double.NaN;
        private double refFeeBuy = Double.NaN;
        private double refFeeSell = Double.NaN;

        OrderBookPanel(String title, boolean isSource) {
            this.title = title;
            this.isSource = isSource;
            setOpaque(true);
        }

        /** leftBuy/Sell = this panel's fees, rightBuy/Sell = ref panel's fees (when this is source). */
        void setSnapshot(Exchange ex,
                         String symbol,
                         OrderBookSnapshot snap,
                         OrderBookSnapshot refSnap,
                         SimpleDateFormat fmt,
                         double thisBuy, double thisSell,
                         double refBuy, double refSell) {
            this.exchange = ex;
            this.symbol = symbol;
            this.snapshot = snap;
            this.refSnapshot = refSnap;
            this.feeBuy = thisBuy;
            this.feeSell = thisSell;
            this.refFeeBuy = refBuy;
            this.refFeeSell = refSell;
            if (snap != null && snap.getEventTimeMillis() > 0) {
                this.eventTimeStr = fmt.format(new Date(snap.getEventTimeMillis())) + " UTC";
            } else {
                this.eventTimeStr = "eventTime: n/a";
            }
            repaint();
        }

        @Override
        protected void paintComponent(Graphics g) {
            super.paintComponent(g);
            Graphics2D g2 = (Graphics2D) g.create();
            g2.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);

            int w = getWidth();
            int h = getHeight();

            Color bg = new Color(20, 22, 28);
            g2.setColor(bg);
            g2.fillRect(0, 0, w, h);

            if (snapshot == null) {
                g2.setColor(new Color(130, 130, 130));
                String txt = title + " : snapshot = null";
                FontMetrics fm0 = g2.getFontMetrics();
                int x = 8;
                int y = 20 + fm0.getAscent();
                g2.drawString(txt, x, y);
                g2.dispose();
                return;
            }

            Font baseFont = g2.getFont();
            Font monoFont = new Font(Font.MONOSPACED, baseFont.getStyle(), baseFont.getSize() + 2);
            g2.setFont(monoFont);
            FontMetrics fm = g2.getFontMetrics();
            int lineH = fm.getHeight() + 2;
            int pad = 8;

            int midX = w / 2;

            g2.setColor(new Color(200, 200, 200));
            String fundingHdr = formatFundingHeader(exchange, symbol);
            String hdr1 = fundingHdr.isEmpty()
                    ? String.format("%s | %s | %s", title, exchange, symbol)
                    : String.format("%s | %s | %s | %s", title, exchange, symbol, fundingHdr);
            g2.drawString(hdr1, pad, pad + fm.getAscent());
            if (!eventTimeStr.isEmpty()) {
                g2.setColor(new Color(150, 150, 150));
                g2.drawString(eventTimeStr, pad, pad + lineH + fm.getAscent());
            }

            int headerLines = 2;
            int topY = pad + headerLines * lineH;

            g2.setColor(new Color(160, 160, 160));
            String bidHdr;
            String askHdr;
            if (isSource) {
                bidHdr = "BID:    price        size        vol         gain($,%)";
                askHdr = "ASK:    price        size        vol         gain($,%)";
            } else {
                bidHdr = "BID:    price        size        vol";
                askHdr = "ASK:    price        size        vol";
            }
            int hdrY = topY + fm.getAscent();

            int bidHdrX = midX - pad - fm.stringWidth(bidHdr);
            int askHdrX = midX + pad;

            g2.drawString(bidHdr, bidHdrX, hdrY);
            g2.drawString(askHdr, askHdrX, hdrY);

            int rowsStartY = topY + lineH;

            java.util.List<OrderBookLevel> bids = new java.util.ArrayList<>(snapshot.getBids());
            java.util.List<OrderBookLevel> asks = new java.util.ArrayList<>(snapshot.getAsks());
            bids.sort(java.util.Comparator.comparingDouble(OrderBookLevel::getPrice).reversed());
            asks.sort(java.util.Comparator.comparingDouble(OrderBookLevel::getPrice));

            int maxRows = Math.min(DISPLAY_DEPTH, Math.max(bids.size(), asks.size()));

            double maxBidVol = 0.0;
            double maxAskVol = 0.0;
            for (int i = 0; i < maxRows; i++) {
                if (i < bids.size()) {
                    OrderBookLevel b = bids.get(i);
                    maxBidVol = Math.max(maxBidVol, b.getPrice() * b.getSize());
                }
                if (i < asks.size()) {
                    OrderBookLevel a = asks.get(i);
                    maxAskVol = Math.max(maxAskVol, a.getPrice() * a.getSize());
                }
            }
            if (maxBidVol <= 0.0) maxBidVol = 1.0;
            if (maxAskVol <= 0.0) maxAskVol = 1.0;

            int maxBarWidthBid = midX - 2 * pad;
            int maxBarWidthAsk = midX - 2 * pad;

            Color bidBar = new Color(0, 140, 90, 120);
            Color askBar = new Color(190, 80, 80, 120);
            Color textColor = new Color(230, 230, 230);

            double refBestBid = Double.NaN;
            double refBestAsk = Double.NaN;
            if (isSource && refSnapshot != null) {
                java.util.List<OrderBookLevel> refBids = refSnapshot.getBids();
                java.util.List<OrderBookLevel> refAsks = refSnapshot.getAsks();
                if (refBids != null && !refBids.isEmpty()) {
                    refBestBid = refBids.get(0).getPrice();
                }
                if (refAsks != null && !refAsks.isEmpty()) {
                    refBestAsk = refAsks.get(0).getPrice();
                }
            }

            for (int i = 0; i < maxRows; i++) {
                int rowYTop = rowsStartY + i * lineH;
                int baselineY = rowYTop + fm.getAscent();

                if (i < bids.size()) {
                    OrderBookLevel b = bids.get(i);
                    double price = b.getPrice();
                    double amount = b.getSize();
                    double vol = price * amount;
                    int barW = (int) (maxBarWidthBid * (vol / maxBidVol));
                    if (barW > 0) {
                        int barX = pad;
                        g2.setColor(bidBar);
                        g2.fillRect(barX, rowYTop, barW, lineH);
                    }

                    String gainStr = "";
                    if (isSource && !Double.isNaN(feeSell) && !Double.isNaN(refFeeBuy)
                            && !Double.isNaN(refBestAsk) && amount > 0 && price > 0) {
                        double effectiveSell = price * (1.0 - feeSell);
                        double effectiveBuy = refBestAsk * (1.0 + refFeeBuy);
                        double perUnit = effectiveSell - effectiveBuy;
                        double gainAbs = perUnit * amount;
                        double denom = effectiveSell;
                        double gainPct = denom != 0.0 ? (perUnit / denom * 100.0) : Double.NaN;
                        gainStr = String.format("%+10.4f (%.2f%%)", gainAbs, gainPct);
                    }

                    String pStr = formatPrice(price);
                    String baseTxt;
                    if (isSource) {
                        baseTxt = String.format("%10s  %10.4f  %10.2f  %s",
                                pStr, amount, vol, gainStr);
                    } else {
                        baseTxt = String.format("%10s  %10.4f  %10.2f",
                                pStr, amount, vol);
                    }

                    int tx = midX - pad - fm.stringWidth(baseTxt);
                    g2.setColor(textColor);
                    g2.drawString(baseTxt, tx, baselineY);
                }

                if (i < asks.size()) {
                    OrderBookLevel a = asks.get(i);
                    double price = a.getPrice();
                    double amount = a.getSize();
                    double vol = price * amount;
                    int barW = (int) (maxBarWidthAsk * (vol / maxAskVol));
                    if (barW > 0) {
                        int barX = w - pad - barW;
                        g2.setColor(askBar);
                        g2.fillRect(barX, rowYTop, barW, lineH);
                    }

                    String gainStr = "";
                    if (isSource && !Double.isNaN(feeBuy) && !Double.isNaN(refFeeSell)
                            && !Double.isNaN(refBestBid) && amount > 0 && price > 0) {
                        double effectiveBuy = price * (1.0 + feeBuy);
                        double effectiveSell = refBestBid * (1.0 - refFeeSell);
                        double perUnit = effectiveSell - effectiveBuy;
                        double gainAbs = perUnit * amount;
                        double denom = effectiveBuy;
                        double gainPct = denom != 0.0 ? (perUnit / denom * 100.0) : Double.NaN;
                        gainStr = String.format("%+10.4f (%.2f%%)", gainAbs, gainPct);
                    }

                    String pStr = formatPrice(price);
                    String baseTxt;
                    if (isSource) {
                        baseTxt = String.format("%10s  %10.4f  %10.2f  %s",
                                pStr, amount, vol, gainStr);
                    } else {
                        baseTxt = String.format("%10s  %10.4f  %10.2f",
                                pStr, amount, vol);
                    }

                    int tx = midX + pad;
                    g2.setColor(textColor);
                    g2.drawString(baseTxt, tx, baselineY);
                }
            }

            g2.dispose();
        }
    }

    private static String formatPrice(double price) {
        if (Double.isNaN(price) || Double.isInfinite(price)) return "NaN";
        double ap = Math.abs(price);
        int dp;
        if (ap >= 1000) dp = 2;
        else if (ap >= 1) dp = 4;
        else if (ap >= 0.01) dp = 6;
        else if (ap >= 0.0001) dp = 8;
        else dp = 10;
        return String.format("%." + dp + "f", price);
    }

    private class BigGainDetector implements Runnable {

        private static final int TOP_N = 20;

        private final OrderBookStore store;
        private final String[] symbols;
        private final Exchange[] allExchanges;
        private final Map<String, Exchange> refExchangeMap;
        private final long windowMillis;
        private final double totalFeesPct;
        private final double targetVolumeQuote;
        private final double targetGainUsd;
        private final BigGainDetectorWindow window;
        private final MarketDataArbStatsWriter arbStatsWriter;

        BigGainDetector(OrderBookStore store,
                        String[] symbols,
                        Exchange[] allExchanges,
                        Map<String, Exchange> refExchangeMap,
                        long windowMillis,
                        double totalFeesPct,
                        double targetVolumeQuote,
                        double targetGainUsd,
                        BigGainDetectorWindow window,
                        MarketDataArbStatsWriter arbStatsWriter) {
            this.store = store;
            this.symbols = symbols;
            this.allExchanges = allExchanges;
            this.refExchangeMap = refExchangeMap;
            this.windowMillis = windowMillis;
            this.totalFeesPct = totalFeesPct;
            this.targetVolumeQuote = targetVolumeQuote;
            this.targetGainUsd = targetGainUsd;
            this.window = window;
            this.arbStatsWriter = arbStatsWriter;
        }

        @Override
        public void run() {
            while (detectorRunning) {
                long now = System.currentTimeMillis();
                try {
                    java.util.List<Exchange> sourceExchanges = new ArrayList<>();
                    // Note: We add all exchanges here. The filtering happens inside Scanner
                    // because now "Ref" depends on the symbol.
                    for (Exchange ex : allExchanges) {
                        sourceExchanges.add(ex);
                    }

                    java.util.List<String> symbolList = Arrays.asList(symbols);

                    java.util.List<BigGainSnapshot> snapshots = BigGainScanner.scanWindow(
                            store,
                            sourceExchanges,
                            refExchangeMap,
                            symbolList,
                            windowMillis,
                            totalFeesPct,
                            targetVolumeQuote,
                            targetGainUsd,
                            symbolRegistry
                    );

                    int total = snapshots.size();

                    java.util.List<BigGainSnapshot> positive = new ArrayList<>();
                    for (BigGainSnapshot s : snapshots) {
                        if (s.getTotalPositiveGainUsdLatest() > 0.0) {
                            positive.add(s);
                        }
                    }

                    java.util.List<BigGainSnapshot> rank1 = new ArrayList<>(positive);
                    rank1.sort(Comparator.comparingDouble(BigGainSnapshot::getTotalPositiveGainUsdLatest).reversed());

                    java.util.List<BigGainSnapshot> rank2 = new ArrayList<>();
                    for (BigGainSnapshot s : positive) {
                        if (s.getFirstLevelGainUsd() > 0.0) {
                            rank2.add(s);
                        }
                    }
                    rank2.sort(Comparator.comparingDouble(BigGainSnapshot::getFirstLevelGainUsd).reversed());

                    java.util.List<BigGainSnapshot> rank3 = new ArrayList<>();
                    for (BigGainSnapshot s : positive) {
                        double vwap = s.getVwapGainPctForTargetVolume();
                        if (!Double.isNaN(vwap) && vwap > 0.0) {
                            rank3.add(s);
                        }
                    }
                    rank3.sort(Comparator.comparingDouble(BigGainSnapshot::getVwapGainPctForTargetVolume).reversed());

                    java.util.List<BigGainSnapshot> rank4 = new ArrayList<>();
                    for (BigGainSnapshot s : positive) {
                        double volToTarget = s.getVolumeQuoteToReachTargetGain();
                        if (!Double.isNaN(volToTarget) && volToTarget > 0.0) {
                            rank4.add(s);
                        }
                    }
                    rank4.sort(Comparator.comparingDouble(BigGainSnapshot::getVolumeQuoteToReachTargetGain));

                    java.util.List<ArbStatRecord> records = new ArrayList<>();

                    int shownRank1 = logRanking("best_arb_occasions_snapshot", (short) 1, rank1, total, now, records);
                    logRanking("best_first_line_arb", (short) 2, rank2, total, now, records);
                    logRanking("best_x_dollars_volume_arb", (short) 3, rank3, total, now, records);
                    logRanking("best_x_dollars_gain_arb", (short) 4, rank4, total, now, records);

                    window.setLastScan(now, total, shownRank1);

                    if (arbStatsWriter != null && !records.isEmpty()) {
                        try {
                            arbStatsWriter.insertBatch(records);
                        } catch (Exception dbEx) {
                            window.appendAlert("DB write error (arb stats): " + dbEx.getMessage());
                        }
                    }

                    Thread.sleep(windowMillis);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception ex) {
                    window.appendAlert("Detector error: " + ex.getMessage());
                    ex.printStackTrace();
                    try {
                        Thread.sleep(windowMillis);
                    } catch (InterruptedException ignored) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }

        private int logRanking(String title, short rankType, java.util.List<BigGainSnapshot> list, int total, long tsMillis, java.util.List<ArbStatRecord> outRecords) {
            int limit = Math.min(TOP_N, list.size());
            if (limit <= 0) {
                return 0;
            }

            String tsStr = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss 'UTC'") {{
                setTimeZone(TimeZone.getTimeZone("UTC"));
            }}.format(new Date(tsMillis));

            window.appendAlert(String.format(
                    "=== %s | %s | top %d of %d combinations ===",
                    tsStr,
                    title,
                    limit,
                    total
            ));

            for (int i = 0; i < limit; i++) {
                BigGainSnapshot s = list.get(i);

                Exchange specificRef = refExchangeMap.get(s.getSymbol());
                String refName = (specificRef != null) ? specificRef.name() : "UNKNOWN";

                String line;
                if (rankType == 1) {
                    // Ranking 1: Sortowane po TOTAL_GAIN_$ (z całej książki)
                    line = String.format(
                            "#%02d SYMBOL: %s, EX: %s, %s, ref: %s, 1st_gain: %.2f%%, >> TOTAL_GAIN_$: %.4f <<, tot_vol: %.2f, avg_gain: %.2f%%",
                            i + 1,
                            s.getSymbol(),
                            s.getExchange().name(),
                            s.getSide(),
                            refName,
                            s.getFirstLevelGainPctLatest(),
                            s.getTotalPositiveGainUsdLatest(),  // <--- SORT KEY
                            s.getTotalVolumeQuoteLatest(),
                            s.getAvgGainPctTotalLatest()
                    );
                } else if (rankType == 2) {
                    // Ranking 2: Sortowane po 1ST_LEVEL_GAIN_$
                    line = String.format(
                            "#%02d SYMBOL: %s, EX: %s, %s, ref: %s, 1st_gain: %.2f%%, >> 1ST_LEVEL_GAIN_$: %.4f <<, 1st_vol: %.2f",
                            i + 1,
                            s.getSymbol(),
                            s.getExchange().name(),
                            s.getSide(),
                            refName,
                            s.getFirstLevelGainPctLatest(),
                            s.getFirstLevelGainUsd(),           // <--- SORT KEY
                            s.getFirstLevelVolumeQuote()
                    );
                } else if (rankType == 3) {
                    // Ranking 3: Sortowane po VWAP_GAIN_% (dla zadanego wolumenu)
                    // + Poprawka: wyświetlanie konkretnego zysku $ dla tego wolumenu
                    double specificGainUsd = s.getTargetVolumeQuote() * (s.getVwapGainPctForTargetVolume() / 100.0);

                    line = String.format(
                            "#%02d SYMBOL: %s, EX: %s, %s, ref: %s, >> VWAP_GAIN_%%_VOL_%.0f: %.4f%% <<, total_$_gain_for_%.0f_vol: %.4f",
                            i + 1,
                            s.getSymbol(),
                            s.getExchange().name(),
                            s.getSide(),
                            refName,
                            s.getTargetVolumeQuote(),           // np. 5000
                            s.getVwapGainPctForTargetVolume(),  // <--- SORT KEY
                            s.getTargetVolumeQuote(),           // np. 5000
                            specificGainUsd                     // Wyliczony zysk $
                    );
                } else {
                    // Ranking 4: Sortowane po VOLUME_TO_TARGET_GAIN (rosnąco - im mniej kapitału potrzeba by zarobić X$, tym lepiej)
                    line = String.format(
                            "#%02d SYMBOL: %s, EX: %s, %s, ref: %s, gain_%%@target_$%.0f: %.4f%%, >> VOL_TO_TARGET_GAIN: %.4f <<",
                            i + 1,
                            s.getSymbol(),
                            s.getExchange().name(),
                            s.getSide(),
                            refName,
                            s.getTargetGainUsd(),
                            s.getGainPctAtTargetGain(),
                            s.getVolumeQuoteToReachTargetGain() // <--- SORT KEY
                    );
                }
                window.appendAlert(line);

                // Zapis do bazy danych (bez zmian)
                if (outRecords != null) {
                    outRecords.add(new ArbStatRecord(
                            Instant.ofEpochMilli(tsMillis),
                            s.getSymbol(),
                            s.getExchange().name(),
                            refName,
                            s.getSide().name(),
                            rankType,
                            i + 1,
                            s.getFirstLevelGainPctLatest(),
                            s.getFirstLevelGainUsd(),
                            s.getFirstLevelVolumeQuote(),
                            s.getTotalPositiveGainUsdLatest(),
                            s.getTotalVolumeQuoteLatest(),
                            s.getAvgGainPctTotalLatest(),
                            s.getTargetVolumeQuote(),
                            s.getVwapGainPctForTargetVolume(),
                            s.getTargetGainUsd(),
                            s.getVolumeQuoteToReachTargetGain(),
                            s.getGainPctAtTargetGain()
                    ));
                }
            }

            return limit;
        }

    }

}