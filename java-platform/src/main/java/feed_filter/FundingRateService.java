package feed_filter;

import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * FUNDING RATE SERVICE - Wrapper for FundingRateStore with normalization
 * 
 * Purpose:
 * - Read funding rates from FundingRateStore (populated by Feed)
 * - Normalize to 24h equivalent for consistent comparison
 * - Calculate funding impact on gain
 * 
 * Normalization:
 * - 8h interval: FR * 3
 * - 4h interval: FR * 6
 * - 1h interval: FR * 24
 * 
 * Funding Impact on MM:
 * - BUY on source = SHORT on reference
 *   - Negative FR = bad for SHORT (we pay) → reduces gain
 *   - Positive FR = good for SHORT (we receive) → no reduction
 * - SELL on source = LONG on reference (closing short or going long)
 *   - Positive FR = bad for LONG → reduces gain
 *   - Negative FR = good for LONG → no reduction
 */
@Slf4j
public class FundingRateService {

    // Singleton instance
    private static volatile FundingRateService instance;
    private static final Object lock = new Object();

    private final FundingRateStore fundingRateStore;
    
    // Refreshers per exchange (for manual trigger)
    private final Map<Exchange, Runnable> refreshers = new ConcurrentHashMap<>();
    
    // Default interval if not provided
    private static final int DEFAULT_INTERVAL_HOURS = 8;
    
    // =========================================================================
    // SINGLETON
    // =========================================================================
    
    /**
     * Get singleton instance (creates with new FundingRateStore if not exists).
     */
    public static FundingRateService getInstance() {
        if (instance == null) {
            synchronized (lock) {
                if (instance == null) {
                    instance = new FundingRateService(new FundingRateStore());
                }
            }
        }
        return instance;
    }
    
    /**
     * Initialize singleton with specific store.
     */
    public static void initialize(FundingRateStore store) {
        synchronized (lock) {
            if (instance == null) {
                instance = new FundingRateService(store);
            } else {
                log.warn("FundingRateService already initialized, ignoring new store");
            }
        }
    }
    
    // =========================================================================
    // CONSTRUCTOR
    // =========================================================================
    
    public FundingRateService(FundingRateStore fundingRateStore) {
        this.fundingRateStore = fundingRateStore;
    }
    
    // =========================================================================
    // STORE ACCESS
    // =========================================================================
    
    /**
     * Get underlying FundingRateStore.
     */
    public FundingRateStore getStore() {
        return fundingRateStore;
    }
    
    // =========================================================================
    // REFRESH MANAGEMENT
    // =========================================================================
    
    /**
     * Register a refresher callback for an exchange.
     * Called when we want to manually trigger funding rate refresh.
     */
    public void registerRefresher(Exchange exchange, Runnable refresher) {
        refreshers.put(exchange, refresher);
        log.info("Registered funding refresher for {}", exchange);
    }
    
    /**
     * Refresh funding rates for all registered exchanges.
     */
    public void refreshAll() {
        log.debug("Refreshing all funding rates ({} exchanges)", refreshers.size());
        for (Map.Entry<Exchange, Runnable> entry : refreshers.entrySet()) {
            try {
                entry.getValue().run();
            } catch (Exception e) {
                log.error("Failed to refresh funding for {}: {}", entry.getKey(), e.getMessage());
            }
        }
    }
    
    /**
     * Refresh funding for specific exchange.
     */
    public void refresh(Exchange exchange) {
        Runnable refresher = refreshers.get(exchange);
        if (refresher != null) {
            try {
                refresher.run();
            } catch (Exception e) {
                log.error("Failed to refresh funding for {}: {}", exchange, e.getMessage());
            }
        }
    }
    
    // =========================================================================
    // READ API
    // =========================================================================
    
    /**
     * Get funding rate for a symbol.
     * 
     * @param exchange Futures exchange
     * @param symbol Symbol
     * @return FundingInfo or null if not available
     */
    public FundingInfo getFundingInfo(Exchange exchange, String symbol) {
        if (fundingRateStore == null) {
            return null;
        }
        return fundingRateStore.get(exchange, symbol);
    }
    
    /**
     * Get raw funding rate.
     * 
     * @param exchange Futures exchange
     * @param symbol Symbol
     * @return Funding rate as BigDecimal, or ZERO if not available
     */
    public BigDecimal getFundingRate(Exchange exchange, String symbol) {
        FundingInfo info = getFundingInfo(exchange, symbol);
        return info != null ? BigDecimal.valueOf(info.getFundingRate()) : BigDecimal.ZERO;
    }
    
    /**
     * Get funding rate normalized to 24h equivalent.
     * 
     * @param exchange Futures exchange
     * @param symbol Symbol
     * @return Normalized 24h funding rate
     */
    public BigDecimal getFundingRateNormalized24h(Exchange exchange, String symbol) {
        FundingInfo info = getFundingInfo(exchange, symbol);
        if (info == null) {
            return BigDecimal.ZERO;
        }
        
        int intervalHours = info.getFundingIntervalHours() != null 
                ? info.getFundingIntervalHours() 
                : DEFAULT_INTERVAL_HOURS;
        
        return normalizeTo24h(BigDecimal.valueOf(info.getFundingRate()), intervalHours);
    }
    
    /**
     * Get funding interval in hours.
     */
    public int getFundingIntervalHours(Exchange exchange, String symbol) {
        FundingInfo info = getFundingInfo(exchange, symbol);
        return info != null && info.getFundingIntervalHours() != null 
                ? info.getFundingIntervalHours() 
                : DEFAULT_INTERVAL_HOURS;
    }
    
    // =========================================================================
    // GAIN ADJUSTMENT API
    // =========================================================================
    
    /**
     * Adjust gain for funding rate impact.
     * 
     * @param mmSide MM side (BUY/SELL on SOURCE)
     * @param rawGain Raw gain from spread calculation
     * @param refExchange Reference (futures) exchange
     * @param refSymbol Reference symbol
     * @return Adjusted gain (reduced if funding is unfavorable)
     */
    public BigDecimal adjustGainForFunding(String mmSide, BigDecimal rawGain, 
                                            Exchange refExchange, String refSymbol) {
        FundingInfo info = getFundingInfo(refExchange, refSymbol);
        if (info == null) {
            return rawGain;
        }
        
        int intervalHours = info.getFundingIntervalHours() != null 
                ? info.getFundingIntervalHours() 
                : DEFAULT_INTERVAL_HOURS;
        
        return adjustGainForFunding(mmSide, rawGain, 
                BigDecimal.valueOf(info.getFundingRate()), intervalHours);
    }
    
    /**
     * Adjust gain for funding rate impact (with explicit parameters).
     * 
     * @param mmSide MM side (BUY/SELL on SOURCE)
     * @param rawGain Raw gain from spread calculation
     * @param fundingRate Funding rate
     * @param intervalHours Funding interval in hours
     * @return Adjusted gain
     */
    public BigDecimal adjustGainForFunding(String mmSide, BigDecimal rawGain,
                                            BigDecimal fundingRate, int intervalHours) {
        if (fundingRate == null || fundingRate.compareTo(BigDecimal.ZERO) == 0) {
            return rawGain;
        }
        
        BigDecimal normalizedFR = normalizeTo24h(fundingRate, intervalHours);
        
        // mmSide = BUY on source → SHORT on reference
        // Negative FR = unfavorable for SHORT (we pay)
        if ("BUY".equalsIgnoreCase(mmSide) && fundingRate.compareTo(BigDecimal.ZERO) < 0) {
            // FR is negative, so adding it reduces gain
            return rawGain.add(normalizedFR);
        }
        
        // mmSide = SELL on source → LONG on reference (or closing short)
        // Positive FR = unfavorable for LONG
        if ("SELL".equalsIgnoreCase(mmSide) && fundingRate.compareTo(BigDecimal.ZERO) > 0) {
            // FR is positive, subtract from gain
            return rawGain.subtract(normalizedFR);
        }
        
        // FR is favorable or neutral - no adjustment
        return rawGain;
    }
    
    /**
     * Calculate funding impact (how much FR affects gain).
     * 
     * @param mmSide MM side
     * @param refExchange Reference exchange
     * @param refSymbol Reference symbol
     * @return Funding impact (negative = reduces gain, positive = increases gain, zero = no impact)
     */
    public BigDecimal calculateFundingImpact(String mmSide, Exchange refExchange, String refSymbol) {
        FundingInfo info = getFundingInfo(refExchange, refSymbol);
        if (info == null) {
            return BigDecimal.ZERO;
        }
        
        BigDecimal fundingRate = BigDecimal.valueOf(info.getFundingRate());
        int intervalHours = info.getFundingIntervalHours() != null 
                ? info.getFundingIntervalHours() 
                : DEFAULT_INTERVAL_HOURS;
        
        BigDecimal normalizedFR = normalizeTo24h(fundingRate, intervalHours);
        
        // BUY (SHORT) with negative FR = negative impact
        if ("BUY".equalsIgnoreCase(mmSide) && fundingRate.compareTo(BigDecimal.ZERO) < 0) {
            return normalizedFR; // Already negative
        }
        
        // SELL (LONG) with positive FR = negative impact
        if ("SELL".equalsIgnoreCase(mmSide) && fundingRate.compareTo(BigDecimal.ZERO) > 0) {
            return normalizedFR.negate();
        }
        
        // Favorable - no negative impact
        return BigDecimal.ZERO;
    }
    
    // =========================================================================
    // RESULT CLASS (for detailed logging)
    // =========================================================================
    
    public static class FundingAdjustmentResult {
        public final BigDecimal rawGain;
        public final BigDecimal adjustedGain;
        public final BigDecimal fundingRate;
        public final int intervalHours;
        public final BigDecimal normalizedFR24h;
        public final BigDecimal fundingImpact;
        public final boolean isFavorable;
        
        public FundingAdjustmentResult(BigDecimal rawGain, BigDecimal adjustedGain,
                                        BigDecimal fundingRate, int intervalHours,
                                        BigDecimal normalizedFR24h, BigDecimal fundingImpact,
                                        boolean isFavorable) {
            this.rawGain = rawGain;
            this.adjustedGain = adjustedGain;
            this.fundingRate = fundingRate;
            this.intervalHours = intervalHours;
            this.normalizedFR24h = normalizedFR24h;
            this.fundingImpact = fundingImpact;
            this.isFavorable = isFavorable;
        }
        
        @Override
        public String toString() {
            return String.format("FR[rate=%.6f (%dh), norm24h=%.6f, impact=%.4f%%, favorable=%s]",
                    fundingRate, intervalHours, normalizedFR24h, 
                    fundingImpact.multiply(BigDecimal.valueOf(100)), isFavorable);
        }
    }
    
    /**
     * Get detailed funding adjustment result (for logging).
     */
    public FundingAdjustmentResult getAdjustmentDetails(String mmSide, BigDecimal rawGain,
                                                         Exchange refExchange, String refSymbol) {
        FundingInfo info = getFundingInfo(refExchange, refSymbol);
        
        if (info == null) {
            return new FundingAdjustmentResult(rawGain, rawGain, BigDecimal.ZERO, 
                    DEFAULT_INTERVAL_HOURS, BigDecimal.ZERO, BigDecimal.ZERO, true);
        }
        
        BigDecimal fundingRate = BigDecimal.valueOf(info.getFundingRate());
        int intervalHours = info.getFundingIntervalHours() != null 
                ? info.getFundingIntervalHours() 
                : DEFAULT_INTERVAL_HOURS;
        
        BigDecimal normalizedFR = normalizeTo24h(fundingRate, intervalHours);
        BigDecimal adjustedGain = adjustGainForFunding(mmSide, rawGain, fundingRate, intervalHours);
        BigDecimal impact = calculateFundingImpact(mmSide, refExchange, refSymbol);
        boolean favorable = impact.compareTo(BigDecimal.ZERO) >= 0;
        
        return new FundingAdjustmentResult(rawGain, adjustedGain, fundingRate, intervalHours,
                normalizedFR, impact, favorable);
    }
    
    // =========================================================================
    // HELPERS
    // =========================================================================
    
    /**
     * Normalize funding rate to 24h equivalent.
     * 
     * @param fundingRate Raw funding rate
     * @param intervalHours Funding interval (1, 4, 8 hours typical)
     * @return 24h normalized rate
     */
    private BigDecimal normalizeTo24h(BigDecimal fundingRate, int intervalHours) {
        if (intervalHours <= 0) {
            intervalHours = DEFAULT_INTERVAL_HOURS;
        }
        
        BigDecimal multiplier = BigDecimal.valueOf(24.0 / intervalHours);
        return fundingRate.multiply(multiplier).setScale(8, RoundingMode.HALF_UP);
    }
}
