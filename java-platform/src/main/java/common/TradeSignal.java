package common;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.math.BigDecimal;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class TradeSignal {
    private String exchange;
    private String symbol;
    private String side; // "BUY" / "SELL"
    private String type; // [ADDED] "MARKET" / "LIMIT" (Wymagane przez Router)

    // Wewnątrz logiki MM używamy BigDecimal dla precyzji
    private BigDecimal price;
    private BigDecimal worstPrice; // deepest level price for full fill (IOC)
    private BigDecimal qty;

    private String assetUsed;   // np. USDT (co wydajemy)
    private String targetAsset; // np. ETH (co kupujemy)

    private long timestamp;
    
    // Futures-specific: reduce_only flag (for position netting)
    private Boolean reduceOnly;  // null = not specified, true = only reduce position, false = can open/increase
    
    // Execution policy for order execution (retry, timeout, fallback)
    // Optional - if null, router will use default behavior
    private ExecutionPolicy executionPolicy;
    
    // Safe volume for worst_price calculation (fights fake liquidity).
    // Router uses this for retry price calculations instead of remainingQty.
    // If 0 or null, router will use originalQty.
    private Double safeVolumeQty;

    // Cross rates from signal moment (for retry - do not refresh). Used for PnL/expPnL in router.transaction.
    private Double cross1Rate;
    private Double cross2Rate;

    /**
     * Konstruktor kompatybilny z OrderRouterClient (7 argumentów).
     * Ignoruje assetUsed/targetAsset, które nie są krytyczne dla samego routingu w tej fazie.
     */
    public TradeSignal(String exchange, String symbol, String side, String type,
                       BigDecimal qty, BigDecimal price, long timestamp) {
        this.exchange = exchange;
        this.symbol = symbol;
        this.side = side;
        this.type = type;
        this.qty = qty;
        this.price = price;
        this.worstPrice = price; // default to price unless overridden
        this.timestamp = timestamp;
    }

    /**
     * Konwertuje sygnał strategii na żądanie wykonania dla Routera.
     */
    public OrderRequest toOrderRequest() {
        return new OrderRequest(
                this.exchange,
                this.symbol,
                this.side,
                (this.type != null ? this.type : "LIMIT"), // Używamy typu z sygnału lub default
                this.qty.doubleValue(),
                this.price.doubleValue(),
                null,  // fallbackPrice - not available in TradeSignal, will be set by OrderRouterClient
                this.reduceOnly,  // Pass reduceOnly for futures position netting
                null, // cycleId
                null, // logicId
                null, // leg
                this.worstPrice != null ? this.worstPrice.doubleValue() : null,
                null
        );
    }
}