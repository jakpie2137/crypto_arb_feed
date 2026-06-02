package common;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class OrderRequest {
    // Giełda i Para
    private String exchange;
    private String symbol;

    // Szczegóły zlecenia
    private String side;   // "BUY" / "SELL"
    private String type;   // "MARKET", "LIMIT", "LIMIT_MAKER" itp.

    // Ilości i Cena (double wystarczy dla warstwy wykonawczej API)
    private double amount;
    private double price;
    
    // Fallback price for MARKET orders (used for min_vol validation in QUOTE currency)
    // When price=0 for MARKET orders, this can be used to estimate order value
    private Double fallbackPrice;  // null = not available
    
    // Futures-specific: reduce_only flag (for position netting)
    private Boolean reduceOnly;  // null = not specified, true = only reduce position, false = can open/increase

    // Cycle metadata (for tx logging / mm.signal)
    private String cycleId;
    private String logicId;
    private String leg; // SOURCE / HEDGE

    // IOC worst price (deepest OB level needed for full fill)
    private Double worstPrice;
    
    // Per-logic IOC status check delay (ms)
    private Long iocStatusCheckDelayMs;
}