package manager_client;

import feed_filter.Exchange;

import java.math.BigDecimal;

public class FeeProvider {

    private final SymbolRegistry registry;

    public FeeProvider(SymbolRegistry registry) {
        this.registry = registry;
    }

    /**
     * Returns fee pct as BigDecimal (e.g., 0.00045).
     */
    public BigDecimal feePct(Exchange exchange, String internalSymbol, OrderSide side, OrderType type) {
        ManagerSymbol s = registry.getByInternal(exchange, internalSymbol);
        if (s == null) return null;

        if (type == OrderType.MARKET) {
            return side == OrderSide.BUY ? s.feeMarketBuy : s.feeMarketSell;
        } else {
            return side == OrderSide.BUY ? s.feeLimitBuy : s.feeLimitSell;
        }
    }
}
