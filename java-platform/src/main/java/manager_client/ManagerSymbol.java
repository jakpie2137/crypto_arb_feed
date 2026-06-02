// ManagerSymbol.java

package manager_client;

import java.math.BigDecimal;

/**
 * Row from manager.symbol
 * Internal symbol = symbol
 * Exchange ID follows DB naming, but in this migration step we map it to feed_filter.Exchange elsewhere.
 */
public class ManagerSymbol {

    public final long id;

    public final String exchange;         // DB exchange id (e.g., BINANCEFUT)
    public final String symbol;           // internal
    public final boolean isActive;

    public final String baseCurr;
    public final String quoteCurr;

    public final String externalSymbol;
    public final String externalBase;
    public final String externalQuote;

    public final Integer pricePrecision;
    public final Integer volumePrecision;

    public final BigDecimal minVol;
    public final String minVolType;

    public final BigDecimal maxVol;
    public final String maxVolType;

    public final String feeCurrBuy;
    public final String feeCurrSell;

    public final BigDecimal feeLimitBuy;
    public final BigDecimal feeMarketBuy;
    public final BigDecimal feeLimitSell;
    public final BigDecimal feeMarketSell;

    public final BigDecimal quotation;
    public final String symbolType;

    public final BigDecimal contractSize;

    public final BigDecimal maxPosition;
    public final String maxPositionType;

    public final BigDecimal leverage;

    public ManagerSymbol(
            long id,
            String exchange,
            String symbol,
            boolean isActive,
            String baseCurr,
            String quoteCurr,
            String externalSymbol,
            String externalBase,
            String externalQuote,
            Integer pricePrecision,
            Integer volumePrecision,
            BigDecimal minVol,
            String minVolType,
            BigDecimal maxVol,
            String maxVolType,
            String feeCurrBuy,
            String feeCurrSell,
            BigDecimal feeLimitBuy,
            BigDecimal feeMarketBuy,
            BigDecimal feeLimitSell,
            BigDecimal feeMarketSell,
            BigDecimal quotation,
            String symbolType,
            BigDecimal contractSize,
            BigDecimal maxPosition,
            String maxPositionType,
            BigDecimal leverage
    ) {
        this.id = id;
        this.exchange = exchange;
        this.symbol = symbol;
        this.isActive = isActive;
        this.baseCurr = baseCurr;
        this.quoteCurr = quoteCurr;
        this.externalSymbol = externalSymbol;
        this.externalBase = externalBase;
        this.externalQuote = externalQuote;
        this.pricePrecision = pricePrecision;
        this.volumePrecision = volumePrecision;
        this.minVol = minVol;
        this.minVolType = minVolType;
        this.maxVol = maxVol;
        this.maxVolType = maxVolType;
        this.feeCurrBuy = feeCurrBuy;
        this.feeCurrSell = feeCurrSell;
        this.feeLimitBuy = feeLimitBuy;
        this.feeMarketBuy = feeMarketBuy;
        this.feeLimitSell = feeLimitSell;
        this.feeMarketSell = feeMarketSell;
        this.quotation = quotation;
        this.symbolType = symbolType;
        this.contractSize = contractSize;
        this.maxPosition = maxPosition;
        this.maxPositionType = maxPositionType;
        this.leverage = leverage;
    }

    @Override
    public String toString() {
        return "ManagerSymbol{" +
                "exchange='" + exchange + '\'' +
                ", symbol='" + symbol + '\'' +
                ", isActive=" + isActive +
                ", externalSymbol='" + externalSymbol + '\'' +
                ", baseCurr='" + baseCurr + '\'' +
                ", quoteCurr='" + quoteCurr + '\'' +
                '}';
    }
}