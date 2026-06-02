// BigGainScanner.java

package feed_filter;

import manager_client.ManagerSymbol;
import manager_client.SymbolRegistry;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Computes theoretical arbitrage gains between a source exchange and a reference
 * exchange for all (exchange, symbol, side) combinations.
 *
 * Fee rates: when SymbolRegistry is provided, uses fee_market_buy/fee_market_sell
 * from manager.symbol per exchange. Otherwise falls back to totalFeesPct/2.
 */
public class BigGainScanner {

    /**
     * Scan current order books and compute snapshots for all (sourceExchange, symbol, side)
     * combinations versus the active reference exchange for that symbol.
     *
     * @param symbolRegistry When non-null, fee rates are read from manager.symbol per (exchange, symbol).
     */
    public static List<BigGainSnapshot> scanWindow(
            OrderBookStore store,
            List<Exchange> sourceExchanges,
            Map<String, Exchange> refExchangeMap,
            List<String> symbols,
            long windowMillis,
            double totalFeesPct,
            double targetVolumeQuote,
            double targetGainUsd,
            SymbolRegistry symbolRegistry
    ) {
        List<BigGainSnapshot> result = new ArrayList<>();
        if (store == null || sourceExchanges == null || symbols == null || refExchangeMap == null) {
            return result;
        }

        double defaultFee = totalFeesPct / 2.0;

        for (String symbol : symbols) {
            if (symbol == null || symbol.isEmpty()) {
                continue;
            }

            Exchange refExchange = refExchangeMap.get(symbol);
            if (refExchange == null) {
                // Symbol has no active reference defined in DB
                continue;
            }

            OrderBookSnapshot refSnap = store.get(refExchange, symbol);
            if (refSnap == null) {
                continue;
            }

            for (Exchange srcEx : sourceExchanges) {
                if (srcEx == null) continue;

                if (srcEx == refExchange) continue;

                OrderBookSnapshot srcSnap = store.get(srcEx, symbol);
                if (srcSnap == null) {
                    continue;
                }

                // Resolve fee rates from DB (manager.symbol) when available
                double feeSrcBuy = defaultFee;
                double feeSrcSell = defaultFee;
                double feeRefBuy = defaultFee;
                double feeRefSell = defaultFee;
                if (symbolRegistry != null) {
                    ManagerSymbol msSrc = symbolRegistry.getByInternalOrMain(srcEx, symbol);
                    ManagerSymbol msRef = symbolRegistry.getByInternalOrMain(refExchange, symbol);
                    feeSrcBuy = toFee(msSrc != null ? msSrc.feeMarketBuy : null, defaultFee);
                    feeSrcSell = toFee(msSrc != null ? msSrc.feeMarketSell : null, defaultFee);
                    feeRefBuy = toFee(msRef != null ? msRef.feeMarketBuy : null, defaultFee);
                    feeRefSell = toFee(msRef != null ? msRef.feeMarketSell : null, defaultFee);
                }

                // ASK side: buy on src asks (feeSrcBuy), hedge by selling on ref bids (feeRefSell)
                BigGainSnapshot askSnap = computeForSide(
                        symbol,
                        srcEx,
                        BigGainSnapshot.Side.ASK,
                        srcSnap,
                        refSnap,
                        feeSrcBuy,
                        feeRefSell,
                        targetVolumeQuote,
                        targetGainUsd
                );
                if (askSnap != null) {
                    result.add(askSnap);
                }

                // BID side: sell on src bids (feeSrcSell), hedge by buying on ref asks (feeRefBuy)
                BigGainSnapshot bidSnap = computeForSide(
                        symbol,
                        srcEx,
                        BigGainSnapshot.Side.BID,
                        srcSnap,
                        refSnap,
                        feeSrcSell,
                        feeRefBuy,
                        targetVolumeQuote,
                        targetGainUsd
                );
                if (bidSnap != null) {
                    result.add(bidSnap);
                }

            }
        }

        return result;
    }

    private static BigGainSnapshot computeForSide(
            String symbol,
            Exchange srcExchange,
            BigGainSnapshot.Side side,
            OrderBookSnapshot srcSnap,
            OrderBookSnapshot refSnap,
            double feeSource,
            double feeRef,
            double targetVolumeQuote,
            double targetGainUsd
    ) {
        List<OrderBookLevel> srcLevels;
        List<OrderBookLevel> refLevels;

        if (side == BigGainSnapshot.Side.ASK) {
            srcLevels = srcSnap.getAsks();
            refLevels = refSnap.getBids();
        } else {
            srcLevels = srcSnap.getBids();
            refLevels = refSnap.getAsks();
        }

        if (srcLevels == null || refLevels == null || srcLevels.isEmpty() || refLevels.isEmpty()) {
            return null;
        }

        // First level metrics
        OrderBookLevel srcTop = srcLevels.get(0);
        OrderBookLevel refTop = refLevels.get(0);

        double srcPrice = srcTop.getPrice();
        double refPrice = refTop.getPrice();
        double srcQtyTop = srcTop.getSize();
        double refQtyTop = refTop.getSize();

        if (srcPrice <= 0.0 || refPrice <= 0.0 || srcQtyTop <= 0.0 || refQtyTop <= 0.0) {
            return null;
        }


        boolean isAskSide = (side == BigGainSnapshot.Side.ASK);

        // Effective prices including fees and per-unit gain definition
        double effSrcTop;   // price on src leg after fees
        double effRefTop;   // price on ref leg after fees
        double perUnitTop;  // PnL per 1 base unit
        double costPriceTop; // which leg defines capital/risk

        if (isAskSide) {
            // BUY on src (taker against asks), SELL on ref (taker against bids)
            effSrcTop = srcPrice * (1.0 + feeSource);
            effRefTop = refPrice * (1.0 - feeRef);
            perUnitTop = effRefTop - effSrcTop;  // sell high - buy low
            costPriceTop = effSrcTop;            // we commit capital on the buy leg
        } else {
            // SELL on src (taker against bids), BUY on ref (taker against asks)
            effSrcTop = srcPrice * (1.0 - feeSource);
            effRefTop = refPrice * (1.0 + feeRef);
            perUnitTop = effSrcTop - effRefTop;  // sell high - buy low
            costPriceTop = effRefTop;            // capital is committed on the buy leg (ref)
        }

        double firstLevelGainPct = (perUnitTop / costPriceTop) * 100.0;

        // first line notional and gain (quoted in terms of the capital leg)
        double tradableQtyTop = Math.min(srcQtyTop, refQtyTop);
        double firstLevelVolumeQuote = costPriceTop * tradableQtyTop;
        double firstLevelGainUsd = perUnitTop * tradableQtyTop;

        // Walk books to compute total positive gain and VWAP metrics
        double totalPositiveGainUsd = 0.0;
        double totalVolumeQuote = 0.0;

        double vwapGainPctForTargetVolume = Double.NaN;
        double volumeUsedForTargetVolume = 0.0;
        double targetVolumeQuoteEffective = targetVolumeQuote;

        double gainPctAtTargetGain = Double.NaN;
        double volumeQuoteToReachTargetGain = Double.NaN;
        double targetGainUsdEffective = targetGainUsd;

        double cumCostForVolume = 0.0;
        double cumProceedsForVolume = 0.0;

        double cumGainForTarget = 0.0;
        double cumCostForTarget = 0.0;

        int i = 0;
        int j = 0;

        double remainingSrcQty = srcLevels.get(0).getSize();
        double remainingRefQty = refLevels.get(0).getSize();

        while (i < srcLevels.size() && j < refLevels.size()) {
            OrderBookLevel srcLevel = srcLevels.get(i);
            OrderBookLevel refLevel = refLevels.get(j);

            double srcLevelPrice = srcLevel.getPrice();
            double refLevelPrice = refLevel.getPrice();
            if (srcLevelPrice <= 0.0 || refLevelPrice <= 0.0) {
                break;
            }

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

            double srcQty = remainingSrcQty;
            double refQty = remainingRefQty;

            if (srcQty <= 0.0 || refQty <= 0.0) {
                break;
            }


            double effSrc;
            double effRef;
            double costPrice;
            double proceedsPrice;

            if (isAskSide) {
                // BUY src, SELL ref
                effSrc = srcLevelPrice * (1.0 + feeSource);
                effRef = refLevelPrice * (1.0 - feeRef);
                costPrice = effSrc;
                proceedsPrice = effRef;
            } else {
                // SELL src, BUY ref
                effSrc = srcLevelPrice * (1.0 - feeSource);
                effRef = refLevelPrice * (1.0 + feeRef);
                costPrice = effRef;    // capital leg is on the BUY side (ref)
                proceedsPrice = effSrc;
            }

            double perUnit = proceedsPrice - costPrice;
            if (perUnit <= 0.0) {
                // no more positive depth beyond this point for the classic "positive gain" part
                break;
            }

            double tradableQty = Math.min(srcQty, refQty);
            double levelCostQuote = costPrice * tradableQty;
            double levelProceeds = proceedsPrice * tradableQty;
            double levelGainUsd = levelProceeds - levelCostQuote;

            // stats for "classic" positive gain
            totalPositiveGainUsd += levelGainUsd;
            totalVolumeQuote += levelCostQuote;

            // VWAP for target volume
            if (Double.isNaN(vwapGainPctForTargetVolume) && targetVolumeQuoteEffective > 0.0) {
                double remainingVolume = targetVolumeQuoteEffective - cumCostForVolume;
                if (remainingVolume > 0.0) {
                    if (levelCostQuote <= remainingVolume) {
                        // take whole level
                        cumCostForVolume += levelCostQuote;
                        cumProceedsForVolume += levelProceeds;
                    } else {
                        // take partial level to reach exactly targetVolumeQuote
                        double neededQty = remainingVolume / costPrice;
                        double partialCost = costPrice * neededQty;
                        double partialProceeds = proceedsPrice * neededQty;
                        cumCostForVolume += partialCost;
                        cumProceedsForVolume += partialProceeds;
                    }
                    if (cumCostForVolume >= targetVolumeQuoteEffective - 1e-9) {
                        double gainPct = (cumProceedsForVolume - cumCostForVolume) / cumCostForVolume * 100.0;
                        vwapGainPctForTargetVolume = gainPct;
                        volumeUsedForTargetVolume = cumCostForVolume;
                    }
                }
            }

            // Target gain Y
            if (Double.isNaN(volumeQuoteToReachTargetGain) && targetGainUsdEffective > 0.0) {
                double potentialGainWithLevel = cumGainForTarget + levelGainUsd;
                if (potentialGainWithLevel >= targetGainUsdEffective) {
                    double remainingGain = targetGainUsdEffective - cumGainForTarget;
                    if (remainingGain < 0.0) {
                        remainingGain = 0.0;
                    }
                    double neededQty = remainingGain / perUnit;
                    double additionalCost = costPrice * neededQty;

                    cumGainForTarget = targetGainUsdEffective;
                    cumCostForTarget += additionalCost;

                    volumeQuoteToReachTargetGain = cumCostForTarget;
                    double gainPct = cumGainForTarget / cumCostForTarget * 100.0;
                    gainPctAtTargetGain = gainPct;
                } else {
                    cumGainForTarget += levelGainUsd;
                    cumCostForTarget += levelCostQuote;
                }
            }

            // update remaining quantities
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

        double avgGainPctTotal;
        if (totalVolumeQuote > 0.0) {
            avgGainPctTotal = (totalPositiveGainUsd / totalVolumeQuote) * 100.0;
        } else {
            avgGainPctTotal = Double.NaN;
        }

        // if we never reached targetVolumeQuote, leave VWAP as NaN
        if (Double.isNaN(vwapGainPctForTargetVolume)) {
            volumeUsedForTargetVolume = Double.NaN;
        }

        // if we never reached target gain, leave both as NaN
        if (Double.isNaN(volumeQuoteToReachTargetGain)) {
            gainPctAtTargetGain = Double.NaN;
        }

        // min thresholds are no longer used for filtering in the scanner, but
        // we keep them as constant metadata in the snapshot for compatibility.
        double minFirstLevelGainPct = 0.01;
        double minTotalPositiveGainUsd = 1.0;

        return new BigGainSnapshot(
                symbol,
                srcExchange,
                side,
                firstLevelGainPct,
                totalPositiveGainUsd,
                totalVolumeQuote,
                avgGainPctTotal,
                minFirstLevelGainPct,
                minTotalPositiveGainUsd,
                firstLevelVolumeQuote,
                firstLevelGainUsd,
                targetVolumeQuoteEffective,
                vwapGainPctForTargetVolume,
                targetGainUsdEffective,
                volumeQuoteToReachTargetGain,
                gainPctAtTargetGain
        );
    }

    /**
     * Helper: compute VWAP-based gain percentage for a given target quote volume
     * using a single pair of order books (source vs reference) and a given side.
     *
     * This uses the same internal math as the main scanner (computeForSide),
     * but returns only the vwapGainPctForTargetVolume for a single snapshot pair.
     *
     * @return gain percentage (in %), or Double.NaN if books are missing or invalid.
     */

    /**
     * Overload with explicit fee rates. Use when fee rates come from manager.symbol.
     */
    public static double computeVwapGainPctForVolume(
            OrderBookSnapshot srcSnap,
            OrderBookSnapshot refSnap,
            BigGainSnapshot.Side side,
            double feeSource,
            double feeRef,
            double targetVolumeQuote
    ) {
        return computeVwapGainPctForVolumeImpl(srcSnap, refSnap, side, feeSource, feeRef, targetVolumeQuote);
    }

    /**
     * Overload with totalFeesPct (split equally). Kept for backward compatibility.
     */
    public static double computeVwapGainPctForVolume(
            OrderBookSnapshot srcSnap,
            OrderBookSnapshot refSnap,
            BigGainSnapshot.Side side,
            double totalFeesPct,
            double targetVolumeQuote
    ) {
        double fee = totalFeesPct / 2.0;
        return computeVwapGainPctForVolumeImpl(srcSnap, refSnap, side, fee, fee, targetVolumeQuote);
    }

    private static double computeVwapGainPctForVolumeImpl(
            OrderBookSnapshot srcSnap,
            OrderBookSnapshot refSnap,
            BigGainSnapshot.Side side,
            double feeSource,
            double feeRef,
            double targetVolumeQuote
    ) {
        if (srcSnap == null || refSnap == null || side == null) {
            return Double.NaN;
        }
        if (targetVolumeQuote <= 0.0) {
            return Double.NaN;
        }

        java.util.List<OrderBookLevel> srcLevels;
        java.util.List<OrderBookLevel> refLevels;

        if (side == BigGainSnapshot.Side.ASK) {
            srcLevels = srcSnap.getAsks();
            refLevels = refSnap.getBids();
        } else {
            srcLevels = srcSnap.getBids();
            refLevels = refSnap.getAsks();
        }

        if (srcLevels == null || refLevels == null || srcLevels.isEmpty() || refLevels.isEmpty()) {
            return Double.NaN;
        }

        int i = 0;
        int j = 0;

        double remainingSrcQty = srcLevels.get(0).getSize();
        double remainingRefQty = refLevels.get(0).getSize();

        double cumCost = 0.0;
        double cumProceeds = 0.0;

        while (cumCost < targetVolumeQuote - 1e-9) {
            if (remainingSrcQty <= 1e-12) {
                i++;
                if (i >= srcLevels.size()) {
                    break;
                }
                remainingSrcQty = srcLevels.get(i).getSize();
            }
            if (remainingRefQty <= 1e-12) {
                j++;
                if (j >= refLevels.size()) {
                    break;
                }
                remainingRefQty = refLevels.get(j).getSize();
            }
            if (remainingSrcQty <= 1e-12 || remainingRefQty <= 1e-12) {
                break;
            }

            OrderBookLevel srcLevel = srcLevels.get(i);
            OrderBookLevel refLevel = refLevels.get(j);

            double srcPrice = srcLevel.getPrice();
            double refPrice = refLevel.getPrice();

            if (srcPrice <= 0.0 || refPrice <= 0.0) {
                return Double.NaN;
            }


            double effSrc;
            double effRef;
            double costPrice;
            double proceedsPrice;
            if (side == BigGainSnapshot.Side.ASK) {
                // BUY on src, SELL on ref
                effSrc = srcPrice * (1.0 + feeSource);
                effRef = refPrice * (1.0 - feeRef);
                costPrice = effSrc;
                proceedsPrice = effRef;
            } else {
                // SELL on src, BUY on ref
                effSrc = srcPrice * (1.0 - feeSource);
                effRef = refPrice * (1.0 + feeRef);
                costPrice = effRef;
                proceedsPrice = effSrc;
            }

            double tradableQty = Math.min(remainingSrcQty, remainingRefQty);
            double levelCostQuote = costPrice * tradableQty;

            double remainingQuote = targetVolumeQuote - cumCost;
            if (levelCostQuote > remainingQuote) {
                double factor = remainingQuote / levelCostQuote;
                tradableQty = tradableQty * factor;
                levelCostQuote = costPrice * tradableQty;
            }

            double levelProceedsQuote = proceedsPrice * tradableQty;

            cumCost += levelCostQuote;
            cumProceeds += levelProceedsQuote;

            remainingSrcQty -= tradableQty;
            remainingRefQty -= tradableQty;

            if (tradableQty <= 0.0) {
                break;
            }
        }

        if (cumCost < targetVolumeQuote - 1e-3) {
            // not enough depth to fill (almost) the whole target volume
            return Double.NaN;
        }

        double gainPct = (cumProceeds - cumCost) / cumCost * 100.0;
        return gainPct;
    }

    private static double toFee(BigDecimal fee, double defaultFee) {
        if (fee == null) return defaultFee;
        double d = fee.doubleValue();
        return (d >= 0.0 && d <= 1.0) ? d : defaultFee;
    }

}