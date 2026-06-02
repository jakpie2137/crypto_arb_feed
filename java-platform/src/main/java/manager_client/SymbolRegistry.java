package manager_client;

import feed_filter.Exchange;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class SymbolRegistry {

    private final SymbolRepository repo;

    private final Map<Exchange, Map<String, ManagerSymbol>> byInternal = new ConcurrentHashMap<>();
    private final Map<Exchange, Map<String, String>> externalToInternal = new ConcurrentHashMap<>();
    private final Map<Exchange, List<ManagerSymbol>> activeList = new ConcurrentHashMap<>();
    
    // Map for lookup by full exchange name (with subaccount) and external_symbol
    // Key: "EXCHANGE|external_symbol" (e.g., "KUCOIN8SUB0|PRCL-USDT")
    private final Map<String, ManagerSymbol> byFullExchangeAndExternal = new ConcurrentHashMap<>();

    public SymbolRegistry(SymbolRepository repo) {
        this.repo = repo;
    }

    public synchronized void refresh() {
        List<ManagerSymbol> all = repo.loadAllActive();

        Map<Exchange, List<ManagerSymbol>> tmpActive = new EnumMap<>(Exchange.class);
        Map<Exchange, Map<String, ManagerSymbol>> tmpByInternal = new EnumMap<>(Exchange.class);
        Map<Exchange, Map<String, String>> tmpExtToInt = new EnumMap<>(Exchange.class);

        Map<String, ManagerSymbol> tmpByFullExchangeAndExternal = new HashMap<>();
        
        for (ManagerSymbol s : all) {
            Exchange ex = ExchangeMapper.toFeedExchange(s.exchange);
            if (ex == null) {
                // DB may include exchanges not implemented in code yet (e.g., BYBITFUT). Skip for now.
                continue;
            }

            tmpActive.computeIfAbsent(ex, k -> new ArrayList<>()).add(s);

            tmpByInternal.computeIfAbsent(ex, k -> new HashMap<>())
                    .put(norm(s.symbol), s);

            if (s.externalSymbol != null && !s.externalSymbol.isBlank()) {
                Map<String, String> m = tmpExtToInt.computeIfAbsent(ex, k -> new HashMap<>());
                String ext = norm(s.externalSymbol);
                m.put(ext, s.symbol);

                // Also accept common "separator-less" variants from exchange payloads (e.g., BTCUSDT vs BTC_USDT).
                String extAltRaw = s.externalSymbol.replace("_", "").replace("-", "").replace("/", "");
                String extAlt = norm(extAltRaw);
                if (!extAlt.isBlank()) {
                    m.putIfAbsent(extAlt, s.symbol);
                }
                
                // Build map for lookup by full exchange name (with subaccount) and external_symbol
                // Key format: "EXCHANGE|external_symbol" (e.g., "KUCOIN8SUB0|PRCL-USDT")
                String fullKey = norm(s.exchange) + "|" + ext;
                tmpByFullExchangeAndExternal.put(fullKey, s);
                
                // Also add separator-less variant
                String fullKeyAlt = norm(s.exchange) + "|" + extAlt;
                tmpByFullExchangeAndExternal.putIfAbsent(fullKeyAlt, s);
            }
        }

        // finalize: sort activeList by symbol for stable UI
        for (Map.Entry<Exchange, List<ManagerSymbol>> e : tmpActive.entrySet()) {
            e.getValue().sort(Comparator.comparing(o -> norm(o.symbol)));
        }

        byInternal.clear(); byInternal.putAll(tmpByInternal);
        externalToInternal.clear(); externalToInternal.putAll(tmpExtToInt);
        activeList.clear(); activeList.putAll(tmpActive);
        byFullExchangeAndExternal.clear(); byFullExchangeAndExternal.putAll(tmpByFullExchangeAndExternal);
    }

    public List<ManagerSymbol> getActiveSymbols(Exchange exchange) {
        return activeList.getOrDefault(exchange, List.of());
    }

    public ManagerSymbol getByInternal(Exchange exchange, String internalSymbol) {
        Map<String, ManagerSymbol> m = byInternal.get(exchange);
        if (m == null) return null;
        return m.get(norm(internalSymbol));
    }

    /**
     * Lookup ManagerSymbol with fallback to main feed exchange when subaccount has no entry.
     * E.g. BINANCEFUT8SUBFIN -> try BINANCEFUT if null; subaccounts often share symbol+fee with main.
     */
    public ManagerSymbol getByInternalOrMain(Exchange exchange, String internalSymbol) {
        ManagerSymbol ms = getByInternal(exchange, internalSymbol);
        if (ms != null) return ms;
        Exchange main = mainFeedForSubaccount(exchange);
        return main != null ? getByInternal(main, internalSymbol) : null;
    }

    private static Exchange mainFeedForSubaccount(Exchange ex) {
        if (ex == null) return null;
        return switch (ex) {
            case BINANCEFUT8SUBFIN, BINANCEFUT8SUB0 -> Exchange.BINANCEFUT;
            case GATEFUT8SUB0 -> Exchange.GATEFUT;
            case BITGET8SUB0 -> Exchange.BITGET;
            case KUCOIN8SUB0 -> Exchange.KUCOIN;
            case MEXC8SUB0 -> Exchange.MEXC;
            case BITVAVO8SUB0 -> Exchange.BITVAVO;
            case GATE8SUB0 -> Exchange.GATE;
            default -> null;
        };
    }

    public String mapExternalToInternal(Exchange exchange, String externalSymbol) {
        Map<String, String> m = externalToInternal.get(exchange);
        if (m == null) return null;
        return m.get(norm(externalSymbol));
    }
    
    /**
     * Lookup ManagerSymbol by full exchange name (with subaccount) and external_symbol.
     * This is used for trading operations where we need to match exact subaccount.
     * 
     * @param fullExchangeName Full exchange name with subaccount (e.g., "KUCOIN8SUB0", "GATEFUT8SUB0")
     * @param externalSymbol External symbol from exchange API (e.g., "PRCL-USDT", "PRCL_USDT")
     * @return ManagerSymbol or null if not found
     */
    public ManagerSymbol getByFullExchangeAndExternal(String fullExchangeName, String externalSymbol) {
        if (fullExchangeName == null || externalSymbol == null) return null;
        
        String key = norm(fullExchangeName) + "|" + norm(externalSymbol);
        ManagerSymbol result = byFullExchangeAndExternal.get(key);
        
        // Try separator-less variant if not found
        if (result == null) {
            String extAlt = norm(externalSymbol.replace("_", "").replace("-", "").replace("/", ""));
            if (!extAlt.isBlank()) {
                String keyAlt = norm(fullExchangeName) + "|" + extAlt;
                result = byFullExchangeAndExternal.get(keyAlt);
            }
        }
        
        return result;
    }

    private static String norm(String s) {
        return s == null ? "" : s.trim().toUpperCase();
    }
}