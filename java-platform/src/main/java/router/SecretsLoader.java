package router;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Loads API secrets from files.
 * 
 * Looks in:
 * 1. /run/secrets/ (Docker secrets)
 * 2. /opt/trading/secrets/ (Local deployment)
 * 3. ./secrets/ (Development)
 * 
 * File naming convention:
 *   {exchange}_api_key
 *   {exchange}_api_secret
 *   {exchange}_passphrase  (for Bitget)
 * 
 * Examples:
 *   bitget8sub0_api_key
 *   bitget8sub0_api_secret
 *   bitget8sub0_passphrase
 *   gatefut_api_key
 *   gatefut_api_secret
 */
@Slf4j
public class SecretsLoader {

    private static final String[] SECRET_DIRS = {
            "/run/secrets",
            "/opt/trading/secrets",
            "./secrets"
    };

    /**
     * Load a secret value from file.
     * Tries: {name}, then {name}.txt (e.g. mexc8sub0_trade_key.txt in /opt/trading/secrets/).
     *
     * @param name Secret name (e.g., "mexc8sub0_trade_key")
     * @return Secret value or null if not found
     */
    public static String loadSecret(String name) {
        for (String dir : SECRET_DIRS) {
            for (String fileName : new String[]{name, name + ".txt"}) {
                Path path = Paths.get(dir, fileName);
                if (Files.exists(path)) {
                    try {
                        String value = Files.readString(path).trim();
                        log.info("[SECRETS] Loaded {} from {}", fileName, dir);
                        return value;
                    } catch (IOException e) {
                        log.warn("[SECRETS] Failed to read {}: {}", path, e.getMessage());
                    }
                }
            }
        }
        
        // Also check environment variable (uppercase with _ prefix)
        String envName = name.toUpperCase();
        String envValue = System.getenv(envName);
        if (envValue != null && !envValue.isEmpty()) {
            log.info("[SECRETS] Loaded {} from ENV", name);
            return envValue;
        }
        
        log.debug("[SECRETS] Secret not found: {}", name);
        return null;
    }

    /**
     * Check if secrets exist for an exchange.
     */
    public static boolean hasSecretsFor(String exchange) {
        String keyName = exchange.toLowerCase() + "_api_key";
        String secretName = exchange.toLowerCase() + "_api_secret";
        return loadSecret(keyName) != null && loadSecret(secretName) != null;
    }

    /**
     * Holder for API credentials.
     */
    public static class ApiCredentials {
        public final String apiKey;
        public final String apiSecret;
        public final String passphrase;  // Optional (Bitget requires it)

        public ApiCredentials(String apiKey, String apiSecret, String passphrase) {
            this.apiKey = apiKey;
            this.apiSecret = apiSecret;
            this.passphrase = passphrase;
        }

        public boolean isValid() {
            return apiKey != null && !apiKey.isEmpty() 
                    && apiSecret != null && !apiSecret.isEmpty();
        }
    }

    /**
     * Load credentials for an exchange (for trading/router).
     * 
     * Looks for trade-specific keys first (_trade_key), 
     * then falls back to generic keys (_api_key).
     * 
     * For subaccounts (e.g., BITGET8SUB0), also tries base exchange (BITGET) as fallback.
     * 
     * Naming convention:
     *   - Trade keys: {exchange}_trade_key, {exchange}_trade_secret, {exchange}_trade_passphrase
     *   - Fallback:   {exchange}_api_key, {exchange}_api_secret, {exchange}_passphrase
     *   - Subaccount fallback: {baseExchange}_api_key (if subaccount keys not found)
     */
    public static ApiCredentials loadCredentials(String exchange) {
        String ex = exchange.toLowerCase();
        
        // Try trade-specific keys first (recommended for router)
        String apiKey = loadSecret(ex + "_trade_key");
        String apiSecret = loadSecret(ex + "_trade_secret");
        String passphrase = loadSecret(ex + "_trade_passphrase");
        
        // Fallback to generic keys (backward compatibility)
        if (apiKey == null) apiKey = loadSecret(ex + "_api_key");
        if (apiSecret == null) apiSecret = loadSecret(ex + "_api_secret");
        if (passphrase == null) passphrase = loadSecret(ex + "_passphrase");
        
        // For subaccounts (e.g., bitget8sub0), also try base exchange (bitget) as fallback
        // This allows using same credentials for main account and subaccounts
        if ((apiKey == null || apiSecret == null) && ex.matches(".*\\d+sub\\d+$")) {
            String baseEx = ex.replaceAll("\\d+sub\\d+$", "");
            log.debug("[SECRETS] Subaccount {} not found, trying base exchange {}", ex, baseEx);
            
            // Try base exchange trade keys
            if (apiKey == null) apiKey = loadSecret(baseEx + "_trade_key");
            if (apiSecret == null) apiSecret = loadSecret(baseEx + "_trade_secret");
            if (passphrase == null) passphrase = loadSecret(baseEx + "_trade_passphrase");
            
            // Try base exchange generic keys
            if (apiKey == null) apiKey = loadSecret(baseEx + "_api_key");
            if (apiSecret == null) apiSecret = loadSecret(baseEx + "_api_secret");
            if (passphrase == null) passphrase = loadSecret(baseEx + "_passphrase");
        }
        
        return new ApiCredentials(apiKey, apiSecret, passphrase);
    }
}
