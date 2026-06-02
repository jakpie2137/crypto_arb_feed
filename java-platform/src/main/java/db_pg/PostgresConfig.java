package db_pg;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

import java.nio.file.Files;
import java.nio.file.Path;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PostgresConfig {

    // Docker secrets directory (standard location)
    private static final String DOCKER_SECRETS_DIR = "/run/secrets";
    
    // Local secrets directory (same logical path on Linux and Windows)
    // Linux:   /opt/trading/secrets/
    // Windows: C:\opt\trading\secrets\
    private static final String LOCAL_SECRETS_DIR = resolveLocalSecretsDir();

    // Public fields for backward compatibility with existing code
    public String url;
    public String user;
    public String password;

    /**
     * Creates configuration from files or ENV variables.
     * 
     * Priority:
     * 1. Docker secrets (/run/secrets/{name}) - for containerized apps
     * 2. Local secrets dir (/opt/trading/secrets/{name}.txt) - for local dev
     * 3. ENV variable (ARB_PG_XXX)
     * 4. Default value
     * 
     * Docker secrets (mounted by Docker):
     * - pg_password   -> database password
     * - pg_url        -> database URL (optional)
     * - pg_user       -> database user (optional)
     * 
     * Local secrets files (in /opt/trading/secrets/):
     * - pg_password.txt
     * - pg_url.txt (optional)
     * - pg_user.txt (optional)
     */
    public static PostgresConfig fromEnvOrDefaults() {
        System.out.println("[DB CONFIG] Docker secrets dir: " + DOCKER_SECRETS_DIR);
        System.out.println("[DB CONFIG] Local secrets dir: " + LOCAL_SECRETS_DIR);
        
        String url = getSecret("ARB_PG_URL", "pg_url", "jdbc:postgresql://localhost:2137/arb_test");
        String user = getSecret("ARB_PG_USER", "pg_user", "postgres");
        String pass = getSecret("ARB_PG_PASS", "pg_password", "password");
        
        // Log configuration (without password)
        System.out.println("[DB CONFIG] URL: " + url);
        System.out.println("[DB CONFIG] User: " + user);
        System.out.println("[DB CONFIG] Password: " + (pass.equals("password") ? "(default)" : "(from secret)"));
        
        return new PostgresConfig(url, user, pass);
    }

    /**
     * Resolves local secrets directory based on OS.
     * Can be overridden with SECRETS_DIR env variable.
     */
    private static String resolveLocalSecretsDir() {
        String customDir = System.getenv("SECRETS_DIR");
        if (customDir != null && !customDir.isBlank()) {
            return customDir;
        }
        
        boolean isWindows = System.getProperty("os.name", "").toLowerCase().contains("win");
        return isWindows ? "C:\\opt\\trading\\secrets" : "/opt/trading/secrets";
    }

    /**
     * Gets secret value with fallback chain:
     * 1. Docker secret (/run/secrets/{secretName})
     * 2. Local secret file (/opt/trading/secrets/{secretName}.txt)
     * 3. ENV variable
     * 4. Default value
     */
    private static String getSecret(String envKey, String secretName, String defaultValue) {
        // 1. Check Docker secrets (/run/secrets/pg_password)
        String dockerSecretPath = DOCKER_SECRETS_DIR + "/" + secretName;
        String value = readSecretFile(dockerSecretPath);
        if (value != null) {
            System.out.println("[DB CONFIG] " + envKey + " loaded from Docker secret: " + dockerSecretPath);
            return value;
        }
        
        // 2. Check local secrets dir (/opt/trading/secrets/pg_password.txt)
        String sep = LOCAL_SECRETS_DIR.contains("\\") ? "\\" : "/";
        String localSecretPath = LOCAL_SECRETS_DIR + sep + secretName + ".txt";
        value = readSecretFile(localSecretPath);
        if (value != null) {
            System.out.println("[DB CONFIG] " + envKey + " loaded from local file: " + localSecretPath);
            return value;
        }
        
        // 3. Check ENV variable
        String envValue = System.getenv(envKey);
        if (envValue != null && !envValue.isBlank()) {
            System.out.println("[DB CONFIG] " + envKey + " loaded from ENV");
            return envValue;
        }
        
        // 4. Return default
        System.out.println("[DB CONFIG] " + envKey + " using default value");
        return defaultValue;
    }

    /**
     * Reads secret from file, returns null if file doesn't exist or can't be read.
     */
    private static String readSecretFile(String filePath) {
        try {
            Path path = Path.of(filePath);
            if (Files.exists(path)) {
                return Files.readString(path).trim();
            }
        } catch (Exception e) {
            // Silent fail - file doesn't exist or can't be read
        }
        return null;
    }
    
    /**
     * Creates a simple DataSource for this configuration.
     * Uses PGSimpleDataSource from PostgreSQL driver.
     */
    public javax.sql.DataSource getDataSource() {
        try {
            org.postgresql.ds.PGSimpleDataSource ds = new org.postgresql.ds.PGSimpleDataSource();
            ds.setUrl(url);
            ds.setUser(user);
            ds.setPassword(password);
            return ds;
        } catch (Exception e) {
            System.err.println("[DB CONFIG] Failed to create DataSource: " + e.getMessage());
            return null;
        }
    }
}
