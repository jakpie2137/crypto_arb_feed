package common;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Execution policy parameters for order execution.
 * Contains retry logic, timeout, and fallback configuration.
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ExecutionPolicy {
    /**
     * Order type: "LIMIT" or "MARKET"
     */
    private String type; // "LIMIT" or "MARKET"

    /**
     * Maximum time to wait for order to be filled (ms).
     * After this time, order will be canceled and retried.
     */
    private Long maxWaitOrderFilledMs;

    /**
     * Maximum number of retry attempts (legacy - for backward compatibility).
     * If limitRetryParams or marketRetryParams are set, they take precedence.
     */
    private Integer maxRetries;

    /**
     * Retry frequency - how often to check status or retry (ms) (legacy - for backward compatibility).
     * If limitRetryParams or marketRetryParams are set, they take precedence.
     */
    private Long retryFreqMs;
    
    /**
     * Retry parameters for LIMIT (IOC) orders.
     * If null, falls back to maxRetries/retryFreqMs.
     */
    private RetryParams limitRetryParams;
    
    /**
     * Retry parameters for MARKET orders.
     * If null, falls back to maxRetries/retryFreqMs.
     */
    private RetryParams marketRetryParams;
    
    /**
     * Delay before IOC status check (ms). If null, executor uses env default.
     */
    private Long iocStatusCheckDelayMs;

    /**
     * Whether to fallback to MARKET order after LIMIT retries exhausted.
     */
    private Boolean fallbackToMarket;
    
    /**
     * Retry parameters (count and interval).
     */
    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class RetryParams {
        private Integer maxRetries;
        private Long retryFreqMs;
    }

    /**
     * Timeout for LIMIT orders (ms).
     * If order is not filled within this time, it will be canceled.
     */
    private Long limitTimeoutMs;
}
