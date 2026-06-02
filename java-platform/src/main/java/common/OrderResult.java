package common;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Represents the result of an order submission attempt.
 * Provides clear information about what happened with the order.
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class OrderResult {
    
    public enum Status {
        ACCEPTED,           // Order accepted by exchange, orderId available
        REJECTED,           // Order rejected (pre-flight validation, exchange rejection)
        PENDING_RETRY,      // Order failed but retry is scheduled
        PENDING_FALLBACK,   // Order failed, fallback to MARKET is scheduled
        ERROR               // Unexpected error during processing
    }
    
    private Status status;
    private String orderId;         // Only set if status == ACCEPTED
    private String rejectReason;    // Only set if status == REJECTED or ERROR
    private boolean retryable;      // If rejected, can it be retried?
    private int retryAttempt;       // Current retry attempt (0 = first attempt)
    private int maxRetries;         // Max retries from ExecutionPolicy
    
    // Factory methods for common cases
    public static OrderResult accepted(String orderId) {
        return OrderResult.builder()
                .status(Status.ACCEPTED)
                .orderId(orderId)
                .retryable(false)
                .build();
    }
    
    public static OrderResult rejected(String reason, boolean retryable) {
        return OrderResult.builder()
                .status(Status.REJECTED)
                .rejectReason(reason)
                .retryable(retryable)
                .build();
    }
    
    public static OrderResult pendingRetry(String reason, int attempt, int maxRetries) {
        return OrderResult.builder()
                .status(Status.PENDING_RETRY)
                .rejectReason(reason)
                .retryable(true)
                .retryAttempt(attempt)
                .maxRetries(maxRetries)
                .build();
    }
    
    public static OrderResult pendingFallback(String reason, int attempts) {
        return OrderResult.builder()
                .status(Status.PENDING_FALLBACK)
                .rejectReason(reason)
                .retryable(false)
                .retryAttempt(attempts)
                .build();
    }
    
    public static OrderResult error(String reason) {
        return OrderResult.builder()
                .status(Status.ERROR)
                .rejectReason(reason)
                .retryable(false)
                .build();
    }
    
    /**
     * Returns true if the order was accepted (has orderId)
     */
    public boolean isAccepted() {
        return status == Status.ACCEPTED && orderId != null;
    }
    
    /**
     * Returns true if the order was rejected and cannot be retried
     */
    public boolean isFinalRejection() {
        return status == Status.REJECTED && !retryable;
    }
    
    /**
     * Returns true if retry or fallback is in progress
     */
    public boolean isPending() {
        return status == Status.PENDING_RETRY || status == Status.PENDING_FALLBACK;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("OrderResult{status=").append(status);
        if (orderId != null) {
            sb.append(", orderId=").append(orderId);
        }
        if (rejectReason != null) {
            sb.append(", reason=").append(rejectReason);
        }
        if (retryable) {
            sb.append(", retryable=true");
        }
        if (retryAttempt > 0) {
            sb.append(", attempt=").append(retryAttempt).append("/").append(maxRetries);
        }
        sb.append("}");
        return sb.toString();
    }
}
