package common;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true) // Safely ignore any extra fields (like "filled" if it still appears)
public class ExecutionReport {
    private String orderId;
    private String exchange;
    private String symbol;
    private String side;

    private OrderStatus status;

    // [FIX] Removed @JsonAlias("filled") because "filled" is a boolean in the JSON, not a number.
    private double filledQty;

    private double avgPrice;
    private String message;
    private long timestamp;
    
    // [E1] Retryable flag - false for hard-blocked errors (max_position, insufficient balance)
    // Default true (most failures are retryable like timeout, rate limit, etc.)
    @Builder.Default
    private boolean retryable = true;

    /**
     * [FIX] Added @JsonIgnore to prevent Jackson from serializing this
     * as a field named "filled": true in the JSON.
     */
    @JsonIgnore
    public boolean isFilled() {
        return status == OrderStatus.FILLED;
    }

    /**
     * [FIX] Added @JsonIgnore to prevent serializing as "closed": true.
     */
    @JsonIgnore
    public boolean isClosed() {
        return status == OrderStatus.FILLED ||
                status == OrderStatus.CANCELED ||
                status == OrderStatus.REJECTED ||
                status == OrderStatus.EXPIRED;
    }
}