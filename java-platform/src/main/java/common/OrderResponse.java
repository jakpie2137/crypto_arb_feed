package common;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class OrderResponse {
    private boolean success;
    private String orderId;
    private String errorMessage;
    private boolean retryable;  // true if error is temporary (e.g., insufficient balance), false if permanent (e.g., symbol not in allowlist)

    // Metoda fabryczna sukcesu
    public static OrderResponse success(String orderId) {
        OrderResponse r = new OrderResponse();
        r.success = true;
        r.orderId = orderId;
        r.errorMessage = null;
        r.retryable = false;
        return r;
    }

    // Metoda fabryczna błędu (default: not retryable)
    public static OrderResponse error(String msg) {
        return error(msg, false);
    }
    
    // Metoda fabryczna błędu z flagą retryable
    public static OrderResponse error(String msg, boolean retryable) {
        OrderResponse r = new OrderResponse();
        r.success = false;
        r.orderId = null;
        r.errorMessage = msg;
        r.retryable = retryable;
        return r;
    }

    // [FIX] Alias dla kompatybilności z Routerem, który woła .getMessage()
    public String getMessage() {
        return errorMessage;
    }

    // Alias dla kompatybilności (gdyby router wołał failure zamiast error)
    public static OrderResponse failure(String msg) {
        return error(msg, false);
    }
}