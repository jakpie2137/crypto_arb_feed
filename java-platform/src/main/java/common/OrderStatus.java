package common;

public enum OrderStatus {
    NEW,                // Order accepted by Router/Exchange, no fills yet
    PARTIALLY_FILLED,   // Some quantity filled, still open
    FILLED,             // Completely filled
    CANCELED,           // Canceled by user or system
    REJECTED,           // Rejected by exchange (validation error, insufficient funds)
    EXPIRED,            // Time in force expired (or timeout)
    UNKNOWN             // Router doesn't know this ID
}