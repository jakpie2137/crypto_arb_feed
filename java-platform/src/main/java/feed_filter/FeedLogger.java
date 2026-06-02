package feed_filter;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * Centralized logger with timestamps.
 * Format: "2025-11-29 15:30:01.156 [INFO] [TAG] Message"
 */
public class FeedLogger {
    private static final SimpleDateFormat FMT;

    static {
        FMT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        // Using default timezone as requested, or switch to "UTC" if preferred.
        FMT.setTimeZone(TimeZone.getDefault());
    }

    public static void info(String tag, String message) {
        print("INFO", tag, message);
    }

    public static void warn(String tag, String message) {
        print("WARN", tag, message);
    }

    public static void error(String tag, String message) {
        print("ERROR", tag, message);
    }

    public static void error(String tag, String message, Throwable t) {
        String cause = (t != null) ? t.toString() : "null";
        print("ERROR", tag, message + " | " + cause);
    }

    private static synchronized void print(String level, String tag, String msg) {
        String ts = FMT.format(new Date());
        System.out.printf("%s [%s] [%s] %s%n", ts, level, tag, msg);
    }
}