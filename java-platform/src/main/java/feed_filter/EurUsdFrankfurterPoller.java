package feed_filter;

import okhttp3.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * EUR/USD reference poller.
 *
 * Uses Frankfurter (ECB-based reference rates).
 * Example response: https://api.frankfurter.dev/v1/latest?base=EUR&symbols=USD
 */
public class EurUsdFrankfurterPoller {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final OkHttpClient http;
    private final ScheduledExecutorService ses;

    private final long periodMs;

    private final AtomicBoolean started = new AtomicBoolean(false);

    public EurUsdFrankfurterPoller() {
        this.periodMs = Long.parseLong(System.getenv().getOrDefault("FX_EURUSD_POLL_MS", "60000")); // 5 min
        this.http = new OkHttpClient.Builder()
                .callTimeout(java.time.Duration.ofSeconds(10))
                .build();
        this.ses = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "fx-eurusd-poller");
            t.setDaemon(true);
            return t;
        });
    }

    public void start() {
        if (!started.compareAndSet(false, true)) return;
        // run immediately, then periodically
        ses.scheduleAtFixedRate(this::pollOnceSafe, 0, periodMs, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        ses.shutdownNow();
    }

    private void pollOnceSafe() {
        try {
            pollOnce();
        } catch (Exception e) {
            System.err.println("EURUSD poll error: " + e.getMessage());
        }
    }

    private void pollOnce() throws IOException {
        String url = System.getenv().getOrDefault(
                "FX_EURUSD_URL",
                "https://api.frankfurter.dev/v1/latest?base=EUR&symbols=USD"
        );

        Request req = new Request.Builder().url(url).get().build();
        try (Response resp = http.newCall(req).execute()) {
            if (!resp.isSuccessful() || resp.body() == null) {
                throw new IOException("HTTP " + resp.code());
            }
            String body = resp.body().string();
            JsonNode root = MAPPER.readTree(body);
            double eurUsd = root.path("rates").path("USD").asDouble(Double.NaN);
            if (Double.isFinite(eurUsd) && eurUsd > 0) {
                FxRates.updateEurUsdRef(eurUsd, System.currentTimeMillis(), "FRANKFURTER");
            } else {
                throw new IOException("Bad EURUSD in response");
            }
        }
    }
}