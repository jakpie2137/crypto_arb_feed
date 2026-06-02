package feed_filter;

public class FundingInfo {
    private final double fundingRate;
    private final Integer fundingIntervalHours;
    private final Long nextFundingTimeMillis;
    private final Long lastFundingTimeMillis;
    private final long sourceTsMillis;

    public FundingInfo(double fundingRate,
                       Integer fundingIntervalHours,
                       Long nextFundingTimeMillis,
                       Long lastFundingTimeMillis,
                       long sourceTsMillis) {
        this.fundingRate = fundingRate;
        this.fundingIntervalHours = fundingIntervalHours;
        this.nextFundingTimeMillis = nextFundingTimeMillis;
        this.lastFundingTimeMillis = lastFundingTimeMillis;
        this.sourceTsMillis = sourceTsMillis;
    }

    public double getFundingRate() {
        return fundingRate;
    }

    public Integer getFundingIntervalHours() {
        return fundingIntervalHours;
    }

    public Long getNextFundingTimeMillis() {
        return nextFundingTimeMillis;
    }

    public Long getLastFundingTimeMillis() {
        return lastFundingTimeMillis;
    }

    public long getSourceTsMillis() {
        return sourceTsMillis;
    }
}
