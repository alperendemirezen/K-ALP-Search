package search;

import java.util.List;
import java.util.Map;

public class SearchRequest {
    private String topic;
    private Map<String, String> filters;
    private List<String> rawFilters;
    private String mode;
    private Integer lastN;
    private String kafkaAddress;
    private String requestId;
    private Long startOffset;
    private Long endOffset;
    private String targetTopic;
    private String filterMode;
    private Integer partition;
    private String date;
    private String dateKey;
    private String targetKafka;
    private int threads;
    private int pollRecords;
    private int timeoutMs;

    public String getTopic() {
        return topic;
    }

    public Map<String, String> getFilters() {
        return filters;
    }

    public List<String> getRawFilters() {
        return rawFilters;
    }

    public String getMode() {
        return mode;
    }

    public Integer getLastN() {
        return lastN;
    }

    public String getKafkaAddress() {
        return kafkaAddress;
    }

    public String getRequestId() {
        return requestId;
    }

    public Long getStartOffset() {
        return startOffset;
    }

    public Long getEndOffset() {
        return endOffset;
    }

    public String getTargetTopic() {
        return targetTopic;
    }

    public String getFilterMode() {
        return filterMode;
    }

    public Integer getPartition() {
        return partition;
    }

    public String getDate() {
        return date;
    }

    public String getDateKey() {
        return dateKey;
    }

    public String getTargetKafka() {
        return targetKafka;
    }

    public int getThreads() {
        return threads;
    }

    public int getPollRecords() {
        return pollRecords;
    }

    public int getTimeoutMs() {
        return timeoutMs;
    }






}


