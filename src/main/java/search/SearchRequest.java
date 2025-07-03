package search;

import java.util.List;
import java.util.Map;

public class SearchRequest {
    private String topic;
    private Map<String, String> filters;
    private List<String> rawFilters; // <- String bazlı filtreler
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


    // Getters & Setters
    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Map<String, String> getFilters() {
        return filters;
    }

    public void setFilters(Map<String, String> filters) {
        this.filters = filters;
    }

    public List<String> getRawFilters() {
        return rawFilters;
    }

    public void setRawFilters(List<String> rawFilters) {
        this.rawFilters = rawFilters;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public Integer getLastN() {
        return lastN;
    }

    public void setLastN(Integer lastN) {
        this.lastN = lastN;
    }

    public String getKafkaAddress() {
        return kafkaAddress;
    }

    public void setKafkaAddress(String kafkaAddress) {
        this.kafkaAddress = kafkaAddress;
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public Long getStartOffset() {
        return startOffset;
    }

    public void setStartOffset(Long startOffset) {
        this.startOffset = startOffset;
    }

    public Long getEndOffset() {
        return endOffset;
    }

    public void setEndOffset(Long endOffset) {
        this.endOffset = endOffset;
    }

    public String getTargetTopic() {
        return targetTopic;
    }

    public void setTargetTopic(String targetTopic) {
        this.targetTopic = targetTopic;
    }

    public String getFilterMode() {
        return filterMode;
    }
    public void setFilterMode(String filterMode) {
        this.filterMode = filterMode;
    }
    public Integer getPartition() {
        return partition;
    }
    public void setPartition(Integer partition) {
        this.partition = partition;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getDateKey() { return dateKey; }
    public void setDateKey(String dateKey) { this.dateKey = dateKey; }
}
