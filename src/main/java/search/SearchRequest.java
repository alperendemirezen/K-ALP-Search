package search;

import java.util.Map;

public class SearchRequest {
    private String topic;
    private Map<String, String> filters;
    private String mode;
    private Integer lastN;
    private String kafkaAddress;

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

}