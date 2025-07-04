package search;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

public class SearchContext {
    public String topic;
    public String mode;
    public int maxResults;
    public Integer lastN;
    public String requestId;
    public List<Future<List<String>>> futures;
    public Properties props;
    public KafkaConsumer<String, String> consumer;
    public List<TopicPartition> partitions;
    public Map<TopicPartition, Long> beginningOffsets;
    public Map<TopicPartition, Long> endOffsets;
}
