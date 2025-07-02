package search;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.springframework.web.bind.annotation.*;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

@CrossOrigin(origins = "*")
@RestController
@RequestMapping("/search")
public class KafkaSearchController {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Map<String, List<Future<List<String>>>> activeRequests = new ConcurrentHashMap<>();

    @GetMapping("/topics")
    public List<String> getAllTopics(@RequestParam String kafkaAddress) {
        Properties props = getKafkaProps(kafkaAddress);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            return new ArrayList<>(consumer.listTopics().keySet());
        } catch (Exception e) {
            e.printStackTrace();
            return Collections.emptyList();
        }
    }

    @PostMapping
    public List<String> search(@RequestBody SearchRequest request) throws InterruptedException, ExecutionException {
        String topic = request.getTopic();
        Map<String, String> filters = request.getFilters();
        String mode = request.getMode();
        Integer lastN = request.getLastN();
        int maxResults = "last".equalsIgnoreCase(mode) ? 1 : Integer.MAX_VALUE;

        String requestId = request.getRequestId();

        List<Future<List<String>>> futures = new ArrayList<>();
        activeRequests.put(requestId, futures);

        Properties props = getKafkaProps(request.getKafkaAddress());

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
            if (partitionInfos == null || partitionInfos.isEmpty()) {
                return Collections.singletonList("Topic not found.");
            }

            List<TopicPartition> partitions = new ArrayList<>();
            for (PartitionInfo p : partitionInfos) {
                partitions.add(new TopicPartition(p.topic(), p.partition()));
            }

            consumer.assign(partitions);
            Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(partitions);
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);

            if ("last".equalsIgnoreCase(mode)) {
                return searchLastRecord(topic, partitions, props, filters);
            }

            ExecutorService executor = Executors.newFixedThreadPool(4);

            for (TopicPartition tp : partitions) {
                long start = beginningOffsets.get(tp);
                long end = endOffsets.get(tp);
                if (end < start) continue;

                if ("lastN".equalsIgnoreCase(mode) && lastN != null) {
                    start = Math.max(end - lastN + 1, start);
                }

                long rangeSize = Math.max(1, (end - start + 1) / 4);

                for (int i = 0; i < 4; i++) {
                    long partStart = start + i * rangeSize;
                    long partEnd = (i == 3) ? end : Math.min(partStart + rangeSize - 1, end);

                    Callable<List<String>> task = () -> consumePartitionRange(
                            topic, tp.partition(), partStart, partEnd, props, filters, maxResults
                    );
                    futures.add(executor.submit(task));
                }
            }

            List<String> results = new ArrayList<>();
            for (Future<List<String>> f : futures) {
                List<String> partial = f.get();
                Collections.reverse(partial);
                results.addAll(0, partial);
                if (results.size() >= maxResults) break;
            }
            executor.shutdownNow();
            return results.size() > maxResults ? results.subList(0, maxResults) : results;
        }
    }

    @PostMapping("/simple-string-search")
    public List<String> simpleStringSearch(@RequestBody SearchRequest request) throws InterruptedException, ExecutionException {
        String topic = request.getTopic();
        List<String> rawFilters = request.getRawFilters();
        String mode = request.getMode();
        Integer lastN = request.getLastN();
        int maxResults = "last".equalsIgnoreCase(mode) ? 1 : Integer.MAX_VALUE;

        String requestId = request.getRequestId();
        if (requestId == null || requestId.isEmpty()) {
            requestId = UUID.randomUUID().toString();
        }

        List<Future<List<String>>> futures = new ArrayList<>();
        activeRequests.put(requestId, futures);

        Properties props = getKafkaProps(request.getKafkaAddress());

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
            if (partitionInfos == null || partitionInfos.isEmpty()) {
                return Collections.singletonList("Topic not found.");
            }

            List<TopicPartition> partitions = new ArrayList<>();
            for (PartitionInfo p : partitionInfos) {
                partitions.add(new TopicPartition(p.topic(), p.partition()));
            }

            consumer.assign(partitions);
            Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(partitions);
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);

            ExecutorService executor = Executors.newFixedThreadPool(4);

            for (TopicPartition tp : partitions) {
                long start = beginningOffsets.get(tp);
                long end = endOffsets.get(tp);
                if (end < start) continue;

                if ("lastN".equalsIgnoreCase(mode) && lastN != null) {
                    start = Math.max(end - lastN + 1, start);
                }

                long rangeSize = Math.max(1, (end - start + 1) / 4);

                for (int i = 0; i < 4; i++) {
                    long partStart = start + i * rangeSize;
                    long partEnd = (i == 3) ? end : Math.min(partStart + rangeSize - 1, end);

                    Callable<List<String>> task = () -> consumePartitionRangeSimpleString(
                            topic, tp.partition(), partStart, partEnd, props, rawFilters, maxResults
                    );
                    futures.add(executor.submit(task));
                }
            }

            List<String> results = new ArrayList<>();
            for (Future<List<String>> f : futures) {
                results.addAll(f.get());
                if (results.size() >= maxResults) break;
            }
            executor.shutdownNow();
            return results.size() > maxResults ? results.subList(0, maxResults) : results;
        }
    }

    @PostMapping("/cancel")
    public void cancelSearch(@RequestBody Map<String, String> payload) {
        String requestId = payload.get("requestId");
        List<Future<List<String>>> futures = activeRequests.get(requestId);
        if (futures != null) {
            for (Future<List<String>> future : futures) {
                future.cancel(true);
            }
            activeRequests.remove(requestId);
            System.out.println("Cancelled request: " + requestId);
        }
    }

    private boolean matchesSimpleStringFilters(String value, List<String> rawFilters) {
        if (rawFilters == null || rawFilters.isEmpty()) return true;
        if (value == null) return false;

        String lowerValue = value.toLowerCase();

        for (String filter : rawFilters) {
            if (!lowerValue.contains(filter.trim().toLowerCase())) {
                return false;
            }
        }
        return true;
    }

    private List<String> consumePartitionRangeSimpleString(String topic, int partition, long startOffset, long endOffset,
                                                           Properties props, List<String> rawFilters, int maxResults) {
        List<String> foundRecords = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            TopicPartition tp = new TopicPartition(topic, partition);
            consumer.assign(List.of(tp));
            consumer.seek(tp, startOffset);

            long currentOffset = startOffset;

            while (currentOffset <= endOffset && foundRecords.size() < maxResults) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                if (records.isEmpty()) break;

                for (ConsumerRecord<String, String> record : records) {
                    if (record.offset() > endOffset) break;
                    if (matchesSimpleStringFilters(record.value(), rawFilters)) {
                        foundRecords.add(String.format("{\"offset\": %d, \"value\": \"%s\"}", record.offset(), record.value()));
                        if (foundRecords.size() >= maxResults) break;
                    }
                    currentOffset = record.offset() + 1;
                }
            }
        }
        return foundRecords;
    }

    private List<String> searchLastRecord(String topic,
                                          List<TopicPartition> partitions,
                                          Properties props,
                                          Map<String, String> filters) {
        int stepSize = 50_000;
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
            Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(partitions);

            for (TopicPartition tp : partitions) {
                long start = beginningOffsets.get(tp);
                long end = endOffsets.get(tp);
                long fromOffset = end - 1;

                while (fromOffset >= start) {
                    long batchStart = Math.max(start, fromOffset - stepSize + 1);

                    consumer.assign(Collections.singletonList(tp));
                    consumer.seek(tp, batchStart);

                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                    List<ConsumerRecord<String, String>> recordList = new ArrayList<>(records.records(tp));
                    Collections.reverse(recordList);

                    for (ConsumerRecord<String, String> record : recordList) {
                        if (matchesFilters(record.value(), filters)) {
                            try {
                                JsonNode valueNode = objectMapper.readTree(record.value());
                                ObjectNode resultNode = objectMapper.createObjectNode();
                                resultNode.put("offset", record.offset());
                                resultNode.set("value", valueNode);
                                return List.of(objectMapper.writeValueAsString(resultNode));
                            } catch (Exception e) {
                                return List.of(String.format("{\"offset\": %d, \"value\": \"%s\"}", record.offset(), record.value()));
                            }
                        }
                    }
                    fromOffset = batchStart - 1;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return List.of();
    }

    private List<String> consumePartitionRange(String topic, int partition, long startOffset, long endOffset,
                                               Properties props, Map<String, String> filters, int maxResults) {
        List<String> foundRecords = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            TopicPartition tp = new TopicPartition(topic, partition);
            consumer.assign(List.of(tp));
            consumer.seek(tp, startOffset);

            long currentOffset = startOffset;
            long blockSize = 50_000;

            while (currentOffset <= endOffset && foundRecords.size() < maxResults) {
                long remaining = endOffset - currentOffset + 1;
                long batchSize = Math.min(blockSize, remaining);
                long blockEnd = currentOffset + batchSize - 1;

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                if (records.isEmpty()) break;

                for (ConsumerRecord<String, String> record : records) {
                    if (record.offset() > blockEnd) break;
                    if (matchesFilters(record.value(), filters)) {
                        try {
                            JsonNode valueNode = objectMapper.readTree(record.value());
                            ObjectNode resultNode = objectMapper.createObjectNode();
                            resultNode.put("offset", record.offset());
                            resultNode.set("value", valueNode);
                            foundRecords.add(objectMapper.writeValueAsString(resultNode));
                        } catch (Exception e) {
                            foundRecords.add(0, String.format("{\"offset\": %d, \"value\": \"%s\"}", record.offset(), record.value()));
                        }
                        if (foundRecords.size() >= maxResults) break;
                    }
                    currentOffset = record.offset() + 1;
                }
            }
        }
        return foundRecords;
    }

    private boolean matchesFilters(String json, Map<String, String> filters) {
        if (filters == null || filters.isEmpty()) return true;
        try {
            JsonNode node = objectMapper.readTree(json);
            for (Map.Entry<String, String> entry : filters.entrySet()) {
                JsonNode val = node.get(entry.getKey());
                if (val == null || !val.asText().equals(entry.getValue())) {
                    return false;
                }
            }
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private Properties getKafkaProps(String bootstrapServers) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", "web-search-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");
        return props;
    }
}