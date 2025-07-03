package search;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.springframework.web.bind.annotation.*;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@CrossOrigin(origins = "*")
@RestController
@RequestMapping("/search")
public class KafkaSearchController {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Map<String, List<Future<List<String>>>> activeRequests = new ConcurrentHashMap<>();

    private Properties getKafkaProps(String bootstrapServers) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", "web-search-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");
        return props;
    }

    private Properties getKafkaProducerProps(String kafkaAddress) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaAddress);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }

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
    @GetMapping("/topic-offsets")
    public Map<String, Object> getOffsetsAndPartitions(@RequestParam String kafkaAddress, @RequestParam String topic) {
        Properties props = getKafkaProps(kafkaAddress);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            List<PartitionInfo> partitions = consumer.partitionsFor(topic);
            if (partitions == null || partitions.isEmpty()) {
                return Map.of("startOffset", 0L, "endOffset", 0L, "partitions", List.of(0));
            }

            List<TopicPartition> topicPartitions = new ArrayList<>();
            List<Integer> partitionNumbers = new ArrayList<>();
            for (PartitionInfo p : partitions) {
                topicPartitions.add(new TopicPartition(p.topic(), p.partition()));
                partitionNumbers.add(p.partition());
            }

            Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(topicPartitions);
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(topicPartitions);

            long min = beginningOffsets.values().stream().min(Long::compareTo).orElse(0L);
            long max = endOffsets.values().stream().max(Long::compareTo).orElse(0L);

            Map<String, Object> response = new HashMap<>();
            response.put("startOffset", min);
            response.put("endOffset", max - 1);
            response.put("partitions", partitionNumbers);
            return response;
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

    @PostMapping
    public List<String> JSONSearch(@RequestBody SearchRequest request) throws InterruptedException, ExecutionException {
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
                return searchLastRecordJSON(topic, partitions, props, filters);
            }
            if ("copy".equalsIgnoreCase(mode)) {
                return copyMode(request);
            }
            if ("firstByDate".equalsIgnoreCase(request.getMode())) {
                return DateMode(request);
            }

            ExecutorService executor = Executors.newFixedThreadPool(4);

            for (TopicPartition tp : partitions) {
                long start = beginningOffsets.get(tp);
                long end = endOffsets.get(tp);
                if (end < start) continue;

                if ("offsetRange".equalsIgnoreCase(mode) && request.getStartOffset() != null && request.getEndOffset() != null) {
                    start = Math.max(start, request.getStartOffset());
                    end = Math.min(end, request.getEndOffset());
                }

                if ("lastN".equalsIgnoreCase(mode) && lastN != null) {
                    start = Math.max(end - lastN + 1, start);
                }

                long rangeSize = Math.max(1, (end - start + 1) / 4);

                for (int i = 0; i < 4; i++) {
                    long partStart = start + i * rangeSize;
                    long partEnd = (i == 3) ? end : Math.min(partStart + rangeSize - 1, end);

                    Callable<List<String>> task = () -> consumePartitionRangeJSON(
                            topic, tp.partition(), partStart, partEnd, props, filters, maxResults
                    );
                    futures.add(executor.submit(task));
                }
            }

            List<String> results = new ArrayList<>();
            for (Future<List<String>> f : futures) {
                List<String> partial = f.get();
                results.addAll(partial);
                if (results.size() >= maxResults) break;
            }
            Collections.reverse(results);
            executor.shutdownNow();
            return results.size() > maxResults ? results.subList(0, maxResults) : results;
        }
    }

    @PostMapping("/simple-string-search")
    public List<String> stringSearch(@RequestBody SearchRequest request) throws InterruptedException, ExecutionException {
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

            if ("last".equalsIgnoreCase(mode)) {
                return searchLastRecordString(topic, partitions, props, rawFilters);
            }
            if ("copy".equalsIgnoreCase(mode)) {
                return copyMode(request);
            }

            consumer.assign(partitions);
            Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(partitions);
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);

            ExecutorService executor = Executors.newFixedThreadPool(4);

            for (TopicPartition tp : partitions) {
                long start = beginningOffsets.get(tp);
                long end = endOffsets.get(tp);
                if (end < start) continue;

                if ("offsetRange".equalsIgnoreCase(mode) && request.getStartOffset() != null && request.getEndOffset() != null) {
                    start = Math.max(start, request.getStartOffset());
                    end = Math.min(end, request.getEndOffset());
                }

                if ("lastN".equalsIgnoreCase(mode) && lastN != null) {
                    start = Math.max(end - lastN + 1, start);
                }



                long rangeSize = Math.max(1, (end - start + 1) / 4);

                for (int i = 0; i < 4; i++) {
                    long partStart = start + i * rangeSize;
                    long partEnd = (i == 3) ? end : Math.min(partStart + rangeSize - 1, end);

                    Callable<List<String>> task = () -> consumePartitionRangeString(
                            topic, tp.partition(), partStart, partEnd, props, rawFilters, maxResults
                    );
                    futures.add(executor.submit(task));
                }
            }

            List<String> results = new ArrayList<>();
            for (Future<List<String>> f : futures) {
                List<String> partial = f.get();
                results.addAll(partial);
                if (results.size() >= maxResults) break;
            }
            executor.shutdownNow();
            Collections.reverse(results);
            return results.size() > maxResults ? results.subList(0, maxResults) : results;
        }
    }

    @PostMapping("/pattern-search")
    public List<String> patternSearch(@RequestBody SearchRequest request) throws InterruptedException, ExecutionException {
        String topic = request.getTopic();
        List<String> patternStrings = request.getRawFilters();
        String mode = request.getMode();
        Integer lastN = request.getLastN();
        int maxResults = "last".equalsIgnoreCase(mode) ? 1 : Integer.MAX_VALUE;

        String requestId = request.getRequestId();

        List<Future<List<String>>> futures = new ArrayList<>();
        activeRequests.put(requestId, futures);

        Properties props = getKafkaProps(request.getKafkaAddress());


        List<Pattern> patterns = new ArrayList<>();
        for (String p : patternStrings) {
            patterns.add(Pattern.compile(p, Pattern.CASE_INSENSITIVE));
        }

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
            if (partitionInfos == null || partitionInfos.isEmpty()) {
                return Collections.singletonList("Topic not found.");
            }

            List<TopicPartition> partitions = new ArrayList<>();
            for (PartitionInfo p : partitionInfos) {
                partitions.add(new TopicPartition(p.topic(), p.partition()));
            }

            if ("last".equalsIgnoreCase(mode)) {
                return searchLastRecordPattern(topic, partitions, props, patterns);
            }

            consumer.assign(partitions);
            Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(partitions);
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);

            ExecutorService executor = Executors.newFixedThreadPool(4);

            for (TopicPartition tp : partitions) {
                long start = beginningOffsets.get(tp);
                long end = endOffsets.get(tp);
                if (end < start) continue;

                if ("offsetRange".equalsIgnoreCase(mode) && request.getStartOffset() != null && request.getEndOffset() != null) {
                    start = Math.max(start, request.getStartOffset());
                    end = Math.min(end, request.getEndOffset());
                }

                if ("lastN".equalsIgnoreCase(mode) && lastN != null) {
                    start = Math.max(end - lastN + 1, start);
                }

                long rangeSize = Math.max(1, (end - start + 1) / 4);

                for (int i = 0; i < 4; i++) {
                    long partStart = start + i * rangeSize;
                    long partEnd = (i == 3) ? end : Math.min(partStart + rangeSize - 1, end);

                    final List<Pattern> finalPatterns = patterns;
                    Callable<List<String>> task = () -> consumePartitionRangePattern(
                            topic, tp.partition(), partStart, partEnd, props, finalPatterns, maxResults
                    );
                    futures.add(executor.submit(task));
                }
            }

            List<String> results = new ArrayList<>();
            for (Future<List<String>> f : futures) {
                List<String> partial = f.get();
                results.addAll(partial);
                if (results.size() >= maxResults) break;
            }
            executor.shutdownNow();
            Collections.reverse(results);
            return results.size() > maxResults ? results.subList(0, maxResults) : results;
        }
    }


    private List<String> searchLastRecordJSON(String topic,
                                              List<TopicPartition> partitions,
                                              Properties props,
                                              Map<String, String> filters) {
        int stepSize = 500;

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
            Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(partitions);

            for (TopicPartition tp : partitions) {
                long start = beginningOffsets.get(tp);
                long end = endOffsets.get(tp);
                long fromOffset = end;

                while (fromOffset > start) {
                    long batchStart = Math.max(start, fromOffset - stepSize);

                    consumer.assign(Collections.singletonList(tp));
                    consumer.seek(tp, batchStart);

                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                    List<ConsumerRecord<String, String>> matchedRecords = new ArrayList<>();

                    for (ConsumerRecord<String, String> record : records.records(tp)) {

                        if (record.offset() >= end) continue;
                        if (matchesFiltersJSON(record.value(), filters)) {
                            matchedRecords.add(record);
                        }
                    }

                    if (!matchedRecords.isEmpty()) {
                        ConsumerRecord<String, String> maxOffsetRecord = Collections.max(
                                matchedRecords,
                                Comparator.comparingLong(ConsumerRecord::offset)
                        );
                        try {
                            JsonNode valueNode = objectMapper.readTree(maxOffsetRecord.value());
                            ObjectNode resultNode = objectMapper.createObjectNode();
                            resultNode.put("offset", maxOffsetRecord.offset());
                            resultNode.set("value", valueNode);
                            return List.of(objectMapper.writeValueAsString(resultNode));
                        } catch (Exception e) {
                            return List.of(String.format("{\"offset\": %d, \"value\": \"%s\"}",
                                    maxOffsetRecord.offset(), maxOffsetRecord.value()));
                        }
                    }

                    fromOffset = batchStart; // Geriye git
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return List.of();
    }

    private List<String> searchLastRecordString(String topic,
                                                List<TopicPartition> partitions,
                                                Properties props,
                                                List<String> rawFilters) {
        int stepSize = 500;

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
            Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(partitions);

            for (TopicPartition tp : partitions) {
                long start = beginningOffsets.get(tp);
                long end = endOffsets.get(tp);
                long fromOffset = end;

                while (fromOffset > start) {
                    long batchStart = Math.max(start, fromOffset - stepSize);

                    consumer.assign(Collections.singletonList(tp));
                    consumer.seek(tp, batchStart);

                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                    List<ConsumerRecord<String, String>> matchedRecords = new ArrayList<>();

                    for (ConsumerRecord<String, String> record : records.records(tp)) {
                        if (record.offset() >= end) continue;
                        if (matchesFiltersString(record.value(), rawFilters)) {
                            matchedRecords.add(record);
                        }
                    }

                    if (!matchedRecords.isEmpty()) {
                        ConsumerRecord<String, String> maxOffsetRecord = Collections.max(
                                matchedRecords,
                                Comparator.comparingLong(ConsumerRecord::offset)
                        );
                        return List.of(String.format("{\"offset\": %d, \"value\": \"%s\"}",
                                maxOffsetRecord.offset(), maxOffsetRecord.value()));
                    }

                    fromOffset = batchStart;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return List.of();
    }

    private List<String> searchLastRecordPattern(String topic,
                                                 List<TopicPartition> partitions,
                                                 Properties props,
                                                 List<Pattern> patterns) {
        int stepSize = 500;

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
            Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(partitions);

            for (TopicPartition tp : partitions) {
                long start = beginningOffsets.get(tp);
                long end = endOffsets.get(tp);
                long fromOffset = end;

                while (fromOffset > start) {
                    long batchStart = Math.max(start, fromOffset - stepSize);

                    consumer.assign(Collections.singletonList(tp));
                    consumer.seek(tp, batchStart);

                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                    List<ConsumerRecord<String, String>> matchedRecords = new ArrayList<>();

                    for (ConsumerRecord<String, String> record : records.records(tp)) {
                        if (record.offset() >= end) continue;

                        if (matchesPatterns(record.value(), patterns)) {
                            matchedRecords.add(record);
                        }
                    }
                    if (!matchedRecords.isEmpty()) {
                        ConsumerRecord<String, String> maxOffsetRecord = Collections.max(
                                matchedRecords,
                                Comparator.comparingLong(ConsumerRecord::offset)
                        );
                        return List.of(String.format("{\"offset\": %d, \"value\": \"%s\"}",
                                maxOffsetRecord.offset(), maxOffsetRecord.value()));
                    }

                    fromOffset = batchStart;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return List.of();
    }



    private List<String> consumePartitionRangeJSON(String topic, int partition, long startOffset, long endOffset,
                                                   Properties props, Map<String, String> filters, int maxResults) {
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
                    if (matchesFiltersJSON(record.value(), filters)) {
                        try {
                            JsonNode valueNode = objectMapper.readTree(record.value());
                            ObjectNode resultNode = objectMapper.createObjectNode();
                            resultNode.put("offset", record.offset());
                            resultNode.set("value", valueNode);
                            foundRecords.add(objectMapper.writeValueAsString(resultNode));
                        } catch (Exception e) {
                            foundRecords.add(String.format("{\"offset\": %d, \"value\": \"%s\"}",
                                    record.offset(), record.value()));
                        }
                        if (foundRecords.size() >= maxResults) break;
                    }
                    currentOffset = record.offset() + 1;
                }
            }
        }
        return foundRecords;
    }

    private List<String> consumePartitionRangeString(String topic, int partition, long startOffset, long endOffset,
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
                    if (matchesFiltersString(record.value(), rawFilters)) {
                        foundRecords.add(String.format("{\"offset\": %d, \"value\": \"%s\"}", record.offset(), record.value()));
                        if (foundRecords.size() >= maxResults) break;
                    }
                    currentOffset = record.offset() + 1;
                }
            }
        }
        return foundRecords;
    }

    private List<String> consumePartitionRangePattern(String topic, int partition, long startOffset, long endOffset,
                                                      Properties props, List<Pattern> patterns, int maxResults) {
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
                    if (matchesPatterns(record.value(), patterns)) {
                        foundRecords.add(String.format("{\"offset\": %d, \"value\": \"%s\"}", record.offset(), record.value()));
                        if (foundRecords.size() >= maxResults) break;
                    }
                    currentOffset = record.offset() + 1;
                }
            }
        }
        return foundRecords;
    }

    private boolean matchesFiltersJSON(String json, Map<String, String> filters) {
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
    private boolean matchesFiltersString(String value, List<String> rawFilters) {
        if (rawFilters == null || rawFilters.isEmpty()) return true;
        if (value == null) return false;

        for (String filter : rawFilters) {
            if (!value.toLowerCase().contains(filter.toLowerCase().trim())) {
                return false;
            }
        }
        return true;
    }
    private boolean matchesPatterns(String value, List<Pattern> patterns) {
        if (value == null || patterns == null || patterns.isEmpty()) return true;
        for (Pattern pattern : patterns) {
            Matcher matcher = pattern.matcher(value);
            if (!matcher.find()) {
                return false;
            }
        }
        return true;
    }

    private List<String> copyMode(SearchRequest request) {
        List<String> copiedMessages = new ArrayList<>();

        String sourceTopic = request.getTopic();
        String targetTopic = request.getTargetTopic();
        String kafkaAddress = request.getKafkaAddress();
        long startOffset = request.getStartOffset();
        long endOffset = request.getEndOffset();
        int partitionId = request.getPartition();
        String filterMode = request.getFilterMode();

        Map<String, String> filters = request.getFilters();
        List<String> rawFilters = request.getRawFilters();

        Properties props = getKafkaProps(kafkaAddress);
        try (
                KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
                KafkaProducer<String, String> producer = new KafkaProducer<>(getKafkaProducerProps(kafkaAddress))
        ) {
            TopicPartition partition = new TopicPartition(sourceTopic, partitionId);
            consumer.assign(Collections.singletonList(partition));
            consumer.seek(partition, startOffset);

            boolean done = false;
            while (!done) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    if (record.offset() > endOffset) {
                        done = true;
                        break;
                    }

                    boolean matches = true;

                    if ("json".equalsIgnoreCase(filterMode)) {
                        matches = matchesFiltersJSON(record.value(), filters);
                    } else if ("string".equalsIgnoreCase(filterMode)) {
                        matches = matchesFiltersString(record.value(), rawFilters);
                    } else if ("pattern".equalsIgnoreCase(filterMode)) {
                        matches = matchesFiltersString(record.value(), rawFilters);
                    }

                    if (matches) {
                        ProducerRecord<String, String> newRecord = new ProducerRecord<>(targetTopic, record.key(), record.value());
                        producer.send(newRecord);
                        copiedMessages.add("Copied offset " + record.offset());
                    }
                }
            }

            producer.flush();
        } catch (Exception e) {
            e.printStackTrace();
            copiedMessages.add("Error: " + e.getMessage());
        }

        return copiedMessages;
    }

    private List<String> DateMode(SearchRequest request) {
        String topic = request.getTopic();
        String kafkaAddress = request.getKafkaAddress();
        int partition = request.getPartition();
        String key = request.getDateKey();
        String expectedDatePrefix = request.getDate();

        Properties props = getKafkaProps(kafkaAddress);
        List<String> results = new ArrayList<>();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            TopicPartition tp = new TopicPartition(topic, partition);
            consumer.assign(List.of(tp));
            consumer.seekToBeginning(List.of(tp));
            long low = consumer.position(tp);
            consumer.seekToEnd(List.of(tp));
            long high = consumer.position(tp) - 1;

            long matchedOffset = -1;

            while (low <= high) {
                long mid = (low + high) / 2;
                consumer.seek(tp, mid);
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));

                // poll() birden fazla mesaj dönebilir, sadece mid offset'ini kontrol ediyoruz
                Optional<ConsumerRecord<String, String>> midRecordOpt = records.records(tp)
                        .stream()
                        .filter(r -> r.offset() == mid)
                        .findFirst();

                if (midRecordOpt.isEmpty()) {
                    break;
                }

                ConsumerRecord<String, String> record = midRecordOpt.get();
                JsonNode json = objectMapper.readTree(record.value());
                JsonNode dateNode = json.get(key);

                if (dateNode != null && dateNode.asText().startsWith(expectedDatePrefix)) {
                    matchedOffset = mid;
                    high = mid - 1;
                } else if (dateNode == null || dateNode.asText().compareTo(expectedDatePrefix) < 0) {
                    low = mid + 1;
                } else {
                    high = mid - 1;
                }
            }

            if (matchedOffset != -1) {
                consumer.seek(tp, matchedOffset);
                ConsumerRecords<String, String> finalRecords = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : finalRecords.records(tp)) {
                    if (record.offset() == matchedOffset) {
                        JsonNode jsonNode = objectMapper.readTree(record.value());
                        ObjectNode result = objectMapper.createObjectNode();
                        result.put("offset", record.offset());
                        result.set("value", jsonNode);
                        results.add(result.toString());
                        break; // sadece bir tane
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return results;
    }



}