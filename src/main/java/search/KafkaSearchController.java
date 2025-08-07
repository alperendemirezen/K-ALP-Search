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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@CrossOrigin(origins = "*")
@RestController
@RequestMapping("/search")
public class KafkaSearchController {

    private static final Logger logger = LogManager.getLogger(KafkaSearchController.class);

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Map<String, List<Future<List<String>>>> activeRequests = new ConcurrentHashMap<>();


    private Properties getKafkaProps(String bootstrapServers) {
        logger.info("Creating Kafka props with default timeout configs for bootstrap.servers: {}", bootstrapServers);
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", "web-search-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");
        props.put("max.poll.records", 500);
        props.put("request.timeout.ms", "30000");
        props.put("session.timeout.ms", "30000");
        props.put("default.api.timeout.ms", "30000");
        props.put("max.poll.interval.ms", "30000");

        logger.debug("Kafka default timeout configs set - request.timeout.ms: 30000, session.timeout.ms: 30000, default.api.timeout.ms: 30000, max.poll.interval.ms: 30000");
        return props;
    }

    private Properties getKafkaProps(String bootstrapServers, int pollRecords, int timeoutMs) {
        logger.info("Creating Kafka props with custom timeout configs for bootstrap.servers: {}, pollRecords: {}, timeoutMs: {}",
                bootstrapServers, pollRecords, timeoutMs);
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", "web-search-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");
        props.put("max.poll.records", String.valueOf(pollRecords));
        props.put("request.timeout.ms", String.valueOf(timeoutMs));
        props.put("session.timeout.ms", String.valueOf(timeoutMs));
        props.put("default.api.timeout.ms", String.valueOf(timeoutMs));
        props.put("max.poll.interval.ms", String.valueOf(timeoutMs));

        logger.debug("Kafka custom timeout configs set - request.timeout.ms: {}, session.timeout.ms: {}, default.api.timeout.ms: {}, max.poll.interval.ms: {}",
                timeoutMs, timeoutMs, timeoutMs, timeoutMs);
        return props;
    }

    private Properties getKafkaProducerProps(String kafkaAddress) {
        logger.info("Creating Kafka producer props for address: {}", kafkaAddress);
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaAddress);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        logger.debug("Producer props created successfully");
        return props;
    }

    @GetMapping("/topics")
    public List<String> getAllTopics(@RequestParam("kafkaAddress") String kafkaAddress) {
        logger.info("getAllTopics request received for Kafka address: {}", kafkaAddress);

        Properties props = getKafkaProps(kafkaAddress);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            logger.debug("Kafka consumer created for topic listing, attempting to connect...");
            List<String> topicList = new ArrayList<>(consumer.listTopics().keySet());
            logger.info("Successfully fetched {} topics from Kafka", topicList.size());
            return topicList;
        } catch (Exception e) {
            logger.error("Error occurred while fetching topic list from Kafka address: {}", kafkaAddress, e);
            return Collections.emptyList();
        }
    }


    @GetMapping("/topic-offsets")
    public Map<String, Object> getOffsetsAndPartitions(
            @RequestParam("kafkaAddress") String kafkaAddress,
            @RequestParam("topic") String topic) {

        logger.info("getOffsetsAndPartitions request received for topic: {} on Kafka: {}", topic, kafkaAddress);

        Properties props = getKafkaProps(kafkaAddress);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            logger.debug("Kafka consumer created, attempting to get partitions for topic: {}", topic);

            List<PartitionInfo> partitions = consumer.partitionsFor(topic);
            if (partitions == null || partitions.isEmpty()) {
                logger.warn("No partitions found for topic: {}", topic);
                return Map.of("startOffset", 0L, "endOffset", 0L, "partitions", List.of(0));
            }

            List<TopicPartition> topicPartitions = new ArrayList<>();
            List<Integer> partitionNumbers = new ArrayList<>();
            for (PartitionInfo p : partitions) {
                topicPartitions.add(new TopicPartition(p.topic(), p.partition()));
                partitionNumbers.add(p.partition());
            }

            logger.debug("Fetching beginning and end offsets for {} partitions...", partitions.size());

            Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(topicPartitions);
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(topicPartitions);

            long min = beginningOffsets.values().stream().min(Long::compareTo).orElse(0L);
            long max = endOffsets.values().stream().max(Long::compareTo).orElse(0L);

            logger.info("Offsets fetched successfully for topic: {} - Start: {}, End: {}, Partitions: {}",
                    topic, min, max - 1, partitionNumbers);

            Map<String, Object> response = new HashMap<>();
            response.put("startOffset", min);
            response.put("endOffset", max - 1);
            response.put("partitions", partitionNumbers);
            return response;

        } catch (Exception e) {
            logger.error("Error occurred while fetching offsets for topic: {} on Kafka: {}", topic, kafkaAddress, e);
            return Map.of("startOffset", 0L, "endOffset", 0L, "partitions", List.of(0));
        }
    }


    @PostMapping("/cancel")
    public void cancelSearch(@RequestBody Map<String, String> payload) {
        String requestId = payload.get("requestId");
        logger.info("Cancel search request received for requestId: {}", requestId);

        List<Future<List<String>>> futures = activeRequests.get(requestId);
        if (futures != null) {
            logger.debug("Cancelling {} active futures for requestId: {}", futures.size(), requestId);
            for (Future<List<String>> future : futures) {
                future.cancel(true);
            }
            activeRequests.remove(requestId);
            logger.info("Request cancelled and removed from activeRequests: {}", requestId);
        } else {
            logger.warn("No active request found with requestId: {}", requestId);
        }
    }


    @PostMapping
    public List<String> JSONSearch(@RequestBody SearchRequest request) throws InterruptedException, ExecutionException {
        return performSearch(request, "json");
    }

    @PostMapping("/simple-string-search")
    public List<String> stringSearch(@RequestBody SearchRequest request) throws InterruptedException, ExecutionException {
        return performSearch(request, "string");
    }

    @PostMapping("/pattern-search")
    public List<String> patternSearch(@RequestBody SearchRequest request) throws InterruptedException, ExecutionException {
        return performSearch(request, "pattern");
    }

    private List<String> performSearch(@RequestBody SearchRequest request, String searchType) throws InterruptedException, ExecutionException {
        logger.info("{} request received for topic: {}, mode: {}",
                searchType + "Search", request.getTopic(), request.getMode());

        List<Pattern> patterns = new ArrayList<>();
        if ("pattern".equals(searchType)) {
            for (String p : request.getRawFilters()) {
                try {
                    patterns.add(Pattern.compile(p, Pattern.CASE_INSENSITIVE));
                } catch (Exception e) {
                    logger.error("Invalid regex pattern: {}", p, e);
                    return Collections.singletonList("Invalid regex: " + p);
                }
            }
        }

        logger.info("Preparing search context for topic: {}, mode: {}, requestId: {}",
                request.getTopic(), request.getMode(), request.getRequestId());

        String topic = request.getTopic();
        String mode = request.getMode();
        Long lastN = request.getLastN();
        int maxResults = "last".equalsIgnoreCase(mode) ? 1 : Integer.MAX_VALUE;
        String requestId = request.getRequestId();

        logger.debug("Using provided requestId: {}", requestId);

        List<Future<List<String>>> futures = new ArrayList<>();
        activeRequests.put(requestId, futures);

        int safePollRecords = request.getPollRecords() > 0 ? request.getPollRecords() : 500;
        int safeTimeoutMs = request.getTimeoutMs() > 0 ? request.getTimeoutMs() : 30000;
        logger.debug("Using safePollRecords: {}, safeTimeoutMs: {}", safePollRecords, safeTimeoutMs);

        Properties props = getKafkaProps(request.getKafkaAddress(), safePollRecords, safeTimeoutMs);

        ExecutorService executor;
        if ("last".equalsIgnoreCase(mode)) {
            logger.info("Executing 'last' mode with single thread executor in {}", searchType + "Search");
            executor = Executors.newSingleThreadExecutor();

            Callable<List<String>> task;
            if ("json".equals(searchType)) {
                task = () -> searchLastRecordJSON(topic, props, request.getFilters());

            } else if ("string".equals(searchType)) {
                task = () -> searchLastRecordString(topic, props, request.getRawFilters());

            } else {
                task = () -> searchLastRecordPattern(topic, props, patterns);
            }

            futures.add(executor.submit(task));

        } else if ("copy".equalsIgnoreCase(mode)) {
            logger.info("Executing 'copy' mode with single thread executor in {}", searchType + "Search");
            executor = Executors.newSingleThreadExecutor();
            Callable<List<String>> task = () -> copyMode(request, props);
            futures.add(executor.submit(task));

        } else if ("date".equalsIgnoreCase(mode)) {
            logger.info("Executing 'date' mode with single thread executor in {}", searchType + "Search");
            executor = Executors.newSingleThreadExecutor();
            Callable<List<String>> task = () -> DateMode(request, props);
            futures.add(executor.submit(task));

        } else {

            KafkaConsumer<String, String> consumer;
            try {
                logger.debug("Creating Kafka consumer with timeout configs...");
                consumer = new KafkaConsumer<>(props);
                logger.info("Kafka consumer created successfully");
            } catch (Exception e) {
                logger.error("Failed to create Kafka consumer", e);
                throw new RuntimeException("Failed to create Kafka consumer", e);
            }

            List<TopicPartition> partitions;
            Map<TopicPartition, Long> beginningOffsets;
            Map<TopicPartition, Long> endOffsets;

            try {
                logger.debug("Getting partitions for topic: {}", topic);
                List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
                if (partitionInfos == null || partitionInfos.isEmpty()) {
                    logger.warn("Topic not found: {}", topic);
                    return Collections.singletonList("Topic not found.");
                }

                partitions = new ArrayList<>();
                for (PartitionInfo p : partitionInfos) {
                    partitions.add(new TopicPartition(p.topic(), p.partition()));
                }

                logger.info("Found {} partitions for topic: {}", partitions.size(), topic);

                logger.debug("Assigning partitions and fetching offset info...");
                consumer.assign(partitions);
                beginningOffsets = consumer.beginningOffsets(partitions);
                endOffsets = consumer.endOffsets(partitions);

                logger.info("Offset info fetched successfully. Assigned partitions: {}", partitions);
                logger.info("Active timeout configs - request.timeout.ms: {}, session.timeout.ms: {}, default.api.timeout.ms: {}, max.poll.interval.ms: {}",
                        props.getProperty("request.timeout.ms"), props.getProperty("session.timeout.ms"),
                        props.getProperty("default.api.timeout.ms"), props.getProperty("max.poll.interval.ms"));

            } catch (Exception e) {
                logger.error("Error during search context preparation for topic: {}", topic, e);
                throw new RuntimeException("Failed to prepare search context", e);
            }

            int threads = request.getThreads() > 0 ? request.getThreads() : 4;
            logger.info("Starting parallel {} search with {} threads for {} partitions",
                    searchType, threads, partitions.size());

            executor = Executors.newFixedThreadPool(threads);

            for (TopicPartition tp : partitions) {
                long start = beginningOffsets.get(tp);
                long end = endOffsets.get(tp);

                if (end < start) {
                    logger.debug("Skipping partition {} - end offset {} is less than start offset {}",
                            tp.partition(), end, start);
                    continue;
                }

                if ("manual".equalsIgnoreCase(mode) && request.getStartOffset() != null && request.getEndOffset() != null) {
                    start = Math.max(start, request.getStartOffset());
                    end = Math.min(end, request.getEndOffset());
                    logger.info("Manual mode active for partition {}. Range adjusted: start={}, end={}",
                            tp.partition(), start, end);
                }

                if ("lastN".equalsIgnoreCase(mode) && lastN != null) {
                    start = Math.max(end - lastN + 1, start);
                    logger.info("lastN mode active for partition {}. New start offset: {}", tp.partition(), start);
                }

                long rangeSize = Math.max(1, (end - start + 1) / threads);
                logger.debug("Partition {} processing - start={}, end={}, rangeSize={}",
                        tp.partition(), start, end, rangeSize);

                for (int i = 0; i < threads; i++) {
                    long partStart = start + i * rangeSize;
                    long partEnd = (i == threads - 1) ? end : Math.min(partStart + rangeSize - 1, end);

                    logger.debug("Submitting {} task for partition {}, thread {}, range [{} - {}]",
                            searchType, tp.partition(), i, partStart, partEnd);

                    Callable<List<String>> task;
                    if ("json".equals(searchType)) {
                        task = () -> consumePartitionRangeJSON(
                                topic, tp.partition(), partStart, partEnd, props, request.getFilters(), maxResults
                        );
                    } else if ("string".equals(searchType)) {
                        task = () -> consumePartitionRangeString(
                                topic, tp.partition(), partStart, partEnd, props, request.getRawFilters(), maxResults
                        );
                    } else { // pattern
                        final List<Pattern> finalPatterns = patterns;
                        task = () -> consumePartitionRangePattern(
                                topic, tp.partition(), partStart, partEnd, props, finalPatterns, maxResults
                        );
                    }
                    futures.add(executor.submit(task));
                }
            }
        }

        List<String> results = new ArrayList<>();
        for (Future<List<String>> f : futures) {
            try {
                List<String> partial = f.get();
                results.addAll(partial);
                if (results.size() >= maxResults) {
                    logger.debug("Max result limit reached in {} processing. Truncating results.", searchType);
                    break;
                }
            } catch (Exception e) {
                logger.error("Error getting results from future in parallel {} processing", searchType, e);
            }
        }

        if (executor != null) {
            executor.shutdownNow();
        }

        logger.info("{} parallel processing completed. Total results: {}",
                searchType + "Search", results.size());

        return results.size() > maxResults ? results.subList(0, maxResults) : results;
    }


    private List<String> searchLastRecordJSON(String topic, Properties props, Map<String, String> filters) {
        int stepSize = 500;
        logger.info("Starting searchLastRecordJSON for topic: {}, stepSize: {}", topic, stepSize);
        logger.debug("Filters applied: {}", filters);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {

            List<TopicPartition> partitions;
            Map<TopicPartition, Long> beginningOffsets;
            Map<TopicPartition, Long> endOffsets;
            logger.debug("Consumer created, fetching offset information...");

            try {
                logger.debug("Getting partitions for topic: {}", topic);
                List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
                if (partitionInfos == null || partitionInfos.isEmpty()) {
                    logger.warn("Topic not found: {}", topic);
                    return Collections.singletonList("Topic not found.");
                }

                partitions = new ArrayList<>();
                for (PartitionInfo p : partitionInfos) {
                    partitions.add(new TopicPartition(p.topic(), p.partition()));
                }

                logger.info("Found {} partitions for topic: {}", partitions.size(), topic);
                logger.debug("Assigning partitions and fetching offset info...");
                consumer.assign(partitions);
                beginningOffsets = consumer.beginningOffsets(partitions);
                endOffsets = consumer.endOffsets(partitions);

            } catch (Exception e) {
                logger.error("Error during search context preparation for topic: {}", topic, e);
                throw new RuntimeException("Failed to prepare search context", e);
            }


            for (TopicPartition tp : partitions) {
                long start = beginningOffsets.get(tp);
                long end = endOffsets.get(tp);
                long fromOffset = end;

                logger.info("Searching partition {} from offset {} to {}", tp.partition(), start, end);

                while (fromOffset > start) {
                    long batchStart = Math.max(start, fromOffset - stepSize);
                    logger.debug("Polling range [{} - {}] with timeout from props", batchStart, fromOffset - 1);

                    consumer.assign(Collections.singletonList(tp));
                    consumer.seek(tp, batchStart);

                    long pollTimeoutMs = 30000;
                    logger.debug("Executing consumer.poll() with timeout: {}ms", pollTimeoutMs);
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(pollTimeoutMs));

                    if (records.isEmpty()) {
                        logger.debug("Poll returned no records for range [{} - {}]", batchStart, fromOffset - 1);
                    } else {
                        logger.debug("Poll returned {} records", records.count());
                    }

                    List<ConsumerRecord<String, String>> matchedRecords = new ArrayList<>();

                    for (ConsumerRecord<String, String> record : records.records(tp)) {
                        if (record.offset() >= end) continue;
                        if (matchesFiltersJSON(record.value(), filters)) {
                            matchedRecords.add(record);
                            logger.debug("Record at offset {} matches filters", record.offset());
                        }
                    }

                    if (!matchedRecords.isEmpty()) {
                        logger.info("Found {} matching record(s) in partition {}. Returning the latest one.", matchedRecords.size(), tp.partition());
                        ConsumerRecord<String, String> maxOffsetRecord = Collections.max(
                                matchedRecords,
                                Comparator.comparingLong(ConsumerRecord::offset)
                        );
                        try {
                            JsonNode valueNode = objectMapper.readTree(maxOffsetRecord.value());
                            ObjectNode resultNode = objectMapper.createObjectNode();
                            resultNode.put("offset", maxOffsetRecord.offset());
                            resultNode.set("value", valueNode);
                            logger.info("Returning JSON result for offset: {}", maxOffsetRecord.offset());
                            return List.of(objectMapper.writeValueAsString(resultNode));
                        } catch (Exception e) {
                            logger.warn("Failed to parse JSON for offset: {}. Returning raw value.", maxOffsetRecord.offset(), e);
                            return List.of(String.format("{\"offset\": %d, \"value\": \"%s\"}",
                                    maxOffsetRecord.offset(), maxOffsetRecord.value()));
                        }
                    }

                    fromOffset = batchStart;
                }

                logger.debug("No matching records found in partition {}", tp.partition());
            }
        } catch (Exception e) {
            logger.error("Error in searchLastRecordJSON for topic: {}", topic, e);
        }

        logger.info("searchLastRecordJSON finished. No matching records found.");
        return List.of();
    }


    private List<String> searchLastRecordString(String topic,
                                                Properties props,
                                                List<String> rawFilters) {
        int stepSize = 500;
        logger.info("Starting searchLastRecordString for topic: {}, stepSize: {}", topic, stepSize);
        logger.debug("Filters applied: {}", rawFilters);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            logger.debug("Consumer created, fetching offset information...");
            List<TopicPartition> partitions;
            Map<TopicPartition, Long> beginningOffsets;
            Map<TopicPartition, Long> endOffsets;

            try {
                logger.debug("Getting partitions for topic: {}", topic);
                List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
                if (partitionInfos == null || partitionInfos.isEmpty()) {
                    logger.warn("Topic not found: {}", topic);
                    return Collections.singletonList("Topic not found.");
                }

                partitions = new ArrayList<>();
                for (PartitionInfo p : partitionInfos) {
                    partitions.add(new TopicPartition(p.topic(), p.partition()));
                }

                logger.info("Found {} partitions for topic: {}", partitions.size(), topic);

                logger.debug("Assigning partitions and fetching offset info...");
                consumer.assign(partitions);
                beginningOffsets = consumer.beginningOffsets(partitions);
                endOffsets = consumer.endOffsets(partitions);

            } catch (Exception e) {
                logger.error("Error during search context preparation for topic: {}", topic, e);
                throw new RuntimeException("Failed to prepare search context", e);
            }

            for (TopicPartition tp : partitions) {
                long start = beginningOffsets.get(tp);
                long end = endOffsets.get(tp);
                long fromOffset = end;

                logger.info("Searching partition {} from offset {} to {}", tp.partition(), start, end);

                while (fromOffset > start) {
                    long batchStart = Math.max(start, fromOffset - stepSize);
                    logger.debug("Polling range [{} - {}]", batchStart, fromOffset - 1);

                    consumer.assign(Collections.singletonList(tp));
                    consumer.seek(tp, batchStart);

                    long pollTimeoutMs = 30000;
                    logger.debug("Executing consumer.poll() with timeout: {}ms", pollTimeoutMs);
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(pollTimeoutMs));

                    if (records.isEmpty()) {
                        logger.debug("Poll returned no records for range [{} - {}]", batchStart, fromOffset - 1);
                    } else {
                        logger.debug("Poll returned {} records", records.count());
                    }

                    List<ConsumerRecord<String, String>> matchedRecords = new ArrayList<>();

                    for (ConsumerRecord<String, String> record : records.records(tp)) {
                        if (record.offset() >= end) continue;
                        if (matchesFiltersString(record.value(), rawFilters)) {
                            matchedRecords.add(record);
                            logger.debug("Record at offset {} matches string filters", record.offset());
                        }
                    }

                    if (!matchedRecords.isEmpty()) {
                        logger.info("Found {} matching record(s) in partition {}. Returning the latest one.", matchedRecords.size(), tp.partition());
                        ConsumerRecord<String, String> maxOffsetRecord = Collections.max(
                                matchedRecords,
                                Comparator.comparingLong(ConsumerRecord::offset)
                        );
                        logger.info("Returning string result for offset: {}", maxOffsetRecord.offset());
                        return List.of(String.format("{\"offset\": %d, \"value\": \"%s\"}",
                                maxOffsetRecord.offset(), maxOffsetRecord.value()));
                    }

                    fromOffset = batchStart;
                }

                logger.debug("No matching records found in partition {}", tp.partition());
            }
        } catch (Exception e) {
            logger.error("Error in searchLastRecordString for topic: {}", topic, e);
        }

        logger.info("searchLastRecordString finished. No matching records found.");
        return List.of();
    }


    private List<String> searchLastRecordPattern(String topic,
                                                 Properties props,
                                                 List<Pattern> patterns) {
        int stepSize = 500;
        logger.info("Starting searchLastRecordPattern for topic: {}, stepSize: {}, patterns count: {}", topic, stepSize, patterns.size());

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            logger.debug("Consumer created, fetching offset information...");
            List<TopicPartition> partitions;
            Map<TopicPartition, Long> beginningOffsets;
            Map<TopicPartition, Long> endOffsets;

            try {
                logger.debug("Getting partitions for topic: {}", topic);
                List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
                if (partitionInfos == null || partitionInfos.isEmpty()) {
                    logger.warn("Topic not found: {}", topic);
                    return Collections.singletonList("Topic not found.");
                }

                partitions = new ArrayList<>();
                for (PartitionInfo p : partitionInfos) {
                    partitions.add(new TopicPartition(p.topic(), p.partition()));
                }

                logger.info("Found {} partitions for topic: {}", partitions.size(), topic);

                logger.debug("Assigning partitions and fetching offset info...");
                consumer.assign(partitions);
                beginningOffsets = consumer.beginningOffsets(partitions);
                endOffsets = consumer.endOffsets(partitions);

            } catch (Exception e) {
                logger.error("Error during search context preparation for topic: {}", topic, e);
                throw new RuntimeException("Failed to prepare search context", e);
            }

            for (TopicPartition tp : partitions) {
                long start = beginningOffsets.get(tp);
                long end = endOffsets.get(tp);
                long fromOffset = end;

                logger.info("Searching partition {} from offset {} to {}", tp.partition(), start, end);

                while (fromOffset > start) {
                    long batchStart = Math.max(start, fromOffset - stepSize);
                    logger.debug("Polling range [{} - {}]", batchStart, fromOffset - 1);

                    consumer.assign(Collections.singletonList(tp));
                    consumer.seek(tp, batchStart);

                    long pollTimeoutMs = 30000;
                    logger.debug("Executing consumer.poll() with timeout: {}ms", pollTimeoutMs);
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(pollTimeoutMs));

                    if (records.isEmpty()) {
                        logger.debug("Poll returned no records for range [{} - {}]", batchStart, fromOffset - 1);
                    } else {
                        logger.debug("Poll returned {} records", records.count());
                    }

                    List<ConsumerRecord<String, String>> matchedRecords = new ArrayList<>();

                    for (ConsumerRecord<String, String> record : records.records(tp)) {
                        if (record.offset() >= end) continue;

                        if (matchesPatterns(record.value(), patterns)) {
                            matchedRecords.add(record);
                            logger.debug("Record at offset {} matches pattern filters", record.offset());
                        }
                    }

                    if (!matchedRecords.isEmpty()) {
                        logger.info("Found {} matching record(s) in partition {}. Returning the latest one.", matchedRecords.size(), tp.partition());
                        ConsumerRecord<String, String> maxOffsetRecord = Collections.max(
                                matchedRecords,
                                Comparator.comparingLong(ConsumerRecord::offset)
                        );
                        logger.info("Returning pattern result for offset: {}", maxOffsetRecord.offset());
                        return List.of(String.format("{\"offset\": %d, \"value\": \"%s\"}",
                                maxOffsetRecord.offset(), maxOffsetRecord.value()));
                    }

                    fromOffset = batchStart;
                }

                logger.debug("No matching records found in partition {}", tp.partition());
            }
        } catch (Exception e) {
            logger.error("Error in searchLastRecordPattern for topic: {}", topic, e);
        }

        logger.info("searchLastRecordPattern finished. No matching records found.");
        return List.of();
    }

    private List<String> consumePartitionRangeJSON(String topic, int partition, long startOffset, long endOffset, Properties props, Map<String, String> filters, int maxResults) {
        logger.info("Starting consumePartitionRangeJSON - topic: {}, partition: {}, offsets: [{}-{}], maxResults: {}", topic, partition, startOffset, endOffset, maxResults);
        logger.debug("JSON filters: {}", filters);

        List<String> foundRecords = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            TopicPartition tp = new TopicPartition(topic, partition);
            consumer.assign(List.of(tp));
            consumer.seek(tp, startOffset);
            logger.debug("Consumer assigned to partition {} and seeked to offset {}", partition, startOffset);

            long currentOffset = startOffset;

            while (currentOffset <= endOffset && foundRecords.size() < maxResults) {
                long pollTimeout = props.containsKey("request.timeout.ms")
                        ? Long.parseLong(props.getProperty("request.timeout.ms"))
                        : 30000;

                logger.debug("Polling with timeout: {}ms at offset: {}", pollTimeout, currentOffset);
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(pollTimeout));

                if (records.isEmpty()) {
                    logger.debug("No records received at offset {}, breaking consumption", currentOffset);
                    break;
                }
                logger.debug("Received {} records from poll", records.count());

                if (!Thread.currentThread().isInterrupted()) {
                    for (ConsumerRecord<String, String> record : records) {
                        if (record.offset() > endOffset) {
                            logger.debug("Record offset {} exceeds end offset {}, breaking", record.offset(), endOffset);
                            break;
                        }

                        if (matchesFiltersJSON(record.value(), filters)) {
                            try {
                                JsonNode valueNode = objectMapper.readTree(record.value());
                                ObjectNode resultNode = objectMapper.createObjectNode();
                                resultNode.put("offset", record.offset());
                                resultNode.set("value", valueNode);
                                foundRecords.add(objectMapper.writeValueAsString(resultNode));
                                logger.debug("JSON match found at offset {}", record.offset());
                            } catch (Exception e) {
                                logger.warn("Failed to parse JSON at offset {}, using raw value", record.offset(), e);
                                foundRecords.add(String.format("{\"offset\": %d, \"value\": \"%s\"}",
                                        record.offset(), record.value()));
                            }

                            if (foundRecords.size() >= maxResults) {
                                logger.debug("Reached maxResults limit ({})", maxResults);
                                break;
                            }
                        }
                        currentOffset = record.offset() + 1;
                    }
                } else {
                    logger.warn("Thread interrupted during JSON consumption for partition {}", partition);
                    break;
                }
            }
        } catch (Exception e) {
            logger.error("Error in consumePartitionRangeJSON - topic: {}, partition: {}", topic, partition, e);
        }

        logger.info("consumePartitionRangeJSON finished - partition: {}, total matches: {}", partition, foundRecords.size());
        return foundRecords;
    }


    private List<String> consumePartitionRangeString(String topic, int partition, long startOffset, long endOffset,
                                                     Properties props, List<String> rawFilters, int maxResults) {
        logger.info("Starting consumePartitionRangeString - topic: {}, partition: {}, offsets: [{}-{}], maxResults: {}",
                topic, partition, startOffset, endOffset, maxResults);
        logger.debug("String filters: {}", rawFilters);

        List<String> foundRecords = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            TopicPartition tp = new TopicPartition(topic, partition);
            consumer.assign(List.of(tp));
            consumer.seek(tp, startOffset);
            logger.debug("Consumer assigned to partition {} and seeked to offset {}", partition, startOffset);
            long currentOffset = startOffset;

            while (currentOffset <= endOffset && foundRecords.size() < maxResults) {
                long pollTimeout = props.containsKey("request.timeout.ms")
                        ? Long.parseLong(props.getProperty("request.timeout.ms"))
                        : 30000;

                logger.debug("Polling with timeout: {}ms at offset: {}", pollTimeout, currentOffset);
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(pollTimeout));

                if (records.isEmpty()) {
                    logger.debug("No records received at offset {}, breaking consumption", currentOffset);
                    break;
                }
                logger.debug("Received {} records from poll", records.count());

                if (!Thread.currentThread().isInterrupted()) {
                    for (ConsumerRecord<String, String> record : records) {
                        if (record.offset() > endOffset) {
                            logger.debug("Record offset {} exceeds end offset {}, breaking", record.offset(), endOffset);
                            break;
                        }

                        if (matchesFiltersString(record.value(), rawFilters)) {
                            foundRecords.add(String.format("{\"offset\": %d, \"value\": \"%s\"}", record.offset(), record.value()));
                            logger.debug("String match found at offset {}", record.offset());

                            if (foundRecords.size() >= maxResults) {
                                logger.debug("Reached maxResults limit ({})", maxResults);
                                break;
                            }
                        }
                        currentOffset = record.offset() + 1;
                    }
                } else {
                    logger.warn("Thread interrupted during string consumption for partition {}", partition);
                    break;
                }
            }
        } catch (Exception e) {
            logger.error("Error in consumePartitionRangeString - topic: {}, partition: {}", topic, partition, e);
        }

        logger.info("consumePartitionRangeString finished - partition: {}, total matches: {}", partition, foundRecords.size());
        return foundRecords;
    }


    private List<String> consumePartitionRangePattern(String topic, int partition, long startOffset, long endOffset,
                                                      Properties props, List<Pattern> patterns, int maxResults) {
        logger.info("Starting consumePartitionRangePattern - topic: {}, partition: {}, offsets: [{}-{}], maxResults: {}, patterns count: {}",
                topic, partition, startOffset, endOffset, maxResults, patterns.size());

        List<String> foundRecords = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            TopicPartition tp = new TopicPartition(topic, partition);
            consumer.assign(List.of(tp));
            consumer.seek(tp, startOffset);
            logger.debug("Consumer assigned to partition {} and seeked to offset {}", partition, startOffset);
            long currentOffset = startOffset;

            while (currentOffset <= endOffset && foundRecords.size() < maxResults) {
                long pollTimeout = props.containsKey("request.timeout.ms")
                        ? Long.parseLong(props.getProperty("request.timeout.ms"))
                        : 30000;

                logger.debug("Polling with timeout: {}ms at offset: {}", pollTimeout, currentOffset);
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(pollTimeout));

                if (records.isEmpty()) {
                    logger.debug("No records received at offset {}, breaking consumption", currentOffset);
                    break;
                }
                logger.debug("Received {} records from poll", records.count());

                if (!Thread.currentThread().isInterrupted()) {
                    for (ConsumerRecord<String, String> record : records) {
                        if (record.offset() > endOffset) {
                            logger.debug("Record offset {} exceeds end offset {}, breaking", record.offset(), endOffset);
                            break;
                        }

                        if (matchesPatterns(record.value(), patterns)) {
                            foundRecords.add(String.format("{\"offset\": %d, \"value\": \"%s\"}", record.offset(), record.value()));
                            logger.debug("Pattern match found at offset {}", record.offset());

                            if (foundRecords.size() >= maxResults) {
                                logger.debug("Reached maxResults limit ({})", maxResults);
                                break;
                            }
                        }

                        currentOffset = record.offset() + 1;
                    }
                } else {
                    logger.warn("Thread interrupted during pattern consumption for partition {}", partition);
                    break;
                }
            }
        } catch (Exception e) {
            logger.error("Error in consumePartitionRangePattern - topic: {}, partition: {}", topic, partition, e);
        }

        logger.info("consumePartitionRangePattern finished - partition: {}, total matches: {}", partition, foundRecords.size());
        return foundRecords;
    }

    private boolean matchesFiltersJSON(String json, Map<String, String> filters) {
        if (filters == null || filters.isEmpty()) return true;
        try {
            JsonNode node = objectMapper.readTree(json);
            for (Map.Entry<String, String> entry : filters.entrySet()) {
                JsonNode val = node.get(entry.getKey());
                if (val == null || !val.asText().equals(entry.getValue())) {
                    logger.debug("JSON filter mismatch on key: {}, expected: {}, actual: {}",
                            entry.getKey(), entry.getValue(), val != null ? val.asText() : "null");
                    return false;
                }
            }
            return true;
        } catch (Exception e) {
            logger.debug("Invalid JSON format or parse error in matchesFiltersJSON", e);
            return false;
        }
    }


    private boolean matchesFiltersString(String value, List<String> rawFilters) {
        if (rawFilters == null || rawFilters.isEmpty()) return true;
        if (value == null) return false;

        for (String filter : rawFilters) {
            if (!value.toLowerCase().contains(filter.toLowerCase().trim())) {
                logger.debug("String filter mismatch: value does not contain filter '{}'", filter);
                return false;
            }
        }

        logger.debug("All string filters matched");
        return true;
    }


    private boolean matchesPatterns(String value, List<Pattern> patterns) {
        if (value == null || patterns == null || patterns.isEmpty()) return true;

        for (Pattern pattern : patterns) {
            Matcher matcher = pattern.matcher(value);
            if (!matcher.find()) {
                logger.debug("Pattern mismatch: value does not match pattern '{}'", pattern.pattern());
                return false;
            }
        }

        logger.debug("All patterns matched");
        return true;
    }


    private List<String> copyMode(SearchRequest request, Properties props) {
        logger.info("Starting copyMode - source topic: {}, target topic: {}, partition: {}, offsets: [{}-{}]",
                request.getTopic(), request.getTargetTopic(), request.getPartition(),
                request.getStartOffset(), request.getEndOffset());

        List<String> copiedMessages = new ArrayList<>();

        String sourceTopic = request.getTopic();
        String targetTopic = request.getTargetTopic();
        String targetKafka = request.getTargetKafka();
        long startOffset = request.getStartOffset();
        long endOffset = request.getEndOffset();
        int partitionId = request.getPartition();
        String filterMode = request.getFilterMode();

        Map<String, String> filters = request.getFilters();
        List<String> rawFilters = request.getRawFilters();

        logger.debug("Copy filterMode: {}, filters: {}, rawFilters: {}", filterMode, filters, rawFilters);

        try (
                KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
                KafkaProducer<String, String> producer = new KafkaProducer<>(getKafkaProducerProps(targetKafka));
        ) {
            logger.debug("Consumer and producer created successfully");
            TopicPartition partition = new TopicPartition(sourceTopic, partitionId);
            consumer.assign(Collections.singletonList(partition));
            consumer.seek(partition, startOffset);
            logger.debug("Consumer assigned to partition {} and seeked to start offset {}", partitionId, startOffset);

            boolean done = false;
            while (!done) {
                long pollTimeout = props.containsKey("request.timeout.ms")
                        ? Long.parseLong(props.getProperty("request.timeout.ms"))
                        : 30000;

                logger.debug("Polling in copyMode with timeout: {}ms", pollTimeout);
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(pollTimeout));

                if (records.isEmpty()) {
                    logger.debug("No records received in copyMode, continuing...");
                }

                for (ConsumerRecord<String, String> record : records) {
                    if (record.offset() > endOffset) {
                        logger.info("Reached end offset {} in copyMode, stopping", endOffset);
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
                        logger.debug("Copied message from offset {} to target topic", record.offset());
                    }
                }
            }

            logger.debug("Flushing producer in copyMode...");
            producer.flush();
            logger.info("copyMode completed successfully. Total copied: {}", copiedMessages.size());
        } catch (Exception e) {
            logger.error("Error in copyMode", e);
            copiedMessages.add("Error: " + e.getMessage());
        }

        return copiedMessages;
    }


    private List<String> DateMode(SearchRequest request, Properties props) {
        logger.info("Starting DateMode - topic: {}, partition: {}, dateKey: {}, date prefix: {}",
                request.getTopic(), request.getPartition(), request.getDateKey(), request.getDate());

        String topic = request.getTopic();
        int partition = request.getPartition();
        String key = request.getDateKey();
        String expectedDatePrefix = request.getDate();

        List<String> results = new ArrayList<>();
        long resultOffset = -1;

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            TopicPartition tp = new TopicPartition(topic, partition);
            consumer.assign(List.of(tp));

            logger.debug("Seeking to beginning to get start offset...");
            long low = consumer.beginningOffsets(List.of(tp)).get(tp);

            logger.debug("Seeking to end to get end offset...");
            long high = consumer.endOffsets(List.of(tp)).get(tp) - 1;

            logger.info("Binary search range for DateMode: [{} - {}]", low, high);

            long closestAfterOffset = -1;
            while (low <= high) {
                long mid = (low + high) / 2;
                logger.debug("Binary search - checking offset: {}", mid);
                consumer.seek(tp, mid);

                long pollTimeout = props.containsKey("request.timeout.ms")
                        ? Long.parseLong(props.getProperty("request.timeout.ms"))
                        : 30000;

                logger.debug("Polling at mid offset {} with timeout: {}ms", mid, pollTimeout);
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(pollTimeout));

                Optional<ConsumerRecord<String, String>> recordOpt = records.records(tp).stream()
                        .filter(r -> r.offset() == mid)
                        .findFirst();

                if (recordOpt.isEmpty()) {
                    logger.debug("No record at offset {}, moving right in binary search", mid);
                    low = mid + 1;
                    continue;
                }

                ConsumerRecord<String, String> record = recordOpt.get();
                JsonNode json = objectMapper.readTree(record.value());
                JsonNode dateNode = json.get(key);

                if (dateNode == null || dateNode.asText().length() < 14) {
                    logger.debug("Invalid or missing timestamp at offset {}, moving left in binary search", mid);
                    high = mid - 1;
                    continue;
                }

                String datePrefix = dateNode.asText().substring(0, 14);
                int cmp = datePrefix.compareTo(expectedDatePrefix);

                logger.debug("Binary search - offset {} has date prefix '{}', comparison result: {}", mid, datePrefix, cmp);

                if (cmp < 0) {
                    low = mid + 1;
                } else {
                    if (cmp == 0) {
                        resultOffset = mid;
                        logger.info("Exact match found at offset {} in DateMode", mid);
                    } else {
                        closestAfterOffset = mid;
                        logger.debug("Setting closest after offset to {}", mid);
                    }
                    high = mid - 1;
                }
            }

            if (resultOffset == -1 && closestAfterOffset != -1) {
                resultOffset = closestAfterOffset;
                logger.info("No exact match in DateMode. Using closest after offset: {}", resultOffset);
            }

            if (resultOffset != -1) {
                logger.debug("Fetching records starting from result offset: {}", resultOffset);
                consumer.seek(tp, resultOffset);
                int fetchedCount = 0;
                long maxOffset = resultOffset + 100;

                while (fetchedCount < 20) {
                    logger.debug("Fetching batch in DateMode, current count: {}", fetchedCount);
                    ConsumerRecords<String, String> fetchedRecords = consumer.poll(Duration.ofMillis(5000));
                    if (fetchedRecords.isEmpty()) {
                        logger.debug("No more records available in DateMode fetch");
                        break;
                    }

                    for (ConsumerRecord<String, String> record : fetchedRecords.records(tp)) {
                        if (record.offset() >= maxOffset) {
                            logger.debug("Reached max offset {} in DateMode fetch", maxOffset);
                            break;
                        }

                        JsonNode jsonNode = objectMapper.readTree(record.value());
                        ObjectNode result = objectMapper.createObjectNode();
                        result.put("offset", record.offset());
                        result.set("value", jsonNode);
                        results.add(result.toString());
                        fetchedCount++;
                        logger.debug("Added record at offset {} to DateMode results", record.offset());

                        if (fetchedCount >= 20) break;
                    }
                }

                logger.info("DateMode retrieved {} records starting from offset {}", results.size(), resultOffset);
            } else {
                logger.warn("No matching record found for given date prefix in DateMode");
            }

        } catch (Exception e) {
            logger.error("Error in DateMode for topic: {}, partition: {}", topic, partition, e);
        }

        logger.info("DateMode search complete. Total matches: {}", results.size());
        return results;
    }
}