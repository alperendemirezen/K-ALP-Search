package search;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.coyote.Request;
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
        props.put("max.poll.records", 500);
        props.put("request.timeout.ms", "30000");
        props.put("session.timeout.ms", "30000");
        props.put("default.api.timeout.ms", "30000");
        props.put("max.poll.interval.ms", "30000");
        return props;
    }

    private Properties getKafkaProps(String bootstrapServers, int pollRecords, int timeoutMs) {
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
    public List<String> getAllTopics(@RequestParam("kafkaAddress") String kafkaAddress) {
        System.out.println(" [getAllTopics] Request received to fetch Kafka topic list from address: " + kafkaAddress);

        Properties props = getKafkaProps(kafkaAddress);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            List<String> topicList = new ArrayList<>(consumer.listTopics().keySet());
            System.out.println(" [getAllTopics] Successfully fetched " + topicList.size() + " topics.");
            return topicList;
        } catch (Exception e) {
            System.out.println(" [getAllTopics] Error occurred while fetching topic list: " + e.getMessage());
            e.printStackTrace();
            return Collections.emptyList();
        }
    }


    @GetMapping("/topic-offsets")
    public Map<String, Object> getOffsetsAndPartitions(
            @RequestParam("kafkaAddress") String kafkaAddress,
            @RequestParam("topic") String topic) {

        System.out.println(" [getOffsetsAndPartitions] Request received to fetch offsets and partitions for topic: " + topic);

        Properties props = getKafkaProps(kafkaAddress);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {

            List<PartitionInfo> partitions = consumer.partitionsFor(topic);
            if (partitions == null || partitions.isEmpty()) {
                System.out.println(" [getOffsetsAndPartitions] No partitions found for topic: " + topic);
                return Map.of("startOffset", 0L, "endOffset", 0L, "partitions", List.of(0));
            }

            List<TopicPartition> topicPartitions = new ArrayList<>();
            List<Integer> partitionNumbers = new ArrayList<>();
            for (PartitionInfo p : partitions) {
                topicPartitions.add(new TopicPartition(p.topic(), p.partition()));
                partitionNumbers.add(p.partition());
            }

            System.out.println(" [getOffsetsAndPartitions] Fetching beginning and end offsets...");

            Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(topicPartitions);
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(topicPartitions);

            long min = beginningOffsets.values().stream().min(Long::compareTo).orElse(0L);
            long max = endOffsets.values().stream().max(Long::compareTo).orElse(0L);

            System.out.println(" [getOffsetsAndPartitions] Offsets fetched. Start: " + min + ", End: " + (max - 1));
            System.out.println(" [getOffsetsAndPartitions] Partitions: " + partitionNumbers);

            Map<String, Object> response = new HashMap<>();
            response.put("startOffset", min);
            response.put("endOffset", max - 1);
            response.put("partitions", partitionNumbers);
            return response;

        } catch (Exception e) {
            System.out.println(" [getOffsetsAndPartitions] Error occurred: " + e.getMessage());
            e.printStackTrace();
            return Map.of("startOffset", 0L, "endOffset", 0L, "partitions", List.of(0));
        }
    }


    @PostMapping("/cancel")
    public void cancelSearch(@RequestBody Map<String, String> payload) {
        String requestId = payload.get("requestId");
        System.out.println(" [cancelSearch] Cancel request received for requestId: " + requestId);

        List<Future<List<String>>> futures = activeRequests.get(requestId);
        if (futures != null) {
            for (Future<List<String>> future : futures) {
                future.cancel(true);
            }
            activeRequests.remove(requestId);
            System.out.println(" [cancelSearch] Request cancelled and removed from activeRequests: " + requestId);
        } else {
            System.out.println(" [cancelSearch] No active request found with requestId: " + requestId);
        }
    }


    private SearchContext prepareSearch(SearchRequest request) {
        System.out.println(" [prepareSearch] Preparing search context for topic: " + request.getTopic());

        SearchContext ctx = new SearchContext();

        ctx.topic = request.getTopic();
        ctx.mode = request.getMode();
        ctx.lastN = request.getLastN();
        ctx.maxResults = "last".equalsIgnoreCase(ctx.mode) ? 1 : Integer.MAX_VALUE;

        ctx.requestId = request.getRequestId();
        System.out.println(" [prepareSearch] Using provided requestId: " + ctx.requestId);


        ctx.futures = new ArrayList<>();
        activeRequests.put(ctx.requestId, ctx.futures);

        int safePollRecords = request.getPollRecords() > 0 ? request.getPollRecords() : 500;
        int safeTimeoutMs = request.getTimeoutMs() > 0 ? request.getTimeoutMs() : 30000;
        ctx.props = getKafkaProps(request.getKafkaAddress(), safePollRecords, safeTimeoutMs);
        ctx.consumer = new KafkaConsumer<>(ctx.props);

        System.out.println(" [prepareSearch] Kafka consumer created. PollRecords=" + safePollRecords + ", TimeoutMs=" + safeTimeoutMs);

        List<PartitionInfo> partitionInfos = ctx.consumer.partitionsFor(ctx.topic);
        if (partitionInfos == null || partitionInfos.isEmpty()) {
            System.out.println(" [prepareSearch] No partitions found for topic: " + ctx.topic);
            ctx.partitions = null;
            return ctx;
        }

        ctx.partitions = new ArrayList<>();
        for (PartitionInfo p : partitionInfos) {
            ctx.partitions.add(new TopicPartition(p.topic(), p.partition()));
        }

        System.out.println(" [prepareSearch] Found " + ctx.partitions.size() + " partitions for topic: " + ctx.topic);

        ctx.consumer.assign(ctx.partitions);
        ctx.beginningOffsets = ctx.consumer.beginningOffsets(ctx.partitions);
        ctx.endOffsets = ctx.consumer.endOffsets(ctx.partitions);

        System.out.println(" [prepareSearch] Offset info fetched. Assigned partitions: " + ctx.partitions);

        System.out.println(" [prepareSearch] Consumer timeout configs:");
        System.out.println("  request.timeout.ms       = " + ctx.props.getProperty("request.timeout.ms"));
        System.out.println("  session.timeout.ms       = " + ctx.props.getProperty("session.timeout.ms"));
        System.out.println("  default.api.timeout.ms   = " + ctx.props.getProperty("default.api.timeout.ms"));
        System.out.println("  max.poll.interval.ms     = " + ctx.props.getProperty("max.poll.interval.ms"));

        return ctx;
    }



    @PostMapping
    public List<String> JSONSearch(@RequestBody SearchRequest request) throws InterruptedException, ExecutionException {
        System.out.println("[JSONSearch] Search request received for topic: " + request.getTopic());

        SearchContext ctx = prepareSearch(request);
        if (ctx.partitions == null) {
            System.out.println("[JSONSearch] Topic not found: " + request.getTopic());
            return Collections.singletonList("Topic not found.");
        }

        if ("last".equalsIgnoreCase(ctx.mode)) {

            System.out.println("[JSONSearch] Mode 'last' with executor.");

            ExecutorService executor = Executors.newSingleThreadExecutor();

            Callable<List<String>> task = () -> searchLastRecordJSON(
                    ctx.topic, ctx.partitions, ctx.props, request.getFilters()
            );

            ctx.futures.add(executor.submit(task));

            List<String> results = new ArrayList<>();
            for (Future<List<String>> f : ctx.futures) {
                List<String> partial = f.get();
                results.addAll(partial);
                if (results.size() >= ctx.maxResults) {
                    System.out.println("[JSONSearch] Max result limit reached. Truncating results.");
                    break;
                }
            }
            executor.shutdownNow();
            return results;

        }

        if ("copy".equalsIgnoreCase(ctx.mode)) {
            ExecutorService executor = Executors.newSingleThreadExecutor();

            Callable<List<String>> task = () -> copyMode(request);
            ctx.futures.add(executor.submit(task));

            List<String> results = new ArrayList<>();
            for (Future<List<String>> f : ctx.futures) {
                List<String> partial = f.get();
                results.addAll(partial);
                if (results.size() >= ctx.maxResults) {
                    System.out.println("[JSONSearch] Max result limit reached. Truncating results.");
                    break;
                }
            }
            executor.shutdownNow();
            return results;
        }

        if ("date".equalsIgnoreCase(ctx.mode)) {
            ExecutorService executor = Executors.newSingleThreadExecutor();

            Callable<List<String>> task = () -> DateMode(request);
            ctx.futures.add(executor.submit(task));

            List<String> results = new ArrayList<>();
            for (Future<List<String>> f : ctx.futures) {
                List<String> partial = f.get();
                results.addAll(partial);
                if (results.size() >= ctx.maxResults) {
                    System.out.println("[JSONSearch] Max result limit reached. Truncating results.");
                    break;
                }
            }
            executor.shutdownNow();
            return results;
        }

        ctx.consumer.assign(ctx.partitions);
        Map<TopicPartition, Long> beginningOffsets = ctx.consumer.beginningOffsets(ctx.partitions);
        Map<TopicPartition, Long> endOffsets = ctx.consumer.endOffsets(ctx.partitions);

        int threads = request.getThreads() > 0 ? request.getThreads() : 4;
        System.out.println("[JSONSearch] Starting parallel processing with " + threads + " threads.");

        ExecutorService executor = Executors.newFixedThreadPool(threads);

        for (TopicPartition tp : ctx.partitions) {

            long start = beginningOffsets.get(tp);
            long end = endOffsets.get(tp);
            if (end < start) continue;

            if ("manual".equalsIgnoreCase(ctx.mode) && request.getStartOffset() != null && request.getEndOffset() != null) {
                start = request.getStartOffset() >= start ? request.getStartOffset() : start;
                end = request.getEndOffset() <= end ? request.getEndOffset() : end;
                System.out.println("[JSONSearch] Manual mode active. Range adjusted: start=" + start + ", end=" + end);
            }

            if ("lastN".equalsIgnoreCase(ctx.mode) && ctx.lastN != null) {
                start = Math.max(end - ctx.lastN + 1, start);
                System.out.println("[JSONSearch] lastN mode active. New start offset: " + start);
            }

            long rangeSize = Math.max(1, (end - start + 1) / threads);
            System.out.println("[JSONSearch] Partition " + tp.partition() + " - start=" + start + ", end=" + end + ", rangeSize=" + rangeSize);

            for (int i = 0; i < threads; i++) {
                long partStart = start + i * rangeSize;
                long partEnd = (i == threads - 1) ? end : Math.min(partStart + rangeSize - 1, end);

                System.out.println("[JSONSearch] Submitting task for partition " + tp.partition() + ", range [" + partStart + " - " + partEnd + "]");
                Callable<List<String>> task = () -> consumePartitionRangeJSON(
                        ctx.topic, tp.partition(), partStart, partEnd, ctx.props, request.getFilters(), ctx.maxResults
                );
                ctx.futures.add(executor.submit(task));
            }
        }

        List<String> results = new ArrayList<>();
        for (Future<List<String>> f : ctx.futures) {
            List<String> partial = f.get();
            results.addAll(partial);
            if (results.size() >= ctx.maxResults) {
                System.out.println("[JSONSearch] Max result limit reached. Truncating results.");
                break;
            }
        }

        executor.shutdownNow();
        System.out.println("[JSONSearch] Search completed. Total results: " + results.size());

        return results.size() > ctx.maxResults ? results.subList(0, ctx.maxResults) : results;
    }






    @PostMapping("/simple-string-search")
    public List<String> stringSearch(@RequestBody SearchRequest request) throws InterruptedException, ExecutionException {
        System.out.println("[stringSearch] Search request received for topic: " + request.getTopic());

        List<String> rawFilters = request.getRawFilters();
        SearchContext ctx = prepareSearch(request);

        if (ctx.partitions == null) {
            System.out.println("[stringSearch] Topic not found: " + request.getTopic());
            return Collections.singletonList("Topic not found.");
        }

        if ("last".equalsIgnoreCase(ctx.mode)) {

            System.out.println("[stringSearch] Mode 'last' with executor.");

            ExecutorService executor = Executors.newSingleThreadExecutor();

            Callable<List<String>> task = () -> searchLastRecordString(
                    ctx.topic, ctx.partitions, ctx.props, rawFilters
            );

            ctx.futures.add(executor.submit(task));

            List<String> results = new ArrayList<>();
            for (Future<List<String>> f : ctx.futures) {
                List<String> partial = f.get();
                results.addAll(partial);
                if (results.size() >= ctx.maxResults) {
                    System.out.println("[stringSearch] Max result limit reached. Truncating results.");
                    break;
                }
            }
            executor.shutdownNow();
            return results;

        }

        if ("copy".equalsIgnoreCase(ctx.mode)) {
            ExecutorService executor = Executors.newSingleThreadExecutor();

            Callable<List<String>> task = () -> copyMode(request);
            ctx.futures.add(executor.submit(task));

            List<String> results = new ArrayList<>();
            for (Future<List<String>> f : ctx.futures) {
                List<String> partial = f.get();
                results.addAll(partial);
                if (results.size() >= ctx.maxResults) {
                    System.out.println("[stringSearch] Max result limit reached. Truncating results.");
                    break;
                }
            }
            executor.shutdownNow();
            return results;
        }

        if ("date".equalsIgnoreCase(ctx.mode)) {
            ExecutorService executor = Executors.newSingleThreadExecutor();

            Callable<List<String>> task = () -> DateMode(request);
            ctx.futures.add(executor.submit(task));

            List<String> results = new ArrayList<>();
            for (Future<List<String>> f : ctx.futures) {
                List<String> partial = f.get();
                results.addAll(partial);
                if (results.size() >= ctx.maxResults) {
                    System.out.println("[stringSearch] Max result limit reached. Truncating results.");
                    break;
                }
            }
            executor.shutdownNow();
            return results;
        }

        int threads = request.getThreads() > 0 ? request.getThreads() : 4;
        System.out.println("[stringSearch] Starting search with " + threads + " threads.");

        ExecutorService executor = Executors.newFixedThreadPool(threads);

        for (TopicPartition tp : ctx.partitions) {
            long start = ctx.beginningOffsets.get(tp);
            long end = ctx.endOffsets.get(tp);
            if (end < start) continue;

            if ("manual".equalsIgnoreCase(ctx.mode) && request.getStartOffset() != null && request.getEndOffset() != null) {
                start = request.getStartOffset() >= start ? request.getStartOffset() : start;
                end = request.getEndOffset() <= end ? request.getEndOffset() : end;
                System.out.println("[stringSearch] Manual offset range set for partition " + tp.partition() + ": start=" + start + ", end=" + end);
            }

            if ("lastN".equalsIgnoreCase(ctx.mode) && ctx.lastN != null) {
                start = Math.max(end - ctx.lastN + 1, start);
                System.out.println("[stringSearch] lastN mode active. Adjusted start offset for partition " + tp.partition() + ": " + start);
            }

            long rangeSize = Math.max(1, (end - start + 1) / threads);
            System.out.println("[stringSearch] Partition " + tp.partition() + " - start=" + start + ", end=" + end + ", rangeSize=" + rangeSize);

            for (int i = 0; i < threads; i++) {
                long partStart = start + i * rangeSize;
                long partEnd = (i == threads - 1) ? end : Math.min(partStart + rangeSize - 1, end);

                System.out.println("[stringSearch] Submitting task for partition " + tp.partition() + ", range [" + partStart + " - " + partEnd + "]");
                Callable<List<String>> task = () -> consumePartitionRangeString(
                        ctx.topic, tp.partition(), partStart, partEnd, ctx.props, rawFilters, ctx.maxResults
                );
                ctx.futures.add(executor.submit(task));
            }
        }

        List<String> results = new ArrayList<>();
        for (Future<List<String>> f : ctx.futures) {
            results.addAll(f.get());
            if (results.size() >= ctx.maxResults) {
                System.out.println("[stringSearch] Max results reached. Stopping collection.");
                break;
            }
        }

        executor.shutdownNow();
        System.out.println("[stringSearch] Search completed. Total results: " + results.size());

        return results.size() > ctx.maxResults ? results.subList(0, ctx.maxResults) : results;
    }


    @PostMapping("/pattern-search")
    public List<String> patternSearch(@RequestBody SearchRequest request) throws InterruptedException, ExecutionException {
        System.out.println("[patternSearch] Search request received for topic: " + request.getTopic());

        List<String> patternStrings = request.getRawFilters();
        List<Pattern> patterns = new ArrayList<>();
        for (String p : patternStrings) {
            patterns.add(Pattern.compile(p, Pattern.CASE_INSENSITIVE));
        }
        System.out.println("[patternSearch] Compiled " + patterns.size() + " regex pattern(s).");

        SearchContext ctx = prepareSearch(request);
        if (ctx.partitions == null) {
            System.out.println("[patternSearch] Topic not found: " + request.getTopic());
            return Collections.singletonList("Topic not found.");
        }

        if ("last".equalsIgnoreCase(ctx.mode)) {

            System.out.println("[patternSearch] Mode 'last' with executor.");

            ExecutorService executor = Executors.newSingleThreadExecutor();

            Callable<List<String>> task = () -> searchLastRecordPattern(
                    ctx.topic, ctx.partitions, ctx.props, patterns
            );

            ctx.futures.add(executor.submit(task));

            List<String> results = new ArrayList<>();
            for (Future<List<String>> f : ctx.futures) {
                List<String> partial = f.get();
                results.addAll(partial);
                if (results.size() >= ctx.maxResults) {
                    System.out.println("[patternSearch] Max result limit reached. Truncating results.");
                    break;
                }
            }
            executor.shutdownNow();
            return results;

        }

        if ("copy".equalsIgnoreCase(ctx.mode)) {
            ExecutorService executor = Executors.newSingleThreadExecutor();

            Callable<List<String>> task = () -> copyMode(request);
            ctx.futures.add(executor.submit(task));

            List<String> results = new ArrayList<>();
            for (Future<List<String>> f : ctx.futures) {
                List<String> partial = f.get();
                results.addAll(partial);
                if (results.size() >= ctx.maxResults) {
                    System.out.println("[patternSearch] Max result limit reached. Truncating results.");
                    break;
                }
            }
            executor.shutdownNow();
            return results;
        }

        if ("date".equalsIgnoreCase(ctx.mode)) {
            ExecutorService executor = Executors.newSingleThreadExecutor();

            Callable<List<String>> task = () -> DateMode(request);
            ctx.futures.add(executor.submit(task));

            List<String> results = new ArrayList<>();
            for (Future<List<String>> f : ctx.futures) {
                List<String> partial = f.get();
                results.addAll(partial);
                if (results.size() >= ctx.maxResults) {
                    System.out.println("[patternSearch] Max result limit reached. Truncating results.");
                    break;
                }
            }
            executor.shutdownNow();
            return results;
        }

        int threads = request.getThreads() > 0 ? request.getThreads() : 4;
        System.out.println("[patternSearch] Starting search with " + threads + " threads.");

        ExecutorService executor = Executors.newFixedThreadPool(threads);

        for (TopicPartition tp : ctx.partitions) {
            long start = ctx.beginningOffsets.get(tp);
            long end = ctx.endOffsets.get(tp);
            if (end < start) continue;

            if ("manual".equalsIgnoreCase(ctx.mode) && request.getStartOffset() != null && request.getEndOffset() != null) {
                start = request.getStartOffset() >= start ? request.getStartOffset() : start;
                end = request.getEndOffset() <= end ? request.getEndOffset() : end;
                System.out.println("[patternSearch] Manual offset range set for partition " + tp.partition() + ": start=" + start + ", end=" + end);
            }

            if ("lastN".equalsIgnoreCase(ctx.mode) && ctx.lastN != null) {
                start = Math.max(end - ctx.lastN + 1, start);
                System.out.println("[patternSearch] lastN mode active. Adjusted start offset for partition " + tp.partition() + ": " + start);
            }

            long rangeSize = Math.max(1, (end - start + 1) / threads);
            System.out.println("[patternSearch] Partition " + tp.partition() + " - start=" + start + ", end=" + end + ", rangeSize=" + rangeSize);

            for (int i = 0; i < threads; i++) {
                long partStart = start + i * rangeSize;
                long partEnd = (i == threads - 1) ? end : Math.min(partStart + rangeSize - 1, end);

                System.out.println("[patternSearch] Submitting task for partition " + tp.partition() + ", range [" + partStart + " - " + partEnd + "]");
                final List<Pattern> finalPatterns = patterns;
                Callable<List<String>> task = () -> consumePartitionRangePattern(
                        ctx.topic, tp.partition(), partStart, partEnd, ctx.props, finalPatterns, ctx.maxResults
                );
                ctx.futures.add(executor.submit(task));
            }
        }

        List<String> results = new ArrayList<>();
        for (Future<List<String>> f : ctx.futures) {
            results.addAll(f.get());
            if (results.size() >= ctx.maxResults) {
                System.out.println("[patternSearch] Max results reached. Truncating result list.");
                break;
            }
        }

        executor.shutdownNow();
        System.out.println("[patternSearch] Search completed. Total results: " + results.size());

        return results.size() > ctx.maxResults ? results.subList(0, ctx.maxResults) : results;
    }



    private List<String> searchLastRecordJSON(String topic,
                                              List<TopicPartition> partitions,
                                              Properties props,
                                              Map<String, String> filters) {
        boolean isCancelled = false;
        int stepSize = 500;
        System.out.println("[searchLastRecordJSON] Starting last record search for topic: " + topic);
        System.out.println("[searchLastRecordJSON] Filters applied: " + filters);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
            Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(partitions);

            for (TopicPartition tp : partitions) {
                long start = beginningOffsets.get(tp);
                long end = endOffsets.get(tp);
                long fromOffset = end;

                System.out.println("[searchLastRecordJSON] Searching in partition " + tp.partition() + " from offset " + start + " to " + end);

                while (fromOffset > start) {
                    long batchStart = Math.max(start, fromOffset - stepSize);
                    System.out.println("[searchLastRecordJSON] Polling range [" + batchStart + " - " + (fromOffset - 1) + "]");

                    consumer.assign(Collections.singletonList(tp));
                    consumer.seek(tp, batchStart);

                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));
                    List<ConsumerRecord<String, String>> matchedRecords = new ArrayList<>();

                    for (ConsumerRecord<String, String> record : records.records(tp)) {
                        if (record.offset() >= end) continue;
                        if (matchesFiltersJSON(record.value(), filters)) {
                            matchedRecords.add(record);
                        }
                    }

                    if (!matchedRecords.isEmpty()) {
                        System.out.println("[searchLastRecordJSON] Found " + matchedRecords.size() + " matching record(s). Returning the latest one.");
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
                            System.out.println("[searchLastRecordJSON] Failed to parse JSON. Returning raw value.");
                            return List.of(String.format("{\"offset\": %d, \"value\": \"%s\"}",
                                    maxOffsetRecord.offset(), maxOffsetRecord.value()));
                        }
                    }

                    fromOffset = batchStart;
                }

                System.out.println("[searchLastRecordJSON] No matching records found in partition " + tp.partition());
            }
        } catch (Exception e) {
            System.out.println("[searchLastRecordJSON] Error occurred: " + e.getMessage());
            e.printStackTrace();
        }

        System.out.println("[searchLastRecordJSON] Search finished. No matching records found.");
        return List.of();
    }



    private List<String> searchLastRecordString(String topic,
                                                List<TopicPartition> partitions,
                                                Properties props,
                                                List<String> rawFilters) {
        int stepSize = 500;
        System.out.println("[searchLastRecordString] Starting last record search for topic: " + topic);
        System.out.println("[searchLastRecordString] Filters applied: " + rawFilters);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
            Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(partitions);

            for (TopicPartition tp : partitions) {
                long start = beginningOffsets.get(tp);
                long end = endOffsets.get(tp);
                long fromOffset = end;

                System.out.println("[searchLastRecordString] Searching in partition " + tp.partition() + " from offset " + start + " to " + end);

                while (fromOffset > start) {
                    long batchStart = Math.max(start, fromOffset - stepSize);
                    System.out.println("[searchLastRecordString] Polling range [" + batchStart + " - " + (fromOffset - 1) + "]");

                    consumer.assign(Collections.singletonList(tp));
                    consumer.seek(tp, batchStart);

                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));
                    List<ConsumerRecord<String, String>> matchedRecords = new ArrayList<>();

                    for (ConsumerRecord<String, String> record : records.records(tp)) {
                        if (record.offset() >= end) continue;
                        if (matchesFiltersString(record.value(), rawFilters)) {
                            matchedRecords.add(record);
                        }
                    }

                    if (!matchedRecords.isEmpty()) {
                        System.out.println("[searchLastRecordString] Found " + matchedRecords.size() + " matching record(s). Returning the latest one.");
                        ConsumerRecord<String, String> maxOffsetRecord = Collections.max(
                                matchedRecords,
                                Comparator.comparingLong(ConsumerRecord::offset)
                        );
                        return List.of(String.format("{\"offset\": %d, \"value\": \"%s\"}",
                                maxOffsetRecord.offset(), maxOffsetRecord.value()));
                    }

                    fromOffset = batchStart;
                }

                System.out.println("[searchLastRecordString] No matching records found in partition " + tp.partition());
            }
        } catch (Exception e) {
            System.out.println("[searchLastRecordString] Error occurred: " + e.getMessage());
            e.printStackTrace();
        }

        System.out.println("[searchLastRecordString] Search finished. No matching records found.");
        return List.of();
    }


    private List<String> searchLastRecordPattern(String topic,
                                                 List<TopicPartition> partitions,
                                                 Properties props,
                                                 List<Pattern> patterns) {
        int stepSize = 500;
        System.out.println("[searchLastRecordPattern] Starting last record pattern search for topic: " + topic);
        System.out.println("[searchLastRecordPattern] Total compiled patterns: " + patterns.size());

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
            Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(partitions);

            for (TopicPartition tp : partitions) {
                long start = beginningOffsets.get(tp);
                long end = endOffsets.get(tp);
                long fromOffset = end;

                System.out.println("[searchLastRecordPattern] Searching in partition " + tp.partition() + " from offset " + start + " to " + end);

                while (fromOffset > start) {
                    long batchStart = Math.max(start, fromOffset - stepSize);
                    System.out.println("[searchLastRecordPattern] Polling range [" + batchStart + " - " + (fromOffset - 1) + "]");

                    consumer.assign(Collections.singletonList(tp));
                    consumer.seek(tp, batchStart);

                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));
                    List<ConsumerRecord<String, String>> matchedRecords = new ArrayList<>();

                    for (ConsumerRecord<String, String> record : records.records(tp)) {
                        if (record.offset() >= end) continue;

                        if (matchesPatterns(record.value(), patterns)) {
                            matchedRecords.add(record);
                        }
                    }

                    if (!matchedRecords.isEmpty()) {
                        System.out.println("[searchLastRecordPattern] Found " + matchedRecords.size() + " matching record(s). Returning the latest one.");
                        ConsumerRecord<String, String> maxOffsetRecord = Collections.max(
                                matchedRecords,
                                Comparator.comparingLong(ConsumerRecord::offset)
                        );
                        return List.of(String.format("{\"offset\": %d, \"value\": \"%s\"}",
                                maxOffsetRecord.offset(), maxOffsetRecord.value()));
                    }

                    fromOffset = batchStart;
                }

                System.out.println("[searchLastRecordPattern] No matching records found in partition " + tp.partition());
            }
        } catch (Exception e) {
            System.out.println("[searchLastRecordPattern] Error occurred: " + e.getMessage());
            e.printStackTrace();
        }

        System.out.println("[searchLastRecordPattern] Search finished. No matching records found.");
        return List.of();
    }

    private List<String> consumePartitionRangeJSON(String topic, int partition, long startOffset, long endOffset,
                                                   Properties props, Map<String, String> filters, int maxResults) {
        System.out.println("[consumePartitionRangeJSON] Starting JSON consumption for topic=" + topic +
                ", partition=" + partition + ", offsets=[" + startOffset + " - " + endOffset + "], maxResults=" + maxResults);
        System.out.println("[consumePartitionRangeJSON] Filters: " + filters);

        List<String> foundRecords = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            TopicPartition tp = new TopicPartition(topic, partition);
            consumer.assign(List.of(tp));
            consumer.seek(tp, startOffset);

            long currentOffset = startOffset;

            while (currentOffset <= endOffset && foundRecords.size() < maxResults) {
                long pollTimeout = props.containsKey("request.timeout.ms")
                        ? Long.parseLong(props.getProperty("request.timeout.ms"))
                        : 30000;

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(pollTimeout));
                if (records.isEmpty()) {
                    System.out.println("[consumePartitionRangeJSON] No records received at offset " + currentOffset);
                    break;
                }

                if(!Thread.currentThread().isInterrupted()){
                    for (ConsumerRecord<String, String> record : records) {
                        if (record.offset() > endOffset) break;

                        if (matchesFiltersJSON(record.value(), filters)) {
                            try {
                                JsonNode valueNode = objectMapper.readTree(record.value());
                                ObjectNode resultNode = objectMapper.createObjectNode();
                                resultNode.put("offset", record.offset());
                                resultNode.set("value", valueNode);
                                foundRecords.add(objectMapper.writeValueAsString(resultNode));
                                System.out.println("[consumePartitionRangeJSON] Match found at offset " + record.offset());
                            } catch (Exception e) {
                                System.out.println("[consumePartitionRangeJSON] Failed to parse JSON at offset " + record.offset());
                                foundRecords.add(String.format("{\"offset\": %d, \"value\": \"%s\"}",
                                        record.offset(), record.value()));
                            }

                            if (foundRecords.size() >= maxResults) {
                                System.out.println("[consumePartitionRangeJSON] Reached maxResults limit (" + maxResults + ")");
                                break;
                            }
                        }
                        currentOffset = record.offset() + 1;
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("[consumePartitionRangeJSON] Error occurred: " + e.getMessage());
            e.printStackTrace();
        }

        System.out.println("[consumePartitionRangeJSON] Finished. Total matches: " + foundRecords.size());
        return foundRecords;
    }


    private List<String> consumePartitionRangeString(String topic, int partition, long startOffset, long endOffset,
                                                     Properties props, List<String> rawFilters, int maxResults) {
        System.out.println("[consumePartitionRangeString] Starting string consumption for topic=" + topic +
                ", partition=" + partition + ", offsets=[" + startOffset + " - " + endOffset + "], maxResults=" + maxResults);
        System.out.println("[consumePartitionRangeString] Filters: " + rawFilters);

        List<String> foundRecords = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            TopicPartition tp = new TopicPartition(topic, partition);
            consumer.assign(List.of(tp));
            consumer.seek(tp, startOffset);
            long currentOffset = startOffset;

            while (currentOffset <= endOffset && foundRecords.size() < maxResults) {
                long pollTimeout = props.containsKey("request.timeout.ms")
                        ? Long.parseLong(props.getProperty("request.timeout.ms"))
                        : 30000;

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(pollTimeout));
                if (records.isEmpty()) {
                    System.out.println("[consumePartitionRangeString] No records received at offset " + currentOffset);
                    break;
                }

                if(!Thread.currentThread().isInterrupted()){
                    for (ConsumerRecord<String, String> record : records) {
                        if (record.offset() > endOffset) break;

                        if (matchesFiltersString(record.value(), rawFilters)) {
                            foundRecords.add(String.format("{\"offset\": %d, \"value\": \"%s\"}", record.offset(), record.value()));
                            System.out.println("[consumePartitionRangeString] Match found at offset " + record.offset());

                            if (foundRecords.size() >= maxResults) {
                                System.out.println("[consumePartitionRangeString] Reached maxResults limit (" + maxResults + ")");
                                break;
                            }
                        }

                        currentOffset = record.offset() + 1;
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("[consumePartitionRangeString] Error occurred: " + e.getMessage());
            e.printStackTrace();
        }

        System.out.println("[consumePartitionRangeString] Finished. Total matches: " + foundRecords.size());
        return foundRecords;
    }


    private List<String> consumePartitionRangePattern(String topic, int partition, long startOffset, long endOffset,
                                                      Properties props, List<Pattern> patterns, int maxResults) {
        System.out.println("[consumePartitionRangePattern] Starting pattern consumption for topic=" + topic +
                ", partition=" + partition + ", offsets=[" + startOffset + " - " + endOffset + "], maxResults=" + maxResults);
        System.out.println("[consumePartitionRangePattern] Patterns: " + patterns);

        List<String> foundRecords = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            TopicPartition tp = new TopicPartition(topic, partition);
            consumer.assign(List.of(tp));
            consumer.seek(tp, startOffset);
            long currentOffset = startOffset;

            while (currentOffset <= endOffset && foundRecords.size() < maxResults) {
                long pollTimeout = props.containsKey("request.timeout.ms")
                        ? Long.parseLong(props.getProperty("request.timeout.ms"))
                        : 30000;

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(pollTimeout));
                if (records.isEmpty()) {
                    System.out.println("[consumePartitionRangePattern] No records received at offset " + currentOffset);
                    break;
                }

                if (!Thread.currentThread().isInterrupted()) {
                    for (ConsumerRecord<String, String> record : records) {
                        if (record.offset() > endOffset) break;

                        if (matchesPatterns(record.value(), patterns)) {
                            foundRecords.add(String.format("{\"offset\": %d, \"value\": \"%s\"}", record.offset(), record.value()));
                            System.out.println("[consumePartitionRangePattern] Match found at offset " + record.offset());

                            if (foundRecords.size() >= maxResults) {
                                System.out.println("[consumePartitionRangePattern] Reached maxResults limit (" + maxResults + ")");
                                break;
                            }
                        }

                        currentOffset = record.offset() + 1;
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("[consumePartitionRangePattern] Error occurred: " + e.getMessage());
            e.printStackTrace();
        }

        System.out.println("[consumePartitionRangePattern] Finished. Total matches: " + foundRecords.size());
        return foundRecords;
    }

    private boolean matchesFiltersJSON(String json, Map<String, String> filters) {
        if (filters == null || filters.isEmpty()) return true;
        try {
            JsonNode node = objectMapper.readTree(json);
            for (Map.Entry<String, String> entry : filters.entrySet()) {
                JsonNode val = node.get(entry.getKey());
                if (val == null || !val.asText().equals(entry.getValue())) {
                    System.out.println("[matchesFiltersJSON] Mismatch on key: " + entry.getKey() + ", expected: " + entry.getValue());
                    return false;
                }
            }
            return true;
        } catch (Exception e) {
            System.out.println("[matchesFiltersJSON] Invalid JSON format or parse error: " + e.getMessage());
            return false;
        }
    }


    private boolean matchesFiltersString(String value, List<String> rawFilters) {
        if (rawFilters == null || rawFilters.isEmpty()) return true;
        if (value == null) return false;

        for (String filter : rawFilters) {
            if (!value.toLowerCase().contains(filter.toLowerCase().trim())) {
                System.out.println("[matchesFiltersString] Mismatch: value does not contain filter -> " + filter);
                return false;
            }
        }

        System.out.println("[matchesFiltersString] All filters matched.");
        return true;
    }


    private boolean matchesPatterns(String value, List<Pattern> patterns) {
        if (value == null || patterns == null || patterns.isEmpty()) return true;

        for (Pattern pattern : patterns) {
            Matcher matcher = pattern.matcher(value);
            if (!matcher.find()) {
                System.out.println("[matchesPatterns] Mismatch: value does not match pattern -> " + pattern.pattern());
                return false;
            }
        }

        System.out.println("[matchesPatterns] All patterns matched.");
        return true;
    }


    private List<String> copyMode(SearchRequest request) {
        List<String> copiedMessages = new ArrayList<>();

        String sourceTopic = request.getTopic();
        String targetTopic = request.getTargetTopic();
        String kafkaAddress = request.getKafkaAddress();
        String targetKafka = request.getTargetKafka();
        long startOffset = request.getStartOffset();
        long endOffset = request.getEndOffset();
        int partitionId = request.getPartition();
        String filterMode = request.getFilterMode();

        Map<String, String> filters = request.getFilters();
        List<String> rawFilters = request.getRawFilters();

        System.out.println("[copyMode] Starting copy from topic=" + sourceTopic + ", partition=" + partitionId +
                ", offsets=[" + startOffset + " - " + endOffset + "] to topic=" + targetTopic +
                " using filterMode=" + filterMode);

        Properties props = getKafkaProps(kafkaAddress);
        try (
                KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
                KafkaProducer<String, String> producer = new KafkaProducer<>(getKafkaProducerProps(targetKafka));
        ) {
            TopicPartition partition = new TopicPartition(sourceTopic, partitionId);
            consumer.assign(Collections.singletonList(partition));
            consumer.seek(partition, startOffset);

            boolean done = false;
            while (!done) {
                long pollTimeout = props.containsKey("request.timeout.ms")
                        ? Long.parseLong(props.getProperty("request.timeout.ms"))
                        : 30000;

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(pollTimeout));

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
                        System.out.println("[copyMode] Copied message at offset " + record.offset());
                    }
                }
            }

            producer.flush();
            System.out.println("[copyMode] Copy operation completed. Total copied: " + copiedMessages.size());
        } catch (Exception e) {
            System.out.println("[copyMode] Error occurred: " + e.getMessage());
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

        System.out.println("[DateMode] Starting date-based binary search in topic=" + topic + ", partition=" + partition);
        System.out.println("[DateMode] Searching for key='" + key + "' with date prefix='" + expectedDatePrefix + "'");

        Properties props = getKafkaProps(kafkaAddress);
        List<String> results = new ArrayList<>();
        long resultOffset = -1;

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            TopicPartition tp = new TopicPartition(topic, partition);
            consumer.assign(List.of(tp));
            consumer.seekToBeginning(List.of(tp));
            long low = consumer.position(tp);

            consumer.seekToEnd(List.of(tp));
            long high = consumer.position(tp) - 1;

            System.out.println("[DateMode] Offset range: [" + low + " - " + high + "]");



            long closestAfterOffset = -1;
            while (low <= high) {
                long mid = (low + high) / 2;
                consumer.seek(tp, mid);

                long pollTimeout = props.containsKey("request.timeout.ms")
                        ? Long.parseLong(props.getProperty("request.timeout.ms"))
                        : 30000;
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(pollTimeout));
                Optional<ConsumerRecord<String, String>> recordOpt = records.records(tp).stream()
                        .filter(r -> r.offset() == mid)
                        .findFirst();

                if (recordOpt.isEmpty()) {
                    System.out.println("[DateMode] No record at offset " + mid + ". Moving right.");
                    low = mid + 1;
                    continue;
                }

                ConsumerRecord<String, String> record = recordOpt.get();
                JsonNode json = objectMapper.readTree(record.value());
                JsonNode dateNode = json.get(key);

                if (dateNode == null || dateNode.asText().length() < 14) {
                    System.out.println("[DateMode] Invalid or missing full timestamp at offset " + mid + ". Skipping.");
                    high = mid - 1;
                    continue;
                }

                String datePrefix = dateNode.asText().substring(0, 14);
                int cmp = datePrefix.compareTo(expectedDatePrefix);

                System.out.println("[DateMode] Offset " + mid + " has date prefix '" + datePrefix + "', compareTo=" + cmp);

                if (cmp < 0) {
                    low = mid + 1;
                } else {
                    if (cmp == 0) {
                        resultOffset = mid;
                        System.out.println("[DateMode] Match found at offset " + mid);
                    }else{
                        closestAfterOffset = mid;
                    }
                    high = mid - 1;
                }
            }

            if (resultOffset == -1 && closestAfterOffset != -1) {
                resultOffset = closestAfterOffset;
                System.out.println("[DateMode] No exact match found. Using closest after offset " + resultOffset);
            }


            if (resultOffset != -1) {
                consumer.seek(tp, resultOffset);
                int fetchedCount = 0;
                long maxOffset = resultOffset + 100;

                while (fetchedCount < 20) {
                    ConsumerRecords<String, String> fetchedRecords = consumer.poll(Duration.ofMillis(5000));
                    if (fetchedRecords.isEmpty()) {
                        break;
                    }

                    for (ConsumerRecord<String, String> record : fetchedRecords.records(tp)) {
                        if (record.offset() >= maxOffset) {
                            break;
                        }

                        JsonNode jsonNode = objectMapper.readTree(record.value());
                        ObjectNode result = objectMapper.createObjectNode();
                        result.put("offset", record.offset());
                        result.set("value", jsonNode);
                        results.add(result.toString());
                        fetchedCount++;

                        if (fetchedCount >= 20) break;
                    }
                }

                System.out.println("[DateMode] Retrieved " + results.size() + " records starting from offset " + resultOffset);
            } else {
                System.out.println("[DateMode] No matching record found for given date prefix.");
            }

        } catch (Exception e) {
            System.out.println("[DateMode] Error occurred: " + e.getMessage());
            e.printStackTrace();
        }


        System.out.println("[DateMode] Search complete. Matches: " + results.size());
        return results;
    }



}