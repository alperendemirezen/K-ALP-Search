package search;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.*;

public class KafkaDataProducer {
    private static final String[] cities = {"Istanbul", "Ankara", "Izmir", "Bursa", "Antalya"};
    private static final String[] statuses = {"dolu", "boş", "bakımda"};
    private static final Random random = new Random();

    public static void main(String[] args) throws Exception {
        System.out.println("💬 KafkaDataProducer başladı. Kafka'ya 20 milyon veri gönderilecek...");

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        ObjectMapper mapper = new ObjectMapper();

        int baseSize = 100_000;
        List<Map<String, Object>> baseData = new ArrayList<>();

        // Base veri oluştur (100K)
        for (int i = 0; i < baseSize; i++) {
            Map<String, Object> record = new HashMap<>();
            record.put("id", i);
            record.put("city", cities[i % cities.length]);
            record.put("speed", 30 + random.nextInt(70));
            record.put("direction", random.nextInt(361));
            record.put("latitude", 39.0 + random.nextDouble() * 7.0);
            record.put("longitude", 26.0 + random.nextDouble() * 8.0);
            record.put("status", statuses[random.nextInt(statuses.length)]);
            record.put("passengerCount", random.nextInt(100));
            record.put("routeNumber", random.nextInt(100) + 1);
            baseData.add(record);
        }

        int totalRecords = 20_000_000;
        int partitionCount = 4;
        int recordsPerPartition = totalRecords / partitionCount;

        for (int partition = 0; partition < partitionCount; partition++) {
            for (int i = 0; i < recordsPerPartition; i++) {
                int globalIndex = partition * recordsPerPartition + i;

                Map<String, Object> currentRecord = new HashMap<>(baseData.get(globalIndex % baseSize));
                currentRecord.put("timestamp", Instant.now().toString());

                String json = mapper.writeValueAsString(currentRecord);
                // Partition numarasını burada belirt (null yerine partition sayısı)
                ProducerRecord<String, String> record = new ProducerRecord<>("testing", partition, null, json);
                producer.send(record);

                if ((globalIndex + 1) % 1_000_000 == 0) {
                    System.out.println("✅ " + (globalIndex + 1) + " veri gönderildi.");
                }
            }
        }

        producer.flush();
        producer.close();

        System.out.println("🚀 Kafka'ya 20 milyon veri gönderimi tamamlandı.");
    }
}