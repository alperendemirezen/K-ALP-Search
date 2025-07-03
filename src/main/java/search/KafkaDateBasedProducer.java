package search;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class KafkaDateBasedProducer {
    private static final String[] cities = {"Istanbul", "Ankara", "Izmir", "Bursa", "Antalya"};
    private static final String[] statuses = {"dolu", "boş", "bakımda"};
    private static final Random random = new Random();

    public static void main(String[] args) throws Exception {
        System.out.println("📦 KafkaDateBasedProducer başladı. Kafka'ya farklı tarihlerle veri gönderiliyor...");

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        ObjectMapper mapper = new ObjectMapper();

        String topic = "testing-dated";
        int partitionCount = 4;

        // 5 gün için veri üret (örnek: 1-5 Temmuz 2025)
        LocalDate startDate = LocalDate.of(2025, 7, 1);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");

        int recordsPerDayPerPartition = 5000; // her gün, her partition'da 5K veri

        for (int partition = 0; partition < partitionCount; partition++) {
            for (int dayOffset = 0; dayOffset < 5; dayOffset++) {
                LocalDate currentDate = startDate.plusDays(dayOffset);
                String datePrefix = formatter.format(currentDate); // e.g., 20250701

                for (int i = 0; i < recordsPerDayPerPartition; i++) {
                    Map<String, Object> record = new HashMap<>();
                    record.put("id", partition * 1_000_000 + dayOffset * 10000 + i);
                    record.put("city", cities[i % cities.length]);
                    record.put("status", statuses[random.nextInt(statuses.length)]);
                    record.put("speed", 20 + random.nextInt(80));
                    record.put("routeNumber", random.nextInt(50) + 1);
                    record.put("timestamp", datePrefix + String.format("%06d", i)); // e.g., 20250702000017

                    String json = mapper.writeValueAsString(record);
                    ProducerRecord<String, String> message = new ProducerRecord<>(topic, partition, null, json);
                    producer.send(message);
                }

                System.out.println("✅ Partition " + partition + ", Gün " + datePrefix + " verileri gönderildi.");
            }
        }

        producer.flush();
        producer.close();
        System.out.println("🎉 Farklı tarihlerle veri gönderimi tamamlandı.");
    }
}