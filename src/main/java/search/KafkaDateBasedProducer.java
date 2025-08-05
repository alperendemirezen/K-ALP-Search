package search;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class KafkaDateBasedProducer {
    private static final String[] cities = {
            "Istanbul", "Ankara", "Izmir", "Bursa", "Antalya",
            "Konya", "Adana", "Gaziantep", "Mersin", "Kayseri",
            "Samsun", "Eskişehir", "Trabzon", "Diyarbakır", "Erzurum",
            "Van", "Malatya", "Manisa", "Sakarya", "Balıkesir"
    };

    private static final String[] statuses = {
            "dolu", "boş", "bakımda", "gecikmeli", "iptal", "hizmet dışı"
    };

    private static final Random random = new Random();

    public static void main(String[] args) throws Exception {
        System.out.println("📦 KafkaDateBasedProducer başladı. 10M veri (tek partition, zaman sıralı, 5 güne yayılmış)...");

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        ObjectMapper mapper = new ObjectMapper();

        String topic = "testing-dated";
        int totalRecords = 10_000_000;
        int partition = 0;

        DateTimeFormatter timestampFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

        int secondsToCover = 432_000;
        int recordsPerSecond = totalRecords / secondsToCover;
        int remainingRecords = totalRecords;

        LocalDateTime currentTime = LocalDateTime.of(2025, 7, 1, 0, 0, 0);
        int id = 0;

        for (int s = 0; s < secondsToCover && remainingRecords > 0; s++) {
            String timestamp = timestampFormatter.format(currentTime);

            for (int i = 0; i < recordsPerSecond && remainingRecords > 0; i++) {
                Map<String, Object> record = new HashMap<>();
                record.put("id", id++);
                record.put("city", cities[random.nextInt(cities.length)]);
                record.put("status", statuses[random.nextInt(statuses.length)]);
                record.put("speed", 20 + random.nextInt(80));
                record.put("routeNumber", random.nextInt(100) + 1);
                record.put("timestamp", timestamp);

                String json = mapper.writeValueAsString(record);
                ProducerRecord<String, String> message = new ProducerRecord<>(topic, partition, null, json);
                producer.send(message);
                remainingRecords--;
            }

            if (id % 500_000 == 0) {
                System.out.println("📤 Gönderilen kayıt: " + id + " / " + totalRecords + " | Saat: " + timestamp);
            }

            currentTime = currentTime.plusSeconds(1);
        }

        producer.flush();
        producer.close();
        System.out.println("🎉 Toplam 10M veri başarıyla gönderildi.");
    }
}
