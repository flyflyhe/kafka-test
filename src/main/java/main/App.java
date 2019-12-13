package main;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

public class App {
    public static void main(String[] args) throws Exception {
        System.out.println("开始获取");
        Properties properties = new Properties();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);
        properties.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1);
        properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 1000);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        Consumer<Long, String> consumer = new KafkaConsumer<Long, String>(properties);
        consumer.subscribe(Collections.singletonList("test"));
        System.out.println("输出");
        while (true) {
            ConsumerRecords<Long, String> records = consumer.poll(Duration.ofSeconds(10));

            AtomicLong lastOffset = new AtomicLong();
            records.forEach(record -> {
                System.out.printf("\n\roffset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
                lastOffset.set(record.offset());
            });
            System.out.println("lastOffset read: " + lastOffset);
            Thread.sleep(5);
        }
    }
}
