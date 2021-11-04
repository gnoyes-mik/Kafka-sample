import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Consumer {
    private static final String TOPIC_NAME = "test"; //토픽명
    private static final String BROKERS = "192.168.0.170:19092,192.168.0.171:29092,192.168.0.172:39092"; // 브로커 리스트

    public static void main(String[] args) {
        Properties prop = new Properties();

        // Broker list 정의
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        // Key, Value에 사용될 시리얼라이져 지정
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        prop.put(ConsumerConfig.GROUP_ID_CONFIG, "c-group-1");          // 컨슈머 그룹 아이디 지정
        prop.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");     // auto-commit 설정
        prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(prop);

        consumer.subscribe(Collections.singletonList(TOPIC_NAME));
        System.out.println("Consumer start to subscribe!!");
        System.out.println("=============================");
        System.out.println("GroupId:" + consumer.groupMetadata().groupId());
        System.out.println("=============================");

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : records) {
                    printRecord(record);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void printRecord(ConsumerRecord<String, String> record) {
        System.out.print("==> [Record]");
        StringBuilder sb = new StringBuilder();
        sb.append(" topic:").append(record.topic());
        sb.append(" partition:").append(record.partition());
        sb.append(" offset:").append(record.offset());
        sb.append(" key:").append(record.key());
        sb.append(" value:").append(record.value());

        System.out.println(sb);
    }
}
