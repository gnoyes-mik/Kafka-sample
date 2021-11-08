import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

public class Consumer {

    private static Map<String, Integer> map = new HashMap<>();

    private static final String ORDER_TOPIC_FRUITS = "order-fruits"; //토픽명
    private static final String ORDER_TOPIC_FISH = "order-fish"; //토픽명
    private static final String BROKERS = "192.168.0.170:19092,192.168.0.171:29092,192.168.0.172:39092"; // 브로커 리스트

    public static void main(String[] args) {
        Properties prop = new Properties();

        // Broker list 정의
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        // Key, Value에 사용될 시리얼라이져 지정
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        prop.put(ConsumerConfig.GROUP_ID_CONFIG, "c-group-statistic");          // 컨슈머 그룹 아이디 지정
        prop.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");     // auto-commit 설정
        prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        // prop.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(prop);

//        consumer.subscribe(Arrays.asList(ORDER_TOPIC_FRUITS, ORDER_TOPIC_FISH));
        consumer.subscribe(Pattern.compile("order-.*"));
        System.out.println("Consumer start to subscribe!!");
        System.out.println("=============================");
        System.out.println("GroupId:" + consumer.groupMetadata().groupId());
        System.out.println("=============================");


        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : records) {
                    insert(record.value());
                }
                printMap();
                Thread.sleep(1500);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void insert(String item) {
        if (!map.containsKey(item)) {
            map.put(item, 1);
            return;
        }
        map.put(item, map.get(item) + 1);
    }

    private static void printMap() {
        System.out.println(map.toString());
        /*StringBuilder sb = new StringBuilder();
        for (String k : map.keySet()) {
            Integer v = map.get(k);
        }*/
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
