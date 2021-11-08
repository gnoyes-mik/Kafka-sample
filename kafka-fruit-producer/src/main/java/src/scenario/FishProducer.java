package src.scenario;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;

public class FishProducer {
    private static final String TOPIC_NAME = "order-fish"; //토픽명
    private static final String BROKERS = "192.168.0.170:19092,192.168.0.171:29092,192.168.0.172:39092"; // 브로커 리스트

    public static void main(String[] args) throws InterruptedException {
        Properties prop = new Properties();

        // Broker list 정의
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        // Key, Value에 사용될 시리얼라이져 지정
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // producer 생성
        KafkaProducer<String, String> producer = new KafkaProducer<>(prop);
        RandomFish randomFish = new RandomFish();
        // message 전달
        for (int i = 0; i < 30; i++) {
            String msg = randomFish.getRandomFish();

            syncSendMessage(producer, msg);

            Thread.sleep(1000); // 1초
        }
    }

    private static void syncSendMessage(KafkaProducer<String, String> producer, String msg) {
        try {
            Future<RecordMetadata> send = producer.send(new ProducerRecord<>(TOPIC_NAME, msg));

            RecordMetadata metadata = send.get();

            StringBuilder sb = new StringBuilder();
            System.out.print("==> [Sync Send]");
            sb.append(" metadata.topic:").append(metadata.topic());
            sb.append(" metadata.partition:").append(metadata.partition());
            sb.append(" metadata.offset:").append(metadata.offset());
            sb.append(" metadata.timestamp:").append(metadata.timestamp());
            System.out.println(sb);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
