package src.scenario;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import src.ProducerCallBack;

import java.util.Properties;

public class FruitProducer {
    private static final String TOPIC_NAME = "order-fruits"; //토픽명
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
        RandomFruits randomFruits = new RandomFruits();
        // message 전달
        for (int i = 0; i < 30; i++) {
            String msg = randomFruits.getRandomFruit();

            asyncSendMessage(producer, msg);

            Thread.sleep(1000); // 1초
        }
    }

    private static void asyncSendMessage(KafkaProducer<String, String> producer, String msg) {
        producer.send(new ProducerRecord<String, String>(TOPIC_NAME, msg), new ProducerCallBack());
    }
}
