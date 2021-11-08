package src;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;

import java.util.Properties;

public class Producer {
    private static final String TOPIC_NAME = "test"; //토픽명
    private static final String BROKERS = "192.168.0.170:19092,192.168.0.171:29092,192.168.0.172:39092"; // 브로커 리스트

    public static void main(String[] args) throws InterruptedException {
        Properties prop = new Properties();

        // Broker list 정의
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        // Key, Value에 사용될 시리얼라이져 지정
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // prop.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        // prop.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "seyong-producer");
        // prop.put(ProducerConfig.ACKS_CONFIG, "0");

        // producer 생성
        KafkaProducer<String, String> producer = new KafkaProducer<>(prop);
        // message 전달
        for (int i = 1; i < 30; i++) {
            String msg = "send " + i + " msg!";

            // noCheckResponseSendMessage(producer, msg);
            // syncSendMessage(producer, msg);
            asyncSendMessage(producer, msg);
            // eosSend(producer);

            Thread.sleep(1000); // 1초
        }
    }

    private static void eosSend(KafkaProducer<String, String> producer) {
        // Trx 생성 (Trx.id 생성)
        producer.initTransactions();
        try {
            // Trx 시작
            producer.beginTransaction();
            System.out.println(1);

            producer.send(new ProducerRecord<>(TOPIC_NAME, "msg-1"));
            producer.send(new ProducerRecord<>(TOPIC_NAME, "msg-2"));
            producer.send(new ProducerRecord<>(TOPIC_NAME, "msg-3"));
            System.out.println(2);
            // Trx 커밋
            producer.commitTransaction();
            System.out.println(3);
        } catch (KafkaException ke) {
            ke.printStackTrace();
        }
    }


    private static void noCheckResponseSendMessage(KafkaProducer<String, String> producer, String msg) {
        producer.send(new ProducerRecord<String, String>(TOPIC_NAME, msg));
    }

    private static void syncSendMessage(KafkaProducer<String, String> producer, String msg) {
        try {
            RecordMetadata metadata = producer.send(new ProducerRecord<String, String>(TOPIC_NAME, msg))
                    .get();
            // get() 메소드를 이용해 카프카 응답을 기다리기 때문에 동기식 방식이다
            // 성공 시 메타데이터를 반환받고, 실패 시 예외가 발생한다( InterruptedException, ExecutionException )
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

    private static void asyncSendMessage(KafkaProducer<String, String> producer, String msg) {
        producer.send(new ProducerRecord<String, String>(TOPIC_NAME, msg), new ProducerCallBack());
    }
}
