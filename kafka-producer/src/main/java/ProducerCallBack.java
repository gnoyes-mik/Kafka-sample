import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Objects;

public class ProducerCallBack implements Callback {
    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if(Objects.isNull(metadata)){
            exception.printStackTrace();
            return;
        }
        StringBuilder sb = new StringBuilder();
        System.out.print("==> [Async Send]");
        sb.append(" metadata.topic:").append(metadata.topic());
        sb.append(" metadata.partition:").append(metadata.partition());
        sb.append(" metadata.offset:").append(metadata.offset());
        sb.append(" metadata.timestamp:").append(metadata.timestamp());

        System.out.println(sb);
    }
}


