package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class SampleCustomPartitionerProducer {
    private final static String TOPIC_NAME = "first_topic";
    private final static String BOOTSTRAP_SERVERS = "kafka01:9092,kafka02:9092,kafka03:9092";

    public static void main(String[] args) {
        // try-with-resource 패턴 적용 : `Partitioner extends Closeable`
        try (KafkaProducer<String, String> producer = getKafkaProducer()) {
            ProducerRecord<String, String> record;
            // Time Critical 한 작업이 있는 경우 다른 작업들과 분리, 파티션 0번을 고속 처리 전용 Track 으로 사용
            for (int i = 0; i < 5; i++) {
                record = new ProducerRecord<>(TOPIC_NAME, "partitionZero" + i, "this is going to partition ZERO /" + i);
                producer.send(record);
                producer.flush();
            }
            // Steps & Finish 처리를 1번 Partition 에서 전담
            boolean fin = false;
            for (int i = 0; i < 5; i++) {
                record = new ProducerRecord<>(TOPIC_NAME, "step" + i, "this is going to partition No.1 /" + i);
                producer.send(record);
                producer.flush();
                if (i == 4) {
                    fin = true;
                }
            }
            if (fin) {
                record = new ProducerRecord<>(TOPIC_NAME, "finished_well", "this is also going to partition No.1 /fin");
                producer.send(record);
                producer.flush();
            }
            // 유저 관련 Data 에 대해, 요청이 발생한 순서에 따른 처리 보장을 위해 특정 파티션 지정
            producer.send(new ProducerRecord<>(TOPIC_NAME, "key doesn't matter", "customerA data is going to partition No.2 /A1"));
            producer.send(new ProducerRecord<>(TOPIC_NAME, "key doesn't matter", "userA data is going to partition No.2 /A2"));
            producer.send(new ProducerRecord<>(TOPIC_NAME, "key doesn't matter", "customerB is going to partition No.2 /B1"));
            producer.send(new ProducerRecord<>(TOPIC_NAME, "key doesn't matter", "userB is going to partition No.2 /B2"));
            producer.send(new ProducerRecord<>(TOPIC_NAME, "key doesn't matter", "customerC is going to partition No.2 /C1"));
            producer.send(new ProducerRecord<>(TOPIC_NAME, "key doesn't matter", "userC is going to partition No.2 /C2"));
            producer.flush();
        }
    }

    static KafkaProducer<String, String> getKafkaProducer() {
        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class.getName());
        return new KafkaProducer<>(configs);
    }
}
