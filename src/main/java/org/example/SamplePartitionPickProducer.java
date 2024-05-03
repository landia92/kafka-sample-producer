package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class SamplePartitionPickProducer {
    private final static Logger logger = LoggerFactory.getLogger(SampleProducer.class);
    private final static String TOPIC_NAME = "first_topic";
    private final static String BOOTSTRAP_SERVERS = "kafka01:9092,kafka02:9092,kafka03:9092";
    public static void main(String[] args) {
        // 1) producer 생성
        KafkaProducer<String, String> producer = getKafkaProducer();
        // 2) 메시지 레코드 생성시 Partition 명시적 지정
        int partitionNo;
        for (int i = 0; i < 3; i++) {
            partitionNo = i;
            for (int j = 0; j < 5; j++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(
                        TOPIC_NAME, partitionNo, // Record 에 토픽의 특정 파티션 타겟을 지정한 경우
                        "RecordKey_" + j,        // 레코드 키를 바꿔 가며 보내도 동일 파티션 지정됨
                        "partition picked not by key j, but by partitionNo param i: " + i
                );
                producer.send(record);
                logger.info("[Record check] partition: {} key: {}", record.partition(), record.key());
            }
        }

        producer.flush();
        producer.close();
    }

    static KafkaProducer<String, String> getKafkaProducer() {
        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(configs);
    }
}