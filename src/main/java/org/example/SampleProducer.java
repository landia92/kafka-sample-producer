package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class SampleProducer {
    private final static String BOOTSTRAP_SERVERS = "kafka01:9092,kafka02:9092,kafka03:9092";
    private final static String TOPIC_NAME = "first_topic";
    private final static Logger logger = LoggerFactory.getLogger(SampleProducer.class);

    public static void main(String[] args) {
        // 1. .sh 로 수행했을 때는 기본 설정값 또는 '--param' 으로 넣었던 설정값들을 programming 적으로 할당
        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 2. producer 객체 생성
        KafkaProducer<String, String> producer = new KafkaProducer<>(configs);
        // producer 앱은 항상 작업을 마치고 접속 종료 보장을 코드에 명시적으로 구현해야 함
        //     1) 명시적 close() 호출
        //     2) try - with - resource 구문에 producer 생성
        for (int i = 0; i < 10; i++) {
            // 3. 메시지 생성 및 Record 형태로 Topic 에 바인딩
            String msgValue = "this "+ i +"th msg is from java client";
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, msgValue);
            // 4. 메시지 전송 대기 => 메시지를 전송 단위 배치로 미리 구성
            producer.send(record);  // 애플리케이션이 발생시키는 데이터는 배치작업을 구성해서 고성능 통신 추구
            logger.info("[Record Ready] {}", record);
        }
        // 5. Record 배치를 Broker로 전송
        producer.flush();
        producer.close();
    }
}
