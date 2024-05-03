package org.example;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;


public class  CustomPartitioner implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        if (keyBytes == null) {
            throw new InvalidRecordException("Need message key");
        }
        // 파티션 할당을 key 해싱에 맡기지 않고, 특정 메시지 패턴을 특정 partition 에 할당
        if (((String)key).startsWith("partitionZero")) // 고속 처리용 0번 파티션
            return 0;
        if (((String)key).contains("step") || ((String)key).contains("finish")) // 연속 작업 순서 보장용 1번 파티션
            return 1;
        if (((String)value).contains("customer") || ((String)value).contains("user")) // 유저 선착순 처리 보장용 2번 파티션
            return 2;

        // 특정 조건에 빠지지 않은 경우 murmur2 해싱 및 mod 연산을 통한 균일 분배
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
    }

    @Override
    public void configure(Map<String, ?> configs) {}

    @Override
    public void close() {}
}
