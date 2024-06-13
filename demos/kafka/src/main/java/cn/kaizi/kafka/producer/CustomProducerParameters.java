package cn.kaizi.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class CustomProducerParameters {

    public static void main(String[] args) {
        // 1. 创建kafka生产者的配置对象
        Properties properties = new Properties();
        // 2. 给Kafka配置对象加配置信息
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "master:9092");
        // 3. key、value序列化
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 提高生产者吞吐量相关参数
        // batch.size：批次大小，默认 16k
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        // linger.ms：等待时间，默认 0
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        // RecordAccumulator：缓冲区大小，默认32M，buffer.memory
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        // compression.type：压缩，默认none，可配置值 gzip、snappy、lz4和zstd
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        // 4. 创建kafka生产者对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

        // 5. 调用send方法，发送消息
        for (int i = 0; i < 5; i++) {
            kafkaProducer.send(new ProducerRecord<String, String>("first", "hello world" + i));
        }

        // 6. 关闭资源
        kafkaProducer.close();
    }

}
