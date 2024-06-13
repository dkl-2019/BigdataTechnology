package cn.kaizi.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class CustomProducerSync {

    public static void main(String[] args) {
        // 0. 配置
        Properties properties = new Properties();
        // 连接集群
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.183:9092");
        // 指定对应的key和value的序列化类型
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 1. 创建kafka生产者对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

        // 2. 发送数据
        for (int i = 0; i < 5; i++) {
            //kafkaProducer.send(new ProducerRecord<String, String>("first", "hello dongkunling" + i));
            kafkaProducer.send(new ProducerRecord<String, String>("first", "atguigu " + i), new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        System.out.println("主题：" + recordMetadata.topic() + "分区：" + recordMetadata.partition());
                    }
                }
            });
        }

        // 3. 关闭资源
        kafkaProducer.close();
    }

}
