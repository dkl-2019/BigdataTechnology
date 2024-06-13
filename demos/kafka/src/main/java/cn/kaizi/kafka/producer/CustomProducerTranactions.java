package cn.kaizi.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class CustomProducerTranactions {

    public static void main(String[] args) {
        // 1. 创建初始kafka生产者的配置对象
        Properties properties = new Properties();
        // 2. 给kafka配置对象添加配置信息
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "master:9092");
        // key、value序列化
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 设置事务id，名字任意起
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transaction_id_0");

        // 3. 创建kafka生产者对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

        // 初始化事务
        kafkaProducer.initTransactions();
        // 开启事务
        kafkaProducer.beginTransaction();
        try {
            // 4. 调用send方法，发送消息
            for (int i = 0; i < 5; i++) {
                // 发送消息
                kafkaProducer.send(new ProducerRecord<String, String>("first", "nihao kafka" + i));
            }
            // 提交事务
            kafkaProducer.commitTransaction();
        } catch (Exception e) {
            // 终止事务
            kafkaProducer.abortTransaction();
        } finally {
            // 5. 关闭资源
            kafkaProducer.close();
        }
    }

}
