package cn.kaizi.kafka.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

public class MyPartitioner implements Partitioner {
    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {

        // 获取数据
        String msgValues = o1.toString();
        int partition;
        if (msgValues.contains("atguigu")) {
            partition = 0;
        } else {
            partition = 1;
        }

        return partition;
    }

    public void close() {

    }

    public void configure(Map<String, ?> map) {

    }
}
