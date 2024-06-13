package cn.kaizi.flink.javademos;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class _02_BatchWordCount {

    public static void main(String[] args) throws Exception {

        // 批计算入口
        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();

        // 读数据  --  批计算中得到的数据抽象是一个数据集DataSet
        DataSource<String> dataSource = batchEnv.readTextFile("F:/Spark_Maven_Project/Spark/bigdata-spark/datasets/FlinkSourceData");

        dataSource
                .flatMap(new MyFlatMapFunction())
                .groupBy(0)
                .sum(1)
                .print();

    }

}

class MyFlatMapFunction implements FlatMapFunction<String, Tuple2> {

    @Override
    public void flatMap(String value, Collector<Tuple2> out) throws Exception {
        String[] words = value.split("\\s+");
        for (String word : words) {
            out.collect(Tuple2.of(word, 1));
        }
    }
}









