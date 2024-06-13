package cn.kaizi.flink.javademos;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import javax.security.auth.login.AppConfigurationEntry;

/**
 * 通过scoket数据源，去请求一个socket服务，得到数据流，然后统计数据流中出现的单词个数
 */
public class _01_WordCount {

    public static void main(String[] args) throws Exception {
        // 本地模式时，程序的默认并行度，为CPU的核数
        // 创建一个编程入口环境
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();  // 流批API统一

        // 开启本地web环境
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);// 显示申明为本地运行环境，且带webUI


        env.setParallelism(2); // 设置并行度，线程数


        // 通过socket算子，把socket数据加载为一个dataStream（数据流）
        DataStreamSource<String> source = env.socketTextStream("LAPTOP-EN35HFH3", 9999);

        // 通过算子对数据流进行各种转换（计算逻辑）
        SingleOutputStreamOperator<Tuple2<String, Integer>> words = source.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                // 切割单词
                String[] split = s.split("\\s+");
                for (String word : split) {
                    // 返回每一对 （单词，1）
                    collector.collect(Tuple2.of(word, 1));
                }
            }
        });


        // 分组聚合 words.keyBy
        KeyedStream<Tuple2<String, Integer>, String> keyed = words.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.f0;
            }
        });

        // 分组后计算单词个数
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultStream = keyed.sum("f1");

        // 通过sink算子，将结果输出
        resultStream.print();

        // 触发程序提交运行
        env.execute();
    }

}





