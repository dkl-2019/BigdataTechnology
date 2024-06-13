package cn.kaizi.flink.javademos;


import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.LongValue;
import org.apache.flink.util.LongValueSequenceIterator;


import java.util.Arrays;
import java.util.List;

public class _05_SourceOperator_Demos {

    public static void main(String[] args) throws Exception {

        // 创建编程入口
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 开启本地web环境
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);// 显示申明为本地运行环境，且带webUI


        /*
            从集合获取数据流
         */
        DataStreamSource<Integer> fromElements = env.fromElements(1, 2, 3, 4, 5);
        fromElements.map(d->d*10).print(); //lambda表达式写法

        // 创建一个List
        List<String> wordList = Arrays.asList("flink", "spark", "hadoop", "flink");
        // 将List并行化成DataStream
        DataStreamSource<String> fromCollection = env.fromCollection(wordList); //源码写死，单并行度
        fromCollection.map(String::toUpperCase).print();

        // fromParallelCollection 可以实现多并行度
        DataStreamSource<LongValue> parallelCollection = env.fromParallelCollection(new LongValueSequenceIterator(1, 100), TypeInformation.of(LongValue.class));


        DataStreamSource<Long> numbers = env.generateSequence(1L, 100L).setParallelism(3);
        numbers.map(x->x-1).print();

        /**
         * 从文件获取数据流
         */
        DataStreamSource<String> fileSource = env.readTextFile("F:\\Spark_Maven_Project\\Spark\\bigdata-spark\\datasets\\wordcount.txt", "utf-8");
        fileSource.map(String::toUpperCase).print();

        //env.readFile(new TextInputFormat(null), "F:\\Spark_Maven_Project\\Spark\\bigdata-spark\\datasets\\wordcount.txt", File)


        env.execute();
    }

}
