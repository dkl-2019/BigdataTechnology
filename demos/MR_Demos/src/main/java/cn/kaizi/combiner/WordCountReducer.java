package cn.kaizi.combiner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * <KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 *    k2      V2       K3       V3
 */
public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    // reduce方法将新的K2，V2转为K3，V3，并且写入上下文中

    /**
     *
     * @param key：新的K2
     * @param values：新的V2，是一个集合
     * @param context：上下文对象
     */
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long count = 0;
        // 1. 遍历集合，将集合中的数字相加得到V3
        for (LongWritable value:
            values) {
            count += value.get();
        }
        // 2. 将K3，V3写入上下文中
        context.write(key, new LongWritable(count));
    }
}
