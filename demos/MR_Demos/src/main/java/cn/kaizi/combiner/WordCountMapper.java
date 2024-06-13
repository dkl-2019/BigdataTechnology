package cn.kaizi.combiner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * k1，v1：行偏移量，每行的文本数据
 * k2，v2：本文数据，个数
 * 由于Java对于序列化的IO，太过臃肿，Hadoop自己重新写了一套类型
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    // 重写map方法，该方法目的就是将k1，v1转为k2，v2

    /**
     *
     * @param key：K1（行偏移量）
     * @param value：V1（每一行的文本数据）
     * @param context：表示上下文对象
     * 如何转换
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1. 将一行的文本数据拆分
        String[] split = value.toString().split(",");
        // 2. 遍历数组，组装K2，V2
        Text text = new Text();
        LongWritable longWritable =  new LongWritable(1);
        for (String word:
             split) {
            text.set(word);
            longWritable.set(1);
            // 3. 将K2和V2写入上下文中
            context.write(text, longWritable);
        }
    }


}
