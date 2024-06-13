package cn.kaizi.reduce_join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/*
    K1：LongWritable
    V1：Text
    K2：Text（商品id）
    V2：Text（行文本信息）
 */

public class ReduceJoinMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 1：判断数据来自哪个文件
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String fileName = fileSplit.getPath().getName();
        if (fileName.equals("product.txt")) {
            // 数据来自商品表
            // 2：将K1、V1转为K2、V2，写入上下文中
            String[] split = value.toString().split(",");
            String productId = split[0];
            context.write(new Text(productId), value);
        }else {
            // 数据来自订单表
            String[] split = value.toString().split(",");
            String productId = split[2];
            context.write(new Text(productId), value);
        }


    }
}
