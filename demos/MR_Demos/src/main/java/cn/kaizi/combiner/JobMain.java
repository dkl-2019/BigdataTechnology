package cn.kaizi.combiner;

import cn.kaizi.mapreduce.WordCountMapper;
import cn.kaizi.mapreduce.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

public class JobMain extends Configured implements Tool {

    // 该方法用来指定一个Job任务
    @Override
    public int run(String[] strings) throws Exception {
        // 1. 创建一个Job任务对象
        Job job = Job.getInstance(super.getConf(), "wordcount");
        // 打包运行出错，则需要加该配置
        job.setJarByClass(JobMain.class);

        // 2. 配置Job任务（八个步骤）

        // 第一步：指定文件的读取方式和读取路径
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("file:///F:\\Spark_Maven_Project\\Spark\\bigdata-spark\\MR_Demos\\datasets\\combiner\\wordcount.txt"));
        //TextInputFormat.addInputPath(job, new Path("file:///D:\\mapreduce\\input"));  // 本地输入

        // 第二步：指定Map阶段处理方式和数据类型
        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class); // 设置Map阶段K2类型
        job.setMapOutputValueClass(LongWritable.class); // 获取Map阶段V2类型

        // 第三、四、五、六步采用默认方式（Shuffle阶段）：
        // 自定义combiner
        job.setCombinerClass(MyCombiner.class);

        // 第七步：指定Reduce阶段处理方式和数据类型
        job.setReducerClass(WordCountReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // 第八步：设置输出类型
        job.setOutputFormatClass(TextOutputFormat.class);
        // 设置输出路径
        TextOutputFormat.setOutputPath(job, new Path("file:///F:\\Spark_Maven_Project\\Spark\\bigdata-spark\\MR_Demos\\datasets\\combiner\\combiner_out1"));

        // 等待任务结束
        boolean bl = job.waitForCompletion(true);
        return bl ? 0:1; // 三元运算符，如果正常返回0，否则返回1
    }

    public static void main(String[] args) throws Exception {
        // 启动Job任务
        Configuration configuration = new Configuration();
        int run = ToolRunner.run(configuration, new JobMain(), args);
        System.exit(run);
    }
}
