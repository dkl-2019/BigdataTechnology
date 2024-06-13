package cn.kaizi.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

public class JobMain extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        // 1. 创建job对象
        Job job = Job.getInstance(super.getConf(), "mapreduce_sort");
        // 2. 配置job任务（八个步骤）
            // 第一步：设置输入类和输入路径
            job.setInputFormatClass(TextInputFormat.class);
            TextInputFormat.addInputPath(job, new Path("hdfs://master:9000/input/sort_input"));
            // 第二步：设置Mapper类和数据类型
            job.setMapperClass(SortMapper.class);
            job.setMapOutputKeyClass(SortBean.class);
            job.setMapOutputValueClass(NullWritable.class);

            // 第三、四、五、六（Shuffle）

            // 第七步：设置Reducer类和数据类型
            job.setReducerClass(SortReducer.class);
            job.setOutputKeyClass(SortBean.class);
            job.setOutputValueClass(NullWritable.class);

            // 第八步：设置输出类和输出路径
            job.setOutputFormatClass(TextOutputFormat.class);
            TextOutputFormat.setOutputPath(job, new Path("hdfs://master:9000/out/sort_out"));

        // 获取HDFS文件系统，判断输出目录是否存在，如果存在则删除
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://master:9000"), new Configuration());
        Path path = new Path("hdfs://master:9000/out/sort_out");
        boolean exists = fileSystem.exists(path);
        if (exists) {
            fileSystem.delete(path, true);  //递归删除
        }
        // 3. 等待任务结束
        boolean bl = job.waitForCompletion(true);
        return bl ?0:1;
    }

    public static void main(String[] args) throws Exception {
        // 启动job任务
        Configuration configuration = new Configuration();
        int run = ToolRunner.run(configuration, new JobMain(), args);
        System.exit(run);
    }
}



