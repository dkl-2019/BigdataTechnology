package cn.kaizi.partition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

public class JobMain extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        // 1:创建一个job任务对象
        Job job = Job.getInstance(super.getConf(), "partition_maperduce");
        // 打包运行出错，则需要加该配置
        job.setJarByClass(JobMain.class);

        // 2:对job任务进行配置（八个步骤）
            // 第一步：设置输入类和输入路径
            job.setInputFormatClass(TextInputFormat.class);
            TextInputFormat.addInputPath(job,new Path("hdfs://master:9000/input"));
            // 第二步：设置Mapper类和数据类型（K2、V2）
            job.setMapperClass(PartitionMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(NullWritable.class);
            // 第三步：指定分区类
            job.setPartitionerClass(MyPartitioner.class);

            // 第四、五、六 使用默认
            // 第七步：指定Reducer类和数据类型（K3、V3）
            job.setReducerClass(PartitionReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);
            // 设置ReduceTask的个数
            job.setNumReduceTasks(2);

            // 第八步：指定输出类和输出路径
            job.setOutputFormatClass(TextOutputFormat.class);
            TextOutputFormat.setOutputPath(job, new Path("hdfs://master:9000/out/partition_out"));

        // 获取HDFS文件系统，判断输出目录是否存在，如果存在则删除
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://master:9000"), new Configuration());
        Path path = new Path("hdfs://master:9000/out/partition_out");
        boolean exists = fileSystem.exists(path);
        if (exists) {
            fileSystem.delete(path, true);  //递归删除
        }

        // 3:等待任务结束
        boolean bl = job.waitForCompletion(true);
        return bl?0:1;  // 三元运算符，如果正常返回0，否则返回1
    }

    public static void main(String[] args) throws Exception {
        // 启动一个job任务
        Configuration configuration = new Configuration();
        int run = ToolRunner.run(configuration, new JobMain(), args);
        System.exit(run);
    }
}











