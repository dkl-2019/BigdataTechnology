package cn.kaizi.流量统计;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    /*
    将K1、V1转为K2、V2：
            K1                  V1
        行偏移量        1363157985066	13726230503	00-FD-07-A4-72-B8:CMCC	120.196.100.82	i02.c.aliimg.com	游戏娱乐	24	27	2481	24681	200
        ---------------------------------
            K2                  V2
        13726230503         FlowBean(24	27	2481	24681)
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1：拆分行文本数据，得到手机号——>K2
        String[] split = value.toString().split("\t");
        String phoneNum = split[0];
        // 2：创建FlowBean对象，并从行文本数据中拆分出流量字段，并且将4个流量字段赋值给FlowBean对象
        FlowBean flowBean = new FlowBean();
        flowBean.setUpFlow(Integer.parseInt(split[6]));
        flowBean.setDownFlow(Integer.parseInt(split[7]));
        flowBean.setUpCountFlow(Integer.parseInt(split[8]));
        flowBean.setDownCountFlow(Integer.parseInt(split[9]));

        // 3：将K2、V2写入上下文
        context.write(new Text(phoneNum), flowBean);
    }
}
