package cn.kaizi.流量统计;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        // 1：边理集合，并将集合中对应的字段累加
        Integer upFlow = 0;
        Integer downFlow = 0;
        Integer upCountFlow = 0;
        Integer downCountFlow = 0;

        for (FlowBean value:
             values) {
            upFlow += value.getUpFlow();
            downFlow += value.getDownFlow();
            upCountFlow += value.getUpCountFlow();
            downCountFlow += value.getDownCountFlow();
        }
        // 2：创建FlowBean对象，将求和后的字段赋值给FlowBean
        FlowBean flowBean = new FlowBean();
        flowBean.setUpFlow(upFlow);
        flowBean.setDownFlow(downFlow);
        flowBean.setUpCountFlow(upCountFlow);
        flowBean.setDownCountFlow(downCountFlow);
        // 3：将K3、V3写入上下文中
        context.write(key, flowBean);
    }
}
