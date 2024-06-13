package kaizi.momo_chat.service.impl;

import kaizi.momo_chat.entity.Msg;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.logging.SimpleFormatter;

/**
 * 使用 HBase 的原生API来实现消息查询
 */
public class HbaseNativeChatMessageService implements ChatMessageService{

    private Connection connection;
    private SimpleFormatter simpleFormatter;

    public HbaseNativeChatMessageService() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        connection = ConnectionFactory.createConnection(configuration);
    }

    @Override
    public List<Msg> getMessage(String date, String sender, String receiver) throws Exception {
        // 1.构建scan对象
        Scan scan = new Scan();
        // 2.构建用于查询时间的范围，例如：2020-10-05 00:00:00 – 2020-10-05 23:59:59
        String startDateStr = date + " 00:00:00";
        String endDateStr = date + " 23:59:59";
        // 3.构建查询日期的两个Filter，大于等于、小于等于，此处过滤单个列使用SingleColumnValueFilter即可。
        SingleColumnValueFilter startDateFilter = new SingleColumnValueFilter(Bytes.toBytes("C1")
                , Bytes.toBytes("msg_time")
                , CompareOperator.GREATER_OR_EQUAL
                , new BinaryComparator(Bytes.toBytes(startDateStr)));        // 4.构建发件人Filter
        // 5.构建收件人Filter
        // 6.使用FilterList组合所有Filter
        // 7.设置scan对象filter
        // 8.获取HTable对象，并调用getScanner执行
        // 9.获取迭代器，迭代每一行，同时迭代每一个单元格

        return null;
    }

    @Override
    public void close() throws IOException {
        connection.close();
    }
}
