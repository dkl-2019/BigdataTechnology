package kaizi.momo_chat.tool;

/*
    陌陌随机生成器
        1. 通过 ExcelReader 工具类从 Excel 中随机读取数据，并生成一个 Msg对象
        2. 设计 rowKey，避免热点问题，尽量让每条数据均匀的分布到每个 Region 中（已经创建了6个）
        3. 将 Msg对象 put到 HBase中
        4. 生成 10w 条数据方便测试
 */

import kaizi.momo_chat.entity.Msg;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;

import javax.validation.constraints.Max;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class MoMoMsgGen {

    public static void main(String[] args) throws ParseException, IOException {
        // 度去 Excel 中的数据
        Map<String, List<String>> resultMap =
                ExcelReader.readXlsx("F:\\Spark_Maven_Project\\Spark\\bigdata-spark\\HBase_Momo_Project\\datas\\测试数据集.xlsx", "陌陌数据");



        // 生成数据到HBase中实现步骤：
        // 1. 获取Hbase连接
        Configuration config = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(config);

        // 2. 获取HBase表MOMO_CHAT:MSG
        Table table = connection.getTable(TableName.valueOf("MOMO_CHAT:MSG"));

        int i = 0;
        int MAX = 100000;

        while (i < MAX) {
            Msg msg = getOneMessage(resultMap);
            // 3. 初始化操作Hbase所需的变量（列族、列名）
            byte[] rowkey = getRowkey(msg);
            String cf = "C1";
            String colMsg_time = "msg_time";
            String colSender_nickyname = "sender_nickyname";
            String colSender_account = "sender_account";
            String colSender_sex = "sender_sex";
            String colSender_ip = "sender_ip";
            String colSender_os = "sender_os";
            String colSender_phone_type = "sender_phone_type";
            String colSender_network = "sender_network";
            String colSender_gps = "sender_gps";
            String colReceiver_nickyname = "receiver_nickyname";
            String colReceiver_ip = "receiver_ip";
            String colReceiver_account = "receiver_account";
            String colReceiver_os = "receiver_os";
            String colReceiver_phone_type = "receiver_phone_type";
            String colReceiver_network = "receiver_network";
            String colReceiver_gps = "receiver_gps";
            String colReceiver_sex = "receiver_sex";
            String colMsg_type = "msg_type";
            String colDistance = "distance";
            String colMessage = "message";

            // 4. 构建put请求
            Put put = new Put(rowkey);

            // 5. 挨个添加陌陌消息的所有列
            put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(colMsg_time), Bytes.toBytes(msg.getMsg_time()));
            put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(colSender_nickyname), Bytes.toBytes(msg.getSender_nickyname()));
            put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(colSender_account), Bytes.toBytes(msg.getSender_account()));
            put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(colSender_sex), Bytes.toBytes(msg.getSender_sex()));
            put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(colSender_ip), Bytes.toBytes(msg.getSender_ip()));
            put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(colSender_os), Bytes.toBytes(msg.getSender_os()));
            put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(colSender_phone_type), Bytes.toBytes(msg.getSender_phone_type()));
            put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(colSender_network), Bytes.toBytes(msg.getSender_network()));
            put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(colSender_gps), Bytes.toBytes(msg.getSender_gps()));
            put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(colReceiver_nickyname), Bytes.toBytes(msg.getReceiver_nickyname()));
            put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(colReceiver_ip), Bytes.toBytes(msg.getReceiver_ip()));
            put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(colReceiver_account), Bytes.toBytes(msg.getReceiver_account()));
            put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(colReceiver_os), Bytes.toBytes(msg.getReceiver_os()));
            put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(colReceiver_phone_type), Bytes.toBytes(msg.getReceiver_phone_type()));
            put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(colReceiver_network), Bytes.toBytes(msg.getReceiver_network()));
            put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(colReceiver_gps), Bytes.toBytes(msg.getReceiver_gps()));
            put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(colReceiver_sex), Bytes.toBytes(msg.getReceiver_sex()));
            put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(colMsg_type), Bytes.toBytes(msg.getMsg_type()));
            put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(colDistance), Bytes.toBytes(msg.getDistance()));
            put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(colMessage), Bytes.toBytes(msg.getMessage()));
            // 6. 发起put请求
            table.put(put);

            // 显示进度
            ++i;
            System.out.println(i + " / " + MAX);
        }
        table.close();
        connection.close();
    }

    /**
     * 使用基于Excel中读取的数据随机生成一个Msg对象
     * @param resultMap Excel读取的数据（Map结构）
     */
    public static Msg getOneMessage(Map<String, List<String>> resultMap) {
        // 1.	构建Msg实体类对象
        Msg msg = new Msg();

        // 将当前系统的时间设置为消息的时间，以年月日 时分秒的形式存储
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        // 获取系统时间
        Date now = new Date();
        // System.out.println(now);
        msg.setMsg_time(simpleDateFormat.format(now));

        // 2.	调用ExcelReader中的randomColumn随机生成一个列的数据
        // 初始化sender_nickyname字段，调用randomColumn随机取nick_name设置数据
        msg.setSender_nickyname(ExcelReader.randomColumn(resultMap, "sender_nickyname"));
        msg.setSender_account(ExcelReader.randomColumn(resultMap, "sender_account"));
        msg.setSender_sex(ExcelReader.randomColumn(resultMap, "sender_sex"));
        msg.setSender_ip(ExcelReader.randomColumn(resultMap, "sender_ip"));
        msg.setSender_os(ExcelReader.randomColumn(resultMap, "sender_os"));
        msg.setSender_phone_type(ExcelReader.randomColumn(resultMap, "sender_phone_type"));
        msg.setSender_network(ExcelReader.randomColumn(resultMap, "sender_network"));
        msg.setSender_gps(ExcelReader.randomColumn(resultMap, "sender_gps"));
        msg.setReceiver_nickyname(ExcelReader.randomColumn(resultMap, "receiver_nickyname"));
        msg.setReceiver_ip(ExcelReader.randomColumn(resultMap, "receiver_ip"));
        msg.setReceiver_account(ExcelReader.randomColumn(resultMap, "receiver_account"));
        msg.setReceiver_os(ExcelReader.randomColumn(resultMap, "receiver_os"));
        msg.setReceiver_phone_type(ExcelReader.randomColumn(resultMap, "receiver_phone_type"));
        msg.setReceiver_network(ExcelReader.randomColumn(resultMap, "receiver_network"));
        msg.setReceiver_gps(ExcelReader.randomColumn(resultMap, "receiver_gps"));
        msg.setReceiver_sex(ExcelReader.randomColumn(resultMap, "receiver_sex"));
        msg.setMsg_type(ExcelReader.randomColumn(resultMap, "msg_type"));
        msg.setDistance(ExcelReader.randomColumn(resultMap, "distance"));
        msg.setMessage(ExcelReader.randomColumn(resultMap, "message"));

        // 3.	注意时间使用系统当前时间
        return msg;
    }

    /* 根据 Msg 实体对象生成一个 rowkey
    前面我们分析得到：
        ROWKEY = MD5Hash_发件人账号_收件人账号_消息时间戳
        1.其中MD5Hash的计算方式为：发送人账号 + “_” + 收件人账号 + “_” + 消息时间戳
        2.使用MD5Hash.getMD5AsHex方法生成MD5值
        3.取MD5值的前8位，避免过长
        4.最后把发件人账号、收件人账号、消息时间戳和MD5拼接起来
     */
    public static byte[] getRowkey(Msg msg) throws ParseException {

        // 使用StringBuilder将发件人账号、收件人账号、消息时间戳使用下划线（_）拼接起来
        StringBuilder builder = new StringBuilder();
        builder.append(msg.getSender_account());
        builder.append("_");
        builder.append(msg.getReceiver_account());
        builder.append("_");
        // 获取消息时间戳
        String msgDatetime = msg.getMsg_time();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date msgDate = simpleDateFormat.parse(msgDatetime);
        long timestamp = msgDate.getTime();
        builder.append(timestamp);

        // 使用Bytes.toBytes将拼接出来的字符串转换为byte[]数组
        // 使用MD5Hash.getMD5AsHex生成MD5值，并取其前8位
        String md5AsHex = MD5Hash.getMD5AsHex(builder.toString().getBytes());
        String md5Hex8bit = md5AsHex.substring(0, 8);

        // 再将MD5值和之前拼接好的发件人账号、收件人账号、消息时间戳，再使用下划线拼接，转换为Bytes数组
        String rowkeyString = md5Hex8bit + "_" + builder.toString();
        System.out.println(rowkeyString);

        return Bytes.toBytes(rowkeyString);
    }

}


