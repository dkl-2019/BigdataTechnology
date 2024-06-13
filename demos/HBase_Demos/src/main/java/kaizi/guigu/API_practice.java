package kaizi.guigu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;


import java.io.IOException;

public class API_practice {

    private static Connection connection;
    private static Admin admin;

    static {
        try {
            // 1. 获取配置信息
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "master,slave1,slave2");
            // 2. 创建连接对象
            connection = ConnectionFactory.createConnection();
            // 3. 创建 Admin对象
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 1. 判断表是否存在
    public static boolean isTableExist(String tableName) throws IOException{
        return admin.tableExists(TableName.valueOf(tableName));
    }

    // 关闭资源
    public static void close() throws IOException {
        if (admin != null) {
            admin.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    public static void main(String[] args) throws IOException {
        System.out.println(isTableExist("stu5"));

        close();
    }

}
