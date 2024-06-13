package kaizi.hbase.admin.api_test;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class TableExistsTest {

    public static boolean istableExist(String tablename) throws IOException {
        HBaseConfiguration configuration = new HBaseConfiguration();
        configuration.set("hbase.zookeeper.quorum", "master,slave1,slave2");

        //HBaseAdmin admin = new HBaseAdmin(configuration);
        Connection connection = ConnectionFactory.createConnection();
        // 3. 创建 Admin对象
        Admin admin = connection.getAdmin();

        boolean exists = admin.tableExists(TableName.valueOf(tablename));

        admin.close();
        return exists;
    }

    public static void main(String[] args) throws IOException {
        System.out.println(istableExist("stu5"));
    }

}
