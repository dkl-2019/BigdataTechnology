package kaizi.guigu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceExistException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * Connection：通过 ConnectionFactory获取，是重量级实现
 * Table：主要负责 DML 操作
 * Admin：主要负责 DDL 操作
 */

public class HBaseDemo {

    private static Connection connection;

    static {
        //使用 HBaseConfiguration 的单例方法实例化
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "master,slave1,slave2");
        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        createNameSpace("mydb1");
    }

    /**
     * 创建 NameSpace
     */
    public static void createNameSpace(String nameSpace) throws IOException {
        // 基本判空操作
        if (nameSpace == null || nameSpace.equals("")) {
            System.err.println("nameSpace名字不能为空");
            return;
        }
        // 获取 Admin 对象
        Admin admin = connection.getAdmin();
        NamespaceDescriptor.Builder builder = NamespaceDescriptor.create(nameSpace);
        NamespaceDescriptor namespaceDescriptor = builder.build();
        try {
            // 调用方法
            admin.createNamespace(namespaceDescriptor);
            System.out.println(nameSpace + "创建成功");
        } catch (NamespaceExistException e) {
            System.err.println(nameSpace + "已经存在了");
        }

    }

}








