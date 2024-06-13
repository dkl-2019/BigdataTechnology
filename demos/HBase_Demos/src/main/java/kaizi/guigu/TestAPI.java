package kaizi.guigu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * DDL：
 *  1. 判断表是否存在
 *  2. 创建表
 *  3. 创建命名空间
 *  4. 删除表
 *
 * DML：
 *  5. 插入数据
 *  6. 查数据（get）
 *  7. 查数据（scan）
 *  8. 删除数据
 */
public class TestAPI {

    private static Connection connection;
    private static Admin admin;

    static {
        try {
            // 1. 获取配置信息
            //使用 HBaseConfiguration 的单例方法实例化
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
    public static boolean istableExist(String tablename) throws IOException {
//        // 1. 获取配置信息
//        Configuration conf = HBaseConfiguration.create();
//        conf.set("hbase.zookeeper.quorum", "master,slave1,slave2");
//        // 2. 获取管理员对象
//        Connection connection = ConnectionFactory.createConnection(conf);
//        Admin admin = connection.getAdmin();
        // 3. 判断表是否存在
        boolean exists = admin.tableExists(TableName.valueOf(tablename));
        // 4. 关闭连接
        // admin.close();

        return exists;
    }


    // 2. 创建表
    public static void createTable(String tableName, String... cfs) throws IOException {
        // 1. 判断是否存在列簇信息
        if (cfs.length <= 0) {
            System.out.println("请设置列簇信息");
            return;
        }
        // 2. 判断表是否存在
        if (istableExist(tableName)) {
            System.out.println(tableName + "表已经存在！");
            return;
        }

        // 3. 创建表描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        // 4. 循环添加列簇信息
        for (String cf: cfs) {
            // 5. 创建列簇描述器
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
            // 6. 添加列簇信息
            hTableDescriptor.addFamily(hColumnDescriptor);
        }
        // 7.  创建表
        admin.createTable(hTableDescriptor);
    }


    // 3. 删除表
    public static void dropTable(String tableName) throws IOException {
        // 1. 判断表是否存在
        if (istableExist(tableName)) {
            System.out.println(tableName + "表不存在！");
        }

        // 2. 禁用表
        admin.disableTable(TableName.valueOf(tableName));
        // 3. 删除表
        admin.deleteTable(TableName.valueOf(tableName));

    }


    // 4. 创建命名空间
    public static void createNameSpace(String ns) {
        // 1. 创建命名空间描述器
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(ns).build();
        // 2. 创建命名空间
        try {
            admin.createNamespace(namespaceDescriptor);
        } catch (NamespaceExistException e) {
            System.out.println(ns + "命名空间已存在！");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    // 5. 向表中插入数据
    public static void putData(String tableName,
                               String rowKey,
                               String cf,
                               String cn,
                               String value) throws IOException {
        // 1. 获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));

        // 2. 获取Put对象
        Put put = new Put(Bytes.toBytes(rowKey));
        // 3. 给 put对象赋值
        put.addColumn(Bytes.toBytes(cf),
                Bytes.toBytes(cn),
                Bytes.toBytes(value));
        // 4. 插入数据
        table.put(put);
        // 5. 关闭表连接
        table.close();
    }


    // 6. 获取数据 get
    public static void getData(String tableName,
                               String rowKey,
                               String cf,
                               String cn) throws IOException {
        // 1. 获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));
        // 2. 创建 Get 对象
        Get get = new Get(Bytes.toBytes(rowKey));
        //      2.1获取指定列簇
        get.addFamily(Bytes.toBytes(cf));
        //      2.2指定列簇和列
        get.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn));
        //      2.3获取数据的版本数
        get.setMaxVersions();

        // 3. 获取数据
        Result result = table.get(get);
        // 4. 解析result并打印
        for (Cell cell : result.rawCells()) {
            // 5. 打印数据
            System.out.println("CF: " + Bytes.toString(CellUtil.cloneFamily(cell)) +
                    "，CN: " + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                    "，Value: " + Bytes.toString(CellUtil.cloneValue(cell))
            );
        }
        // 6. 关闭表连接
        table.close();
    }


    // 7. 获取数据 scan
    public static void scanTable(String tableName) throws IOException {
        // 1. 获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));
        // 2. 构建 Scan 对象
        Scan scan = new Scan(Bytes.toBytes("1001"), Bytes.toBytes("1003"));
        // Scan scan = new Scan(); // 空参表示全表扫描
        // 3. 扫描表
        ResultScanner scanner = table.getScanner(scan);
        // 4. 解析
        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                // 5. 打印数据
                System.out.println("RK: " + Bytes.toString(CellUtil.cloneRow(cell)) +
                        "CF: " + Bytes.toString(CellUtil.cloneFamily(cell)) +
                        "，CN: " + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                        "，Value: " + Bytes.toString(CellUtil.cloneValue(cell))
                );
            }
        }
        // 7. 关闭表连接
        table.close();
    }


    // 8. 删除删除数据
    public static void deleteData(String tableName,
                                  String rowKey,
                                  String cf,
                                  String cn) throws IOException {
        // 1. 获取 table 对象
        Table table = connection.getTable(TableName.valueOf(tableName));
        // 2. 构建删除对象
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        //      2.1 设置删除的列
        // delete.addColumns(Bytes.toBytes(cf), Bytes.toBytes(cn));
        //      2.2 删除指定的列簇
        delete.addFamily(Bytes.toBytes(cf));

        // 3. 执行删除操作
        table.delete(delete);

        table.close();
    }


    public static void close() throws IOException {
        if (admin != null) {
            admin.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    public static void main(String[] args) throws IOException {
        // 1. 测试表是否存在
        // System.out.println(istableExist("stu5"));

        // 2. 创建表测试
        // createTable("stu5", "info1", "info3");

        // 3. 删除表测试
        // dropTable("stu5");

        // 4. 创建命名空间测试
        // createNameSpace("DongKaizi");
        // createTable("DongKaizi:tb1", "info1", "info2");

        // 5. 插入数据测试
        // putData("DongKaizi:tb1", "1001", "info2", "name", "孙策");

        // 6. 获取单行数据测试
        // getData("DongKaizi:tb1", "1001", "info2", "name");

        // 7. scan扫描测试
        // scanTable("DongKaizi:tb1");

        // 8. 删除数据测试
        deleteData("stu", "1001", "info1", "name");

        // System.out.println(istableExist("stu5"));

        // 关闭资源
        close();
    }

}




