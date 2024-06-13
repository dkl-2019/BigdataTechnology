package cn.kaizi.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class zkClient {

    // 注意：逗号左右不能有空格
    private String connectString = "master:2181,slave1:2181,slave2:2181";
    private int sessionTimeout = 2000;
    private ZooKeeper zkClient;

    @Before
    public void init() throws IOException {
        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            public void process(WatchedEvent watchedEvent) {

            }
        });
    }

    // 创建一个子节点
    @Test
    public void create() throws KeeperException, InterruptedException {
        String nodeCreated = zkClient.create("/atguigu",
                "ss.avi".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
    }

    // 创建子节点
    @Test
    public void getChildren() throws KeeperException, InterruptedException {
        List<String> children = zkClient.getChildren("/", true);
        for (String child : children) {
            System.out.println(child);
        }
        // 延时
        // Thread.sleep(Long.MAX_VALUE);
    }


    // 判断节点是否存在
    @Test
    public void exist() throws KeeperException, InterruptedException {
        Stat stat = zkClient.exists("/atguigu", false);
        System.out.println(stat==null? "not exist" : "exist");
    }

}
