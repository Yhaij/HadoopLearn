package com.zookeeper.learn;

import org.apache.hadoop.util.ThreadUtil;
import org.apache.zookeeper.*;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @Author: yhj
 * @Description:
 * @Date: Created in 2018/6/29.
 */
public class Node implements Watcher {
    private String host;
    private String znode;
    private String value;
    private ZooKeeper zk;
    private static final Charset CHARSET = Charset.forName("UTF-8");
    private static final int SESSION_TIMEOUT = 5000;
    private CountDownLatch connectedSigner = new CountDownLatch(1);
    public Node(String host, String znode, String value) throws IOException, InterruptedException {
        this.host = host;
        this.znode = znode;
        this.value = value;
        connect();
    }
    public void connect() throws IOException, InterruptedException {
        zk = new ZooKeeper(host, SESSION_TIMEOUT,this);
        connectedSigner.await();
    }

    public void close() throws InterruptedException {
        zk.close();
    }

    public void regist() throws KeeperException, InterruptedException {
        try {
            zk.create(znode, value.getBytes(CHARSET), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            System.out.println("master 节点 ：" +  value);
        }catch (KeeperException.NodeExistsException e){
            byte[] bytes = zk.getData(znode, this, null);
            System.out.println(value +  " message : 当前 master 节点 ： " + new String(bytes, CHARSET));
        }
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        if(watchedEvent.getState() == Event.KeeperState.SyncConnected){
            connectedSigner.countDown();
        }
        if(watchedEvent.getType() == Event.EventType.NodeDeleted){
            try {
                System.out.println("master 节点挂了， 重现选举新master");
                regist();
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        Node node1 = new Node("10.1.13.111:2181", "/yhj/cow", "node1");
        Node node2 = new Node("10.1.13.111:2181", "/yhj/cow", "node2");
        Node node3 = new Node("10.1.13.111:2181", "/yhj/cow", "node3");
        node1.connect();node2.connect();node3.connect();
        node1.regist();node2.regist();node3.regist();
        TimeUnit.SECONDS.sleep(5);
        node1.close();
}
}
