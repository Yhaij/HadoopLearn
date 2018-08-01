package com.zookeeper.learn;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;

import java.io.IOException;

/**
 * @Author: yhj
 * @Description:
 * @Date: Created in 2018/6/27.
 */
public class JoinGroup extends  ConnectionWatcher{
    public void create(String groupName, String memberName, String data) throws KeeperException, InterruptedException {
        String path = "/" + groupName + "/" + memberName;
        String createPath = zk.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        System.out.println("create path : " + createPath);
    }

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        JoinGroup joinGroup = new JoinGroup();
        joinGroup.connect("10.1.13.111:2181");
        joinGroup.create(args[0], args[1], "cow");
        Thread.sleep(5000);
        joinGroup.close();
    }
}
