package com.zookeeper.learn;

import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.List;

/**
 * @Author: yhj
 * @Description:
 * @Date: Created in 2018/6/27.
 */
public class DeleteGroup extends ConnectionWatcher {
    public void delete(String groupName) throws KeeperException, InterruptedException {
        String path = "/" + groupName;
        try {
            List<String> childrens = zk.getChildren(path, false);
            for(String children : childrens){
                //zookeeper删除一个znode之前必须将该znode下的所有znode删除，所以采用递归的方式
                delete(groupName + "/" + children);
            }
            //zookeeper删除一个znode,必须提供一致的版本号，将版本号设为-1，可以绕过版本检测机制，不管znode版本号是什么都直接删除
            zk.delete(path, -1);
            System.out.println("delete " + path);
        }catch (KeeperException.NoNodeException e){
            System.out.println("Don't exist this group : " + groupName);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        DeleteGroup deleteGroup = new DeleteGroup();
        deleteGroup.connect("10.1.13.111:2181");
        deleteGroup.delete("yhj/cow");
        deleteGroup.close();
    }
}
