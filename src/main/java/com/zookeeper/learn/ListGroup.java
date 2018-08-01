package com.zookeeper.learn;

import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.List;

/**
 * @Author: yhj
 * @Description:
 * @Date: Created in 2018/6/27.
 */
public class ListGroup extends ConnectionWatcher {

    public void list(String groupName) throws KeeperException, InterruptedException {
        System.out.println(groupName);
        this.list(groupName, 0);
    }

    private void list(String groupName, int layout) throws KeeperException, InterruptedException {
        String path = "/" + groupName;
        String head = "";
        for(int i = 0;i < layout+1; i++){
            head += "  ";
        }
        head += "|-";
        try {
            List<String> childrens = zk.getChildren(path, false);
            if(childrens.isEmpty()){
                return;
            }
            for(String children : childrens){
                System.out.println(head + children);
                list(groupName + "/" + children, layout + 1);
            }
        }catch (KeeperException.NoNodeException e){
            System.out.println("Don't exist this group : " + groupName);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        ListGroup listGroup = new ListGroup();
        listGroup.connect("10.1.13.111:2181");
        listGroup.list("yhj");
        listGroup.close();
    }
}
