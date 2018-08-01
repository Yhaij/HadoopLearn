package com.zookeeper.learn;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

import java.nio.charset.Charset;

/**
 * @Author: yhj
 * @Description:
 * @Date: Created in 2018/6/28.
 */
public class ActiveKeyValueStore extends ConnectionWatcher{
    public static final Charset CHARSET = Charset.forName("UTF-8");
    public void write(String path, String value) throws KeeperException, InterruptedException {
        Stat state = zk.exists(path,false);
        if(state == null){
            zk.create(path, value.getBytes(CHARSET), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }else {
            zk.setData(path, value.getBytes(CHARSET), state.getVersion());
        }
    }

    public String read(String path, Watcher watcher) throws KeeperException, InterruptedException {
        byte[] bytes = zk.getData(path, watcher, null);
        return new String(bytes, CHARSET);
    }
}
