package com.zookeeper.learn;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;

/**
 * @Author: yhj
 * @Description:
 * @Date: Created in 2018/6/28.
 */
public class ResilientActiveKeyValueStore extends ConnectionWatcher {
    public static final Charset CHARSET = Charset.forName("UTF-8");
    private static final int MAX_RETRIES = 5;
    private static final int RETRY_PERIOD_SECONDS = 10;
//    private static RETRY_
    public void write(String path, String value) throws KeeperException, InterruptedException {
        int retries = 0;
        while (true){
            try {
                Stat stat = zk.exists(path, null);
                if(stat == null){
                    zk.create(path, value.getBytes(CHARSET), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }else {
                    zk.setData(path, value.getBytes(CHARSET), stat.getVersion());
                }
                break;
            }catch (KeeperException.SessionExpiredException e){//不可恢复的异常，会话超时或者回话关闭
                throw e;
            }catch (KeeperException e){//可恢复异常或者状态异常
                if(retries >= MAX_RETRIES){
                    throw e;
                }
                TimeUnit.SECONDS.sleep(RETRY_PERIOD_SECONDS);
            }
        }
    }
}
