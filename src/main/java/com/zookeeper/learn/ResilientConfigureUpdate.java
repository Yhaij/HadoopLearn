package com.zookeeper.learn;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Random;

/**
 * @Author: yhj
 * @Description:
 * @Date: Created in 2018/6/28.
 */
public class ResilientConfigureUpdate extends ConnectionWatcher{
    private Random random = new Random();
    private ResilientActiveKeyValueStore store;
    public ResilientConfigureUpdate(String host) throws IOException, InterruptedException {
        store = new ResilientActiveKeyValueStore();
        store.connect(host);
    }

    public void run(String path) throws KeeperException, InterruptedException {
        while (true){
            String value = random.nextInt(100) + "";
            store.write(path, value);
        }
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        while (true){
            try {
                ResilientConfigureUpdate resilientConfigureUpdate = new ResilientConfigureUpdate("10.1.13.111:2181");
                resilientConfigureUpdate.run(ConfigureUpdate.PATH);
                break;
            }catch (KeeperException.SessionExpiredException e){
                //不可恢复的异常，会话超时或者回话关闭，重新开启一个新会话
            }catch (KeeperException e){
                e.printStackTrace();
                break;
            }
        }
    }
}
