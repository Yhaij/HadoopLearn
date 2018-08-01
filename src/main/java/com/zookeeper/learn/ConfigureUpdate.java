package com.zookeeper.learn;

import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.Random;

/**
 * @Author: yhj
 * @Description:
 * @Date: Created in 2018/6/28.
 */
public class ConfigureUpdate {
    public static final String PATH = "/yhj/duck";
    private ActiveKeyValueStore store;
    private Random random = new Random();
    public ConfigureUpdate(String host) throws IOException, InterruptedException {
        store = new ActiveKeyValueStore();
        store.connect(host);
    }

    public void run(String path) throws KeeperException, InterruptedException {
        while (true){
            String value = random.nextInt(100) + "";
            store.write(path, value);
            Thread.sleep(5000);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        ConfigureUpdate configureUpdate = new ConfigureUpdate("10.1.13.111:2181");
        configureUpdate.run(ConfigureUpdate.PATH);
    }
}
