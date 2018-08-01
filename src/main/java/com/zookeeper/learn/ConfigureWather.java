package com.zookeeper.learn;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.io.IOException;

/**
 * @Author: yhj
 * @Description:
 * @Date: Created in 2018/6/28.
 */
public class ConfigureWather implements Watcher {

    private ActiveKeyValueStore store;

    public ConfigureWather(String host) throws IOException, InterruptedException {
        store = new ActiveKeyValueStore();
        store.connect(host);
    }

    public void displayConfigure() throws KeeperException, InterruptedException {
        String value = store.read(ConfigureUpdate.PATH, this);
        System.out.printf("Read %s as %s\n", ConfigureUpdate.PATH, value);
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        if(watchedEvent.getType() == Event.EventType.NodeDataChanged){
            try {
                displayConfigure();
            } catch (KeeperException e) {
                System.err.printf("KeeperException : %s\n" , e);
            } catch (InterruptedException e) {
                System.err.printf("Interrupt : %s. Exiting.\n ", e);
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        ConfigureWather configureWather = new ConfigureWather("10.1.13.111:2181");
        configureWather.displayConfigure();
        Thread.sleep(Integer.MAX_VALUE);
    }
}
