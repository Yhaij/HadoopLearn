package com.hadoop.learn.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

/**
 * @Author: yhj
 * @Description:
 * @Date: Created in 2018/9/30.
 */
public class MyRPCServer {
    public final static int PORT = 8888;
    public final static String ADDRESS = "localhost";
    public static void main(String[] args) throws IOException{
        RPC.Server server = new RPC.Builder(new Configuration())
                .setProtocol(IProxyProtocol.class)
                .setInstance(new MyProxyProtocol())
                .setBindAddress(ADDRESS)
                .setPort(PORT)
                .setNumHandlers(10)
                .build();
        server.start();
    }
}
