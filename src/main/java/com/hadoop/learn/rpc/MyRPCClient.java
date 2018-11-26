package com.hadoop.learn.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.net.InetSocketAddress;

/**
 * @Author: yhj
 * @Description:
 * @Date: Created in 2018/9/30.
 */
public class MyRPCClient {
    public final static int PORT = 8888;
    public final static String ADDRESS = "localhost";
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        IProxyProtocol proxy;
        proxy = RPC.getProxy(IProxyProtocol.class,
                    111 ,
                    new InetSocketAddress(ADDRESS, PORT), conf);
        int result = proxy.add(2, 3);
        System.out.println(result);
        RPC.stopProxy(proxy);
    }
}
