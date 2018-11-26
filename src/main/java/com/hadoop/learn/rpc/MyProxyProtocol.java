package com.hadoop.learn.rpc;

import org.apache.hadoop.ipc.ProtocolInfo;
import org.apache.hadoop.ipc.ProtocolSignature;

import java.io.IOException;

/**
 * @Author: yhj
 * @Description:
 * @Date: Created in 2018/9/30.
 */
public class MyProxyProtocol implements IProxyProtocol {
    @Override
    public int add(int a, int b) {
        System.out.println("I am adding");
        return a+b;
    }

    @Override
    public long getProtocolVersion(String s, long l) throws IOException {
        System.out.println("MyProxy.ProtocolVersion = " + IProxyProtocol.versionID );
        return IProxyProtocol.versionID ;
    }

    @Override
    public ProtocolSignature getProtocolSignature(String s, long l, int i) throws IOException {
        return new ProtocolSignature(IProxyProtocol.versionID , null);
    }
}
