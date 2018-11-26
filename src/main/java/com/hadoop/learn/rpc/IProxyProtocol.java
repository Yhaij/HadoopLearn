package com.hadoop.learn.rpc;

import org.apache.hadoop.ipc.ProtocolInfo;
import org.apache.hadoop.ipc.VersionedProtocol;

import java.io.IOException;

/**
 * @Author: yhj
 * @Description:
 * @Date: Created in 2018/9/30.
 */
@ProtocolInfo(protocolName = "aaa", protocolVersion = 1234L)
public interface IProxyProtocol extends VersionedProtocol{
    long versionID  = 1234L;
    int add(int a, int b);
}
