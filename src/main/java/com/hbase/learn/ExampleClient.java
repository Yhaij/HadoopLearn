package com.hbase.learn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

/**
 * @Author: yhj
 * @Description:
 * @Date: Created in 2018/7/31.
 */
public class ExampleClient {
    private static Logger logger = Logger.getLogger(ExampleClient.class);

    public static void main(String[] args) throws Exception{
        Connection connection = HbaseTable.connection(null, "hbase-site.xml");
//        HbaseTable.createTable(connection, "test_table", "data");
        HbaseTable table = new HbaseTable(connection, "test_table");
        table.put("row1", "data", "name", "小李");
//        table.put("row1", "data", "sex", "male");
        table.put("row2", "data", "name", "小徐");
//        table.put("row2", "data", "address", "tdlab");
//        table.put("row3", "data", "interest", "吃鸡");
        table.scan();
        table.closeTable();
        HbaseTable.closeConnection(connection);
    }
}
