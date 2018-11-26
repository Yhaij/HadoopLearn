package com.hbase.learn;

import org.apache.hadoop.hbase.client.*;
import org.apache.log4j.Logger;

/**
 * @Author: yhj
 * @Description:
 * @Date: Created in 2018/7/31.
 */
public class ExampleClient {
    private static Logger logger = Logger.getLogger(ExampleClient.class);

    public static void main(String[] args) throws Exception{
        Connection connection = HBaseTable.connection(null);
//        HbaseTableOld.createTable(connection, "test_table", "data");
        HBaseTable table = new HBaseTable(connection, "sean:test");
        table.put("row1", "data", "name", "小李");
        table.put("row1", "data", "sex", "male");
        table.put("row2", "data", "name", "小徐");
        table.put("row2", "data", "address", "hdu");
        table.put("row3", "data", "interest", "吃鸡");
        table.scan();
        table.close();
        HBaseTable.closeConnection(connection);
    }
}
