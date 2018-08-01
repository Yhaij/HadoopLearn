package com.hbase.learn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @Author: yhj
 * @Description: Hbase的基本操作，增删改查
 * @Date: Created in 2018/8/1.
 */
public class HbaseTable {
    private static Logger logger = Logger.getLogger(ExampleClient.class);
    private Connection connection = null;
    private Table table = null;
    private TableName tableName = null;
    private boolean isConnecting = false;           //判断table是否连接
    public HbaseTable(Connection connection, String tableName){
        this.connection = connection;
        this.tableName = TableName.valueOf(tableName);
        connectTable();
    }

    public String getTableName() {
        return tableName.getNameAsString();
    }

    public boolean isConnecting() {
        return isConnecting;
    }

    /**
     * 得到hbase表对象
     * @return 是否连接成功
     */
    public boolean connectTable(){
        Admin admin = null;
        try {
            admin = connection.getAdmin();
            if(!isConnecting && admin.tableExists(tableName)){
                table = connection.getTable(tableName);
                isConnecting = true;
            }
        }catch (IOException e){
            e.printStackTrace();
            isConnecting = false;
            table = null;
        }finally {
            if(admin != null){
                try {
                    admin.close();
                }catch (IOException e){
                    e.printStackTrace();
                }

            }
        }
        return isConnecting;
    }

    /**
     * 关闭表
     */
    public void closeTable(){
        if(table != null){
            try {
                table.close();
            }catch (IOException e){
                e.printStackTrace();
            }
            isConnecting = false;
        }
    }

    /**
     * @param rowKey
     * @param columnFamily 列簇
     * @param column 列名
     * @param data 存储的值
     * @return
     */
    public boolean put(String rowKey, String columnFamily, String column, String data){
        if(!isConnecting){
            logger.info("Tabele " + getTableName() + " is not connect");
            return false;
        }
        try {
            Put put = new Put(rowKey.getBytes());
            put.addColumn(columnFamily.getBytes(), column.getBytes(), data.getBytes());
            table.put(put);
            return true;
        }catch (IOException e){
            e.printStackTrace();
        }finally {
            if(table != null){
                try {
                    table.close();
                }catch (IOException e){
                    e.printStackTrace();
                }
            }
        }
        return false;
    }

    public List<Cell> get(String rowKey){
        return get(rowKey, null, null);
    }

    public List<Cell> get(String rowKey, String columnFamily){
        return get(rowKey, null, null);
    }

    /**
     * 获得表下指定的rowkey下列簇的值
     * @param rowKey
     * @param columnFamily
     * @param column
     * @return
     */
    public List<Cell> get(String rowKey, String columnFamily, String column){
        if(!isConnecting){
            logger.info("Tabele " + getTableName() + " is not connect");
            return null;
        }
        try {
            Get get = new Get(Bytes.toBytes(rowKey));
            if(columnFamily != null){
                if(column != null){
                    get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
                }else {
                    get.addFamily(Bytes.toBytes(columnFamily));
                }
            }
            Result result = table.get(get);
            while (result.advance()){
                Cell cell = result.current();
                System.out.println(String.format("%s\t%s:%s\t%s",
                        Bytes.toString(CellUtil.cloneRow(cell)), Bytes.toString(CellUtil.cloneFamily(cell)),
                        Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell))));
            }
            return result.listCells();
        }catch (IOException e){
            e.printStackTrace();
        }
        return null;
    }

    public void scan(){
        scan(null, null, null, null, null);
    }

    public void scan(String startRow, String endRow){
        scan(startRow, endRow, null, null, null);
    }

    public void scan(String startRow, String endRow, String columnFamily){
        scan(startRow, endRow, columnFamily, null, null);
    }

    public void scan(String startRow, String endRow, String columnFamily, String column){
        scan(startRow, endRow, columnFamily, null, null);
    }

    /**
     * scan Hbase表
     * 寻找指定列和列簇的数据，rowKey从startRow开始到endRow（不包括endRow）
     * @param startRow 起始row
     * @param endRow   截止row
     * @param columnFamily 列簇
     * @param column
     * @param filter 过滤器
     */
    public void scan(String startRow, String endRow, String columnFamily, String column, Filter filter){
        if(!isConnecting){
            logger.info("Tabele " + getTableName() + " is not connect");
            return;
        }
        ResultScanner rs = null;
        Scan scan;
        try {
            if(startRow != null){
                if(endRow != null){
                    scan = new Scan(Bytes.toBytes(startRow), Bytes.toBytes(endRow));
                }else {
                    scan = new Scan(Bytes.toBytes(startRow));
                }
            }else {
                scan = new Scan();
            }
            if(columnFamily != null){
                if(column != null){
                    scan.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
                }else {
                    scan.addFamily(Bytes.toBytes(columnFamily));
                }
            }
            if(filter != null){
                scan.setFilter(filter);
            }
            rs = table.getScanner(scan);
            for(Result result:rs){
                for(Cell cell: result.listCells()){
                    System.out.println(String.format("%s\t%s:%s\t%s",
                            Bytes.toString(CellUtil.cloneRow(cell)), Bytes.toString(CellUtil.cloneFamily(cell)),
                            Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell))));
                }
            }
        }catch (IOException e){
            e.printStackTrace();
        }finally {
            if(rs != null){
                rs.close();
            }
        }
    }

    public boolean deleteRow(String... rowKeys){
        return deleteRow(Arrays.asList(rowKeys));
    }

    /**
     * 删除指定的rowkey
     * @param rowKeys
     * @return
     */
    public boolean deleteRow(List<String> rowKeys){
        if(!isConnecting){
            logger.info("Tabele " + getTableName() + " is not connect");
            return false;
        }
        try {
            List<Delete> deletes = new ArrayList<>();
            for(String rowKey:rowKeys){
                deletes.add(new Delete(Bytes.toBytes(rowKey)));
            }
            table.delete(deletes);
            return true;
        }catch (IOException e){
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 创建hbase表
     * @param connection
     * @param tableNameStr 表名
     * @param columnFamilies 列簇
     * @return
     */
    public static boolean createTable(Connection connection, String tableNameStr, String... columnFamilies){
        Admin admin = null;
        try {
            admin = connection.getAdmin();
            TableName tableName = TableName.valueOf(tableNameStr);
            if(admin.tableExists(tableName)){
                logger.warn("table:"+ tableName.getNameAsString() + "exists!");
                return false;
            }else {
                HTableDescriptor htd = new HTableDescriptor(tableName);
                for(String columnFamily: columnFamilies){
                    htd.addFamily(new HColumnDescriptor(columnFamily));
                }
                admin.createTable(htd);
                logger.info("table:"+ tableName.getNameAsString() + "create successful!");
                return true;
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if(admin != null){
                try {
                    admin.close();
                }catch (IOException e){
                    e.printStackTrace();
                }

            }
        }
        return false;
    }

    /**
     * 删除表
     * @param connection
     * @param tableNameStr
     * @return
     */
    public static boolean dropTable(Connection connection, String tableNameStr){
        Admin admin = null;
        try {
            admin = connection.getAdmin();
            TableName tableName = TableName.valueOf(tableNameStr);
            //删除表之前必须disable表
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            return true;
        }catch (IOException e){
            e.printStackTrace();
        }finally {
            if(admin != null){
                try {
                    admin.close();
                }catch (IOException e){
                    e.printStackTrace();
                }
            }
        }
        return false;
    }

    public static Connection connection()throws IOException{
        return connection(null);
    }

    public static Connection connection(Configuration conf) throws IOException{
        return connection(conf, null);
    }

    /**
     * 创建Hbase的连接，读取配置文件
     * @param conf
     * @param confResource
     * @return
     * @throws IOException
     */
    public static Connection connection(Configuration conf, String confResource) throws IOException{
        if(conf == null){
            conf = HBaseConfiguration.create();
        }
        if(confResource != null){
            conf.addResource(confResource);
        }
        Connection connection = null;
        connection = ConnectionFactory.createConnection(conf);
        return connection;
    }

    /**
     * 关闭Hbase的连接
     * @param connection
     */
    public static void closeConnection(Connection connection){
        if(connection != null){
            try {
                connection.close();
            }catch (IOException e){
                e.printStackTrace();
            }
        }
    }
}
