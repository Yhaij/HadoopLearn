package com.hbase.learn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.util.*;

/**
 * @Author: yhj
 * @Description: HBase的api简单实现表的增删改查
 * @Date: Created in 2018/11/24.
 */
public class HBaseTable {
    private static Logger logger = Logger.getLogger(HBaseTable.class);
    private TableName tableName;
    private Table table;
    private Connection conn;
    private boolean isConnect = false;
    public HBaseTable(Connection conn, String tableName){
        this(conn, TableName.valueOf(tableName));
    }

    public HBaseTable(Connection conn, byte[] tableName){
        this(conn, TableName.valueOf(tableName));
    }

    public HBaseTable(Connection conn, TableName tableName){
        this.conn = conn;
        this.tableName = tableName;
        table = connectTable();
    }

    private Table connectTable(){
        Admin admin = null;
        Table table = null;
        try {
            admin = conn.getAdmin();
            if(admin.tableExists(tableName)){
                table = conn.getTable(tableName);
                isConnect = true;
            }else {
                logger.error("not exists table " + tableName.getNameAsString());
            }
            return table;
        }catch (IOException e){
            logger.error("connect table " + tableName.getNameAsString() + " fail...");
            throw new RuntimeException(e);
        }finally {
            closeAdmin(admin);
        }
    }

    public boolean isConnectionTable(){
        if(!isConnect){
            logger.warn("table " + tableName + " is not connect");
        }
        return isConnect;
    }

    /**
     * 放入数据到表中，不检查通单元下是否存在数据
     * @param row 行键
     * @param family 列簇
     * @param qualifier 列限定符
     * @param value
     * @return
     */
    public boolean put(String row, String family, String qualifier, String value){
        if(!isConnectionTable()){
            return false;
        }
        try {
            Put put = new Put(Bytes.toBytes(row));
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
            table.put(put);
        }catch (IOException e){
            e.printStackTrace();
            logger.warn("put data is fail...");
            return false;
        }
        return true;
    }

    /**
     * 放入数据到表中，不检查通单元下是否存在数据
     * @param row 行键
     * @param family 列簇
     * @param qualifier 列限定符
     * @param ts 时间戳
     * @param value
     * @return
     */
    public boolean put(String row, String family, String qualifier, long ts, String value){
        if(!isConnectionTable()){
            return false;
        }
        Put put = new Put(Bytes.toBytes(row));
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), ts, Bytes.toBytes(value));
        try {
            table.put(put);
        }catch (IOException e){
            e.printStackTrace();
            logger.warn("put data is fail...");
            return false;
        }
        return true;
    }

    public List<Cell> get(String row){
        return get(row, null, null);
    }

    public List<Cell> get(String row, String family){
        return get(row, null, null);
    }

    /**
     * 从表中取出相应的值
     * @param row
     * @param family
     * @param qualifier
     * @return
     */
    public List<Cell> get(String row, String family, String qualifier){
        if(!isConnectionTable()){
            return null;
        }
        Get get = new Get(Bytes.toBytes(row));
        get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        try {
            Result result = table.get(get);
//            List<Cell> cells = new ArrayList<>();
//            while (result.advance()){
//                Cell cell = result.current();
//                cells.add(cell);
//                System.out.println(String.format("%s\t%s:%s\t%s",
//                    Bytes.toString(CellUtil.cloneRow(cell)), Bytes.toString(CellUtil.cloneFamily(cell)),
//                    Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell))));
//            }
//            return cells;
            return result.listCells();
        }catch (IOException e){
            e.printStackTrace();
            logger.warn("get data is fail...");
            return null;
        }
    }

    public void scan(){
        scan(null, null, null, null, null);
    }

    public List<List<Cell>> scan(String startRow, String endRow){
        return scan(startRow, endRow, null, null, null);
    }

    public List<List<Cell>> scan(String startRow, String endRow, String columnFamily){
        return scan(startRow, endRow, columnFamily, null, null);
    }

    public List<List<Cell>> scan(String startRow, String endRow, String columnFamily, String column){
        return scan(startRow, endRow, columnFamily, column, null);
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
    public List<List<Cell>> scan(String startRow, String endRow, String columnFamily, String column, Filter filter){
        if(!isConnectionTable()){
            return null;
        }
        Scan scan = null;
        if(startRow != null){
            if(endRow != null){
                scan = new Scan(Bytes.toBytes(startRow), Bytes.toBytes(endRow));
            }else {
                scan = new Scan(Bytes.toBytes(startRow));
            }
        }else {
            scan = new Scan();
        }
        if(filter != null){
            scan.setFilter(filter);
        }
        if(columnFamily != null){
            if(column != null){
                scan.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
            }else {
                scan.addFamily(Bytes.toBytes(columnFamily));
            }
        }
        try {
            List<List<Cell>> result = new ArrayList<>();
            ResultScanner rs = table.getScanner(scan);
            Iterator<Result> iterator = rs.iterator();
            while (iterator.hasNext()){
                Result r = iterator.next();
                while (r.advance()){
                    Cell cell = r.current();
                    System.out.println(String.format("%s\t%s:%s\t%s",
                            Bytes.toString(CellUtil.cloneRow(cell)), Bytes.toString(CellUtil.cloneFamily(cell)),
                                    Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell))));
                }
                result.add(r.listCells());
            }
            return result;
        }catch (IOException e){
            e.printStackTrace();
            logger.warn("scan data is fail...");
            return null;
        }
    }


    public boolean deleteRow(String... rows){
        return deleteRow(Arrays.asList(rows));
    }

    /**
     * 删除指定的rowkey
     * @param rows
     * @return
     */
    public boolean deleteRow(List<String> rows){
        if(!isConnectionTable()){
            return false;
        }
        try {
            List<Delete> list = new ArrayList<>();
            for(String row:rows){
                list.add(new Delete(Bytes.toBytes(row)));
            }
            table.delete(list);
        }catch (IOException e){
            e.printStackTrace();
            logger.warn("delete data is fail...");
            return false;
        }
        return true;
    }

    public boolean close(){
        if(!isConnectionTable()){
            return false;
        }
        try {
            table.close();
        }catch (IOException e){
            e.printStackTrace();
            logger.error("close table " + tableName + " is fail...");
            return false;
        }
        return true;
    }

    /**
     * 删除HBase表
     * @param conn
     * @param tableName 表名
     * @return
     */
    public static boolean dropTable(Connection conn, String tableName){
        Admin admin = null;
        try {
            TableName tName = TableName.valueOf(tableName);
            admin = conn.getAdmin();
            //删除表之前先要disbale
            admin.disableTable(tName);
            admin.deleteTable(tName);
            return true;
        }catch (IOException e){
            e.printStackTrace();
            logger.error("drop table " + tableName + " fail...");
            return false;
        }finally {
            closeAdmin(admin);
        }
    }

    public static boolean dropTable(Configuration conf, String tableName){
        Connection conn = null;
        try {
            conn = connection(conf);
            return dropTable(conn, tableName);
        }finally {
            closeConnection(conn);
        }
    }

    /**
     * 创建hBase表
     * @param conn
     * @param tableName 表名
     * @param columnFamilies 列簇
     * @return
     */
    public static boolean createTable(Connection conn, String tableName, String... columnFamilies){
        Admin admin =null;
        try {
            admin = conn.getAdmin();
            if(admin.tableExists(TableName.valueOf(tableName))){
                logger.warn("table " + tableName + "is already exists");
                return false;
            }
            HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
            for(String columnFamilie: columnFamilies){
                htd.addFamily(new HColumnDescriptor(columnFamilie.getBytes()));
            }
            admin.createTable(htd);
            return true;
        }catch (IOException e){
            e.printStackTrace();
            logger.error("create table fail...");
            return false;
        }finally {
            closeAdmin(admin);
        }
    }

    public static boolean createTbale(Configuration conf, String tableName, String... columnFamilies){
        Connection conn = null;
        try {
            conn = connection(conf);
            return createTable(conn, tableName,columnFamilies);
        }finally {
            closeConnection(conn);
        }
    }

    /**
     * 创建Hbase的连接，读取配置文件
     * @param conf
     * @param confResources
     * @return
     * @throws IOException
     */
    public static Connection connection(Configuration conf, String... confResources){
        conf = getHBaseConfiguration(conf);
        for(String confResource: confResources){
            conf.addResource(confResource);
        }
        return connection(conf);
    }

    private static Configuration getHBaseConfiguration(Configuration conf){
        if(conf == null){
            conf = HBaseConfiguration.create();
        }else {
            conf = HBaseConfiguration.create(conf);
        }
        conf.addResource("hbase-site.xml");  //Configuration的加载资源是classLoader.getResource()
        return conf;
    }

    public static Connection connection(Configuration conf){
        conf = getHBaseConfiguration(conf);
        Connection conn = null;
        try {
            conn = ConnectionFactory.createConnection(conf);
        }catch (IOException e){
            logger.error("connection HBase fail....");
            throw new RuntimeException(e);
        }
        return conn;
    }

    public static boolean closeConnection(Connection connection){
        if(connection != null){
            try {
                connection.close();
            }catch (IOException e){
                e.printStackTrace();
                logger.error("close connection fail...");
                return false;
            }
        }
        return true;
    }

    private static boolean closeAdmin(Admin admin){
        if(admin != null){
            try {
                admin.close();
            }catch (IOException e){
                e.printStackTrace();
                logger.error("close admin fial...");
                return false;
            }
        }
        return true;
    }
}
