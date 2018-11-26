package com.hadoop.learn.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;

/**
 * @Author: yhj
 * @Description:
 * @Date: Created in 2018/9/26.
 */
public class HDFSOperate {
    public static void main(String[] args) throws IOException{
        String url = "hdfs://10.1.13.111:8020/user/test/unit180203/cleanSuccess-r-00000";
        Configuration conf = new Configuration();
        Path path = new Path(url);
//        FileSystem fs = path.getFileSystem(conf);
//        getListStatus(fs, path);
//        fs.close();
        read(path, conf);
    }

    public static void getListStatus(FileSystem fs, Path path) throws IOException{
        FileStatus[] fileStatuses = fs.listStatus(path);
        for(FileStatus status: fileStatuses){
            if(!status.isDirectory()) {
                System.out.println("block size :" + status.getBlockSize() + ", name :" + status.getPath().toString()
                    + ", replication :" + status.getReplication() + ", modification time :" + status.getModificationTime());
            }else {
                getListStatus(fs, status.getPath());
            }
        }
    }

    public static void read(Path path, Configuration conf) throws IOException{
        FileSystem fs = path.getFileSystem(conf);
        InputStream in = fs.open(path);
//        IOUtils.copyBytes(in, System.out, 1024, false);
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        String str;
        while ((str = reader.readLine()) != null){
            System.out.println(str);
        }
    }
}
