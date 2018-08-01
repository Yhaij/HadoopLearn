package com.hadoop.learn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;
import java.util.Map;

/**
 * Created by hp on 2018/5/16.
 */
public class ConfigurationPrinter {
/*    static {
        Configuration.addDefaultResource("core-site.xml");
        Configuration.addDefaultResource("hdfs-site.xml");
        Configuration.addDefaultResource("mapred-site.xml");
        Configuration.addDefaultResource("yarn-site.xml");
    }*/
//    @Override
//    public int run(String[] strings) throws Exception {
//        Configuration configuration = new Configuration();
//        for(Map.Entry<String, String> entry:configuration){
//            System.out.printf("%s=%s", entry.getKey(), entry.getValue());
//        }
//        return 0;
//    }
    public static void main(String[] args)throws Exception {
        String uri = "hdfs://10.1.13.111:8020/";
        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(URI.create(uri), conf);

        //列出hdfs上 /input 目录下的所有文件
        FileStatus[] statuses = fs.listStatus(new Path("/input"));
        for (FileStatus status : statuses) {
            System.out.println(status);
        }
    }
}
