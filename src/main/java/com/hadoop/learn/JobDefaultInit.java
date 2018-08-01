package com.hadoop.learn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

/**
 * @Author: yhj
 * @Description:
 * @Date: Created in 2018/7/7.
 */
public class JobDefaultInit {

    public static Job getSubmintDefaultJob(Tool tool, Configuration conf, String mapredJarPath, String[] args) throws IOException{
        String[] paths = parseInputAndOutput(conf, args);
        conf.addResource("core-site.xml");
        conf.addResource("hdfs-site.xml");
        conf.addResource("mapred-site.xml");
        conf.addResource("yarn-site.xml");
        conf.set("mapred.jar", mapredJarPath);
        return initDefaultJob(tool, conf, paths);
    }


    public static Job getClusterDefaultJob(Tool tool, Configuration conf, String[] args) throws IOException{
        String[] paths = parseInputAndOutput(conf, args);
        return initDefaultJob(tool, conf, paths);
    }

    private static Job initDefaultJob(Tool tool, Configuration conf, String[] paths) throws IOException{
        Job job = Job.getInstance(conf);
        job.setJarByClass(tool.getClass());
        for(int i = 0;i < paths.length-1; i++){
            FileInputFormat.addInputPath(job, new Path(paths[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(paths[paths.length-1]));
        return job;
    }

    private static String[] parseInputAndOutput(Configuration conf, String[] args) throws IOException{
        String[] parse = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(parse.length < 2){
            System.out.println("Usage: Data Deduplication <in> <out>");
            System.exit(0);
        }
        return parse;
    }
}
