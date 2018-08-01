package com.hadoop.learn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @Author: yhj
 * @Description:
 * @Date: Created in 2018/7/4.
 */
public class CombineInputFromatMain extends Configured implements Tool{

    public static class CombineInputFormatMap extends Mapper<Object, Text, Text, Text>{
        private Text outKey = new Text();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            outKey.set(context.getConfiguration().get("mapreduce.map.input.file.name"));
//            outKey.set(((FileSplit)context.getInputSplit()).getPath().getName());
            context.write(outKey, value);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = JobDefaultInit.getSubmintDefaultJob(this, getConf(),
                "E:\\JavaProjects\\hdpWork\\target\\hdpWork-1.0-SNAPSHOT.jar", args);
        job.setJobName("CombineInputFormat Text");
        job.setJarByClass(CombineInputFromatMain.class);
        job.setMapperClass(CombineInputFormatMap.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(0);
//        job.setInputFormatClass(MyCombineInputFormat.class);
        return job.waitForCompletion(true) ? 0:1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new CombineInputFromatMain(), args));
    }
}
