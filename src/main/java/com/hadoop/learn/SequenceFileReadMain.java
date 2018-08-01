package com.hadoop.learn;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @Author: yhj
 * @Description: 读取SequenceFile中的内容
 * @Date: Created in 2018/7/7.
 */
public class SequenceFileReadMain extends Configured implements Tool{

    public static class SequenceFileReadMap extends Mapper<Text, Text, Text, Text>{
        private Text outKey = new Text();
        private Text outValue = new Text();
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            outKey.set("key : " + key.toString());
            outValue.set("value : " + value.toString());
            context.write(outKey, outValue);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = JobDefaultInit.getSubmintDefaultJob(this, getConf(),
                "E:\\JavaProjects\\hdpWork\\target\\hdpWork.jar", args);
        job.setJobName("Sequence File Read");
        job.setMapperClass(SequenceFileReadMap.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception{
        System.exit(ToolRunner.run(new SequenceFileReadMain(), args));
    }
}
