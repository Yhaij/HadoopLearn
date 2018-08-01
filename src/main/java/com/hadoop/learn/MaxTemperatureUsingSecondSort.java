package com.hadoop.learn;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @Author: yhj
 * @Description:
 * @Date: Created in 2018/7/17.
 */
public class MaxTemperatureUsingSecondSort extends Configured implements Tool {

    public static class MaxTemperatureUsingSecondSortMap extends Mapper<LongWritable, Text, IntPairWritable, NullWritable>{

        IntPairWritable outKey = new IntPairWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] infos = value.toString().split("\t");
            outKey.setYear(Integer.parseInt(infos[0]));
            outKey.setTempeture(Integer.parseInt(infos[1]));
            context.write(outKey, NullWritable.get());
        }
    }

    /**
     * reduce的key存储着该年份下的最高气味
     */
    public static class MaxTemperatureUsingSecondSortReduce extends Reducer<IntPairWritable, NullWritable, IntPairWritable, NullWritable>{
        @Override
        protected void reduce(IntPairWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

    /**
     * 只按照年份来分区
     */
    public static class YearPartition extends Partitioner<IntPairWritable, NullWritable>{
        @Override
        public int getPartition(IntPairWritable intPairWritable, NullWritable nullWritable, int i) {
            return Math.abs(intPairWritable.getYear()) % i;
        }
    }

    /**
     * 分组只按照年份来比较
     */
    public static class GroupComparator extends WritableComparator{
        public GroupComparator(){
            super(IntPairWritable.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return ((IntPairWritable)a).getYear() - ((IntPairWritable)b).getYear();
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = JobDefaultInit.getSubmintDefaultJob(this, getConf(),
                "E:\\JavaProjects\\hdpWork\\target\\hdpWork.jar", args);
        job.setJobName("Max Temperature Using Second Sort");
        job.setMapperClass(MaxTemperatureUsingSecondSortMap.class);
        job.setReducerClass(MaxTemperatureUsingSecondSortReduce.class);
        job.setPartitionerClass(YearPartition.class);
        job.setSortComparatorClass(IntPairWritable.Comparator.class);
        job.setGroupingComparatorClass(GroupComparator.class);
        job.setOutputKeyClass(IntPairWritable.class);
        job.setOutputValueClass(NullWritable.class);
        return job.waitForCompletion(true) ? 1:0;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new MaxTemperatureUsingSecondSort(), args));
    }
}
