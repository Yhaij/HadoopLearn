package com.hadoop.learn;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @Author: yhj
 * @Description: 利用采样器全排序建立分布均匀的分区实现全排序
 * @Date: Created in 2018/7/10.
 */
public class SortByTotalPartitioner extends Configured implements Tool {

    public static class MyFileIntputFormat extends FileInputFormat<LongWritable, IntWritable>{

        @Override
        public RecordReader<LongWritable, IntWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            RecordReader<LongWritable, IntWritable> reader = new MyRecordReader();
            reader.initialize(inputSplit, taskAttemptContext);
            return reader;
        }
    }

    public static class MyRecordReader extends RecordReader<LongWritable, IntWritable>{

        private LineRecordReader lineRecordReader;

        public MyRecordReader(){
            lineRecordReader = new LineRecordReader();
        }
        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            lineRecordReader.initialize(inputSplit, taskAttemptContext);
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            return lineRecordReader.nextKeyValue();
        }

        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException {
            return lineRecordReader.getCurrentKey();
        }

        @Override
        public IntWritable getCurrentValue() throws IOException, InterruptedException {
            IntWritable value = new IntWritable(Integer.parseInt(lineRecordReader.getCurrentValue().toString()));
            return value;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return lineRecordReader.getProgress();
        }

        @Override
        public void close() throws IOException {
            lineRecordReader.close();
        }
    }

    public static class MySample extends InputSampler.RandomSampler<Object, IntWritable>{

        public MySample(double freq, int numSamples) {
            super(freq, numSamples);
        }

        public MySample(double freq, int numSamples, int maxSplitsSampled){
            super(freq, numSamples, maxSplitsSampled);
        }

        @Override
        public IntWritable[] getSample(InputFormat<Object, IntWritable> inf, Job job) throws IOException, InterruptedException {
            List<InputSplit> splits = inf.getSplits(job);
            List<IntWritable> samples = new ArrayList<>();
            int splitToSample = Math.min(this.numSamples, splits.size());
            Random random = new Random();
            long seed = random.nextLong();
            random.setSeed(seed);

            //随机交换split
            for(int i = 0; i < splits.size(); i++){
                InputSplit tmp = splits.get(i);
                int index = random.nextInt(splits.size());
                splits.set(i, splits.get(index));
                splits.set(index, tmp);
            }

            //采样
            for(int  i = 0; i < splitToSample || i < this.numSamples && samples.size() < this.numSamples ; i++){
                TaskAttemptContext sampleContext = new TaskAttemptContextImpl(job.getConfiguration(), new TaskAttemptID());
                RecordReader<Object, IntWritable> reader = inf.createRecordReader(splits.get(i), sampleContext);

                while (reader.nextKeyValue()) {
                    if (random.nextDouble() < this.freq) {
                        if (samples.size() < this.numSamples) {
                            IntWritable value = new IntWritable();
                            samples.add(ReflectionUtils.copy(job.getConfiguration(), reader.getCurrentValue(), value));
                        } else {
                            int index = random.nextInt(this.numSamples);
                            if (index != this.numSamples) {
                                IntWritable value = new IntWritable();
                                samples.set(index, ReflectionUtils.copy(job.getConfiguration(), reader.getCurrentValue(), value));
                            }
                            this.freq *= (double) (this.numSamples - 1) / (double) this.numSamples;
                        }
                    }
                }
                reader.close();
            }
            IntWritable[] result = new IntWritable[samples.size()];
            samples.toArray(result);
            return result;
        }
    }

    public static class SortByTotalPartitionerMap extends Mapper<LongWritable, IntWritable, IntWritable, NullWritable>{
        @Override
        protected void map(LongWritable key, IntWritable value, Context context) throws IOException, InterruptedException {
            context.write(value, NullWritable.get());
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = JobDefaultInit.getSubmintDefaultJob(this, getConf(),
                "E:\\JavaProjects\\hdpWork\\target\\hdpWork.jar", args);
//        InputSampler.Sampler<>
        job.setMapperClass(SortByTotalPartitionerMap.class);
        job.setInputFormatClass(MyFileIntputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(NullWritable.class);
        job.setPartitionerClass(TotalOrderPartitioner.class);
        job.setNumReduceTasks(3);

        InputSampler.Sampler<Object, IntWritable> sampler = new MySample(0.1, 1000, 10);  //构造采样器
        TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), new Path("/partitionFile"));   //设置共享分区文件路径
        InputSampler.writePartitionFile(job, sampler);   //写入分区文件,其中调用了 TotalOrderPartitioner.getPartitionFile获取文件路径
        //将共享分区文件加入到分布式缓存中
        String partitionFile = TotalOrderPartitioner.getPartitionFile(job.getConfiguration());
        URI partitionUri = new URI(partitionFile);
        job.addCacheFile(partitionUri);
        return job.waitForCompletion(true) ? 0:1;
    }

    public static void main(String[] args) throws Exception{
        System.exit(ToolRunner.run(new SortByTotalPartitioner(), args));
    }
}
