package com.hadoop.learn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * @Author: yhj
 * @Description: 将多个小文件包装进SequenceFile中
 * @Date: Created in 2018/7/7.
 */
public class SmallFilesToSequenceFileConverter extends Configured implements Tool {

    public static class WholeFileInputFormat extends FileInputFormat<LongWritable, Text>{

        /**
         * 不切分文件，一个split读入整个文件
         * @param context
         * @param filename
         * @return
         */
        @Override
        protected boolean isSplitable(JobContext context, Path filename) {
            return false;
        }

        @Override
        public RecordReader<LongWritable, Text> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            RecordReader reader = new WholeFileRecordReader();
            reader.initialize(inputSplit, taskAttemptContext);
            return reader;
        }
    }


    public static class WholeFileRecordReader extends RecordReader<LongWritable, Text>{

        private FileSplit fileSplit;
        private Configuration conf;
        private LongWritable key = new LongWritable();
        private Text value = new Text();
        private boolean process = false;

        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            this.fileSplit = (FileSplit)inputSplit;
            this.conf = taskAttemptContext.getConfiguration();
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if(!process){
                FileSystem fs = fileSplit.getPath().getFileSystem(conf);
                FSDataInputStream in = null;
                try {
                    in = new FSDataInputStream(fs.open(fileSplit.getPath()));
                    byte[] contextByte = new byte[(int)fileSplit.getLength()];
                    IOUtils.readFully(in, contextByte, 0, contextByte.length);
                    //等同于 in.read(contextByte, 0, contextByte.length);
                    String context = new String(contextByte, "utf-8");
                    key.set(fileSplit.getStart());
                    value.set(context);
                }finally {
                    IOUtils.closeStream(in);
                }
                process = true;
                return true;
            }
            return false;
        }

        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return value;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return process? 1.0f:1.0f;
        }

        @Override
        public void close() throws IOException {

        }
    }


    public static class SmallFilesToSequenceFileMap extends Mapper<Object, Text, Text, Text>{

        private Text outKey = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            outKey.set(((FileSplit)context.getInputSplit()).getPath().toString());
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            context.write(outKey, value);
        }
    }


    @Override
    public int run(String[] args) throws Exception {
        Job job = JobDefaultInit.getSubmintDefaultJob(this, getConf(),
                "E:\\JavaProjects\\hdpWork\\target\\hdpWork.jar", args);
        job.setJobName("SmallFiles To SequenceFile");
//        job.setNumReduceTasks(0);
        job.setMapperClass(SmallFilesToSequenceFileMap.class);
        job.setInputFormatClass(WholeFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        return job.waitForCompletion(true)? 0:1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new SmallFilesToSequenceFileConverter(), args));
    }
}
