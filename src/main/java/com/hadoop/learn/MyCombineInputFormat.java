package com.hadoop.learn;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import java.io.IOException;

/**
 * @Author: yhj
 * @Description:
 * @Date: Created in 2018/7/4.
 */
public class MyCombineInputFormat extends CombineFileInputFormat<LongWritable, Text>{
    @Override
    public RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException {
        RecordReader<LongWritable, Text> reader = new CombineFileRecordReader<>((CombineFileSplit) inputSplit, taskAttemptContext, MyCombineFileRecordReader.class);
        try {
            reader.initialize(inputSplit, taskAttemptContext);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return reader;
    }
}
