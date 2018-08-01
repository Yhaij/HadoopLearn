package com.hadoop.learn;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;

/**
 * @Author: yhj
 * @Description:
 * @Date: Created in 2018/7/4.
 */
public class MyCombineFileRecordReader extends RecordReader<LongWritable, Text> {

    private CombineFileSplit combineFileSplit;
    private int currentIndex;
    private LineRecordReader reader = new LineRecordReader();
    private int totalNum;

    public MyCombineFileRecordReader(CombineFileSplit combineFileSplit, TaskAttemptContext context, Integer index){
        super();
        this.combineFileSplit = combineFileSplit;
        this.currentIndex = index;
        this.totalNum = combineFileSplit.getNumPaths();
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit fileSplit = new FileSplit(combineFileSplit.getPath(currentIndex), combineFileSplit.getOffset(currentIndex),
                combineFileSplit.getLength(currentIndex), combineFileSplit.getLocations());
        context.getConfiguration().set("mapreduce.map.input.file.name", fileSplit.getPath().getName());
        this.reader.initialize(fileSplit, context);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(currentIndex >= 0 && currentIndex < totalNum){
            return reader.nextKeyValue();
        }else {
            return false;
        }
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return reader.getCurrentKey();
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return reader.getCurrentValue();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if(currentIndex >= 0 && currentIndex < totalNum){
            return (float)currentIndex/totalNum;
        }
        return 0;
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
