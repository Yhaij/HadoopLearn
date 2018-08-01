package com.hadoop.learn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Author: yhj
 * @Description:
 * @Date: Created in 2018/7/13.
 */
public class IntPairWritable implements WritableComparable<IntPairWritable> {

    private int year;
    private int tempeture;

    public IntPairWritable(){
        year = 0;
        tempeture = 0;
    }

    public IntPairWritable(IntWritable year, IntWritable tempeture){
        this.year  = year.get();
        this.tempeture = tempeture.get();
    }

    public IntPairWritable(int year, int tempeture){
        this.year = year;
        this.tempeture = tempeture;
    }

    public int getYear() {
        return year;
    }


    public int getTempeture() {
        return tempeture;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public void setTempeture(int tempeture) {
        this.tempeture = tempeture;
    }


    @Override
    public boolean equals(Object obj) {
        if(!(obj instanceof IntPairWritable)){
            return false;
        }
        IntPairWritable other = (IntPairWritable)obj;
        return other.year == year && other.tempeture == tempeture;
    }

    @Override
    public int compareTo(IntPairWritable o) {
        int cmp = year - o.getYear();
        if(cmp != 0){
            return cmp;
        }
        return tempeture - o.getTempeture();
    }

    /**
     * 序列化到输出流中
     * @param dataOutput
     * @throws IOException
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(year);
        dataOutput.writeInt(tempeture);
    }

    /**
     * 从输入流中反序列化读入
     * @param dataInput
     * @throws IOException
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        year = dataInput.readInt();
        tempeture = dataInput.readInt();
    }

    @Override
    public int hashCode() {
        return new Integer(year).hashCode()*163 + new Integer(tempeture).hashCode();
    }

    @Override
    public String toString() {
        return year + "\t" + tempeture;
    }

    /**
     * 自定义Writable类型的排序比较器
     */
    public static class Comparator extends WritableComparator{
        public Comparator(){
            super(IntPairWritable.class, true);
        }
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            int tmp =  ((IntPairWritable)a).getYear() - ((IntPairWritable)b).getYear();
            if(tmp != 0){
                return tmp;
            }
            return - (((IntPairWritable)a).getTempeture() - ((IntPairWritable)b).getTempeture());
        }
    }
}
