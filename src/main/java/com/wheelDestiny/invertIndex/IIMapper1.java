package com.wheelDestiny.invertIndex;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class IIMapper1 extends Mapper<LongWritable, Text,Text, IntWritable> {
    private Text k = new Text();
    private IntWritable v = new IntWritable(1);
    private String fileName;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        fileName = fileSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split("/t");
        for (String w: words) {
            k.set(w+"--"+fileName);
            context.write(k,v);
        }
    }
}
