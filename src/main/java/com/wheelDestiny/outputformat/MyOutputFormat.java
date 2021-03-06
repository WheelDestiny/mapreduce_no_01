package com.wheelDestiny.outputformat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MyOutputFormat extends FileOutputFormat<LongWritable, Text> {
    @Override
    public RecordWriter<LongWritable, Text> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        MyRecordWriter myRecordWriter = new MyRecordWriter();
        //因为是自己定义的初始化方法，所以要自己调用
        myRecordWriter.initialize(job);
        return myRecordWriter;

    }
}
