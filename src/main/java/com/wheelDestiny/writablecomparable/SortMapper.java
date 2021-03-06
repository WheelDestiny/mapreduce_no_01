package com.wheelDestiny.writablecomparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<LongWritable, Text,FlowBean,Text> {
    private FlowBean flowBean = new FlowBean();
    private Text phone = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        phone.set(fields[0]);
        flowBean.setUpFlow(Long.parseLong(fields[1]));
        flowBean.setDownFlow(Long.parseLong(fields[2]));
        flowBean.setSumFlow(Long.parseLong(fields[3]));

        context.write(flowBean,phone);
    }
}
