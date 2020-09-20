package com.wheelDestiny.MR09011;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class SortMR extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new SortMR(),args));
    }
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf,"SortMR");

        job.setJarByClass(SortMR.class);

        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setSortComparatorClass(DesComparator.class);

        FileInputFormat.setInputPaths(job,new Path("D:\\input"));

        FileSystem fs = FileSystem.get(conf);
        Path out = new Path("D:\\output");
        if(fs.exists(out)){
            //递归删除
            fs.delete(out,true);
        }

        FileOutputFormat.setOutputPath(job,out);
        boolean b = job.waitForCompletion(true);

        return b?0:1;
    }
}
class SortMapper extends Mapper<LongWritable,Text,LongWritable,Text>{
    private LongWritable k = new LongWritable();
    private Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] files = value.toString().split("\t");
        String name = files[0];
        long num = Long.parseLong(files[1]);
        k.set(num);
        v.set(num+"\t"+name);
        context.write(k,v);
    }
}
class SortReducer extends Reducer<LongWritable, Text, NullWritable,Text>{
    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(NullWritable.get(),value);
        }
    }
}
class DesComparator extends WritableComparator{

    public DesComparator() {
        super(LongWritable.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        return -super.compare(a, b);
    }
}
