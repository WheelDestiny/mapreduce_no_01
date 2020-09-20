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

public class SortMR1 extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new SortMR1(),args));
    }
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf,"DistinctMR");

        job.setJarByClass(SortMR1.class);

        job.setMapperClass(SortMapper1.class);
        job.setReducerClass(SortReducer1.class);

        job.setMapOutputKeyClass(SortBean.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);


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

class SortMapper1 extends Mapper<LongWritable, Text,SortBean,Text>{
    private SortBean k = new SortBean();
    private Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] files = value.toString().split("\t");
        String name = files[0];
        int num = Integer.parseInt(files[1]);
        k.setNum(num);
        v.set(num+"\t"+name);
        context.write(k,v);
    }
}

class SortReducer1 extends Reducer<SortBean, Text, NullWritable,Text>{
    @Override
    protected void reduce(SortBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(NullWritable.get(),value);
        }
    }
}
