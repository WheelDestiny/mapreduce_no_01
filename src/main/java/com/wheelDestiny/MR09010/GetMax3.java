package com.wheelDestiny.MR09010;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * 求极值
 */
public class GetMax3 extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new GetMax3(),args));
    }
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf,"DistinctMR");

        job.setJarByClass(GetMax3.class);

        job.setMapperClass(GetMax3Mapper.class);
        job.setReducerClass(GetMax3Reducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ValueWriteable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(ValueWriteable.class);

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

class GetMax3Mapper extends Mapper<LongWritable,Text,Text,ValueWriteable>{
    private Text k = new Text("max");
    private ValueWriteable v = new ValueWriteable();
    long maxNum = Long.MIN_VALUE;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        if(split.length!=2){
            context.getCounter("wheelDestiny","useless").increment(1L);
            return;
        }
        String word = split[0];
        long num = Long.parseLong(split[1]);

        if( maxNum<num){
            maxNum = num;
            v.setName(new Text(word));
            v.setNum(maxNum);
        }
    }
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(k,v);
    }
}

class GetMax3Reducer extends Reducer<Text,ValueWriteable,Text,ValueWriteable>{
    private ValueWriteable v = new ValueWriteable();
    long maxNum = Long.MIN_VALUE;
    @Override
    protected void reduce(Text key, Iterable<ValueWriteable> values, Context context) throws IOException, InterruptedException {
        values.forEach((valueWriteable)->{
            if(valueWriteable.getNum()>maxNum){
                maxNum = valueWriteable.getNum();
                v = valueWriteable;
            }
        });
        context.write(key,v);
    }
}