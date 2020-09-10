package com.wheelDestiny.MR09010;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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
public class GetMax extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new GetMax(),args));

    }
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf,"DistinctMR");

        job.setJarByClass(GetMax.class);

        job.setMapperClass(GetMaxMapper.class);
        job.setReducerClass(GetMaxReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

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
class GetMaxMapper extends Mapper<LongWritable,Text,Text,LongWritable>{
    private Text k = new Text();
    private LongWritable v = new LongWritable();
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
            k.set(word);
            maxNum = num;
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        v.set(maxNum);
        context.write(k,v);

    }
}
class GetMaxReducer extends Reducer<Text,LongWritable,Text,LongWritable>{
    private Text k = new Text();
    private LongWritable v = new LongWritable();
    long maxNum = Long.MIN_VALUE;
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        values.forEach((vv)->{
            if(vv.get()>maxNum){
                maxNum = vv.get();
                k.set(key.toString());
            }
        });
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        v.set(maxNum);
        context.write(k,v);

    }
}