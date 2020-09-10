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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * 求极值
 */
public class GetMaxMin extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new GetMaxMin(),args));

    }
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf,"DistinctMR");

        job.setJarByClass(GetMaxMin.class);

        job.setMapperClass(GetMaxMinMapper.class);
        job.setReducerClass(GetMaxMinReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
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

class GetMaxMinMapper extends Mapper<LongWritable,Text,Text,Text>{
    private Text kMax = new Text("max");
    private Text vMax = new Text();

    private Text kMin = new Text("min");
    private Text vMin = new Text();
    long maxNum = Long.MIN_VALUE;
    long minNum = Long.MAX_VALUE;
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
            vMax.set(word+"##"+maxNum);
        }
        if(minNum>num){
            minNum = num;
            vMin.set(word+"##"+minNum);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(kMax,vMax);
        context.write(kMin,vMin);
    }
}

class GetMaxMinReducer extends Reducer<Text,Text,Text,Text>{
    private Text v = new Text();
    private long maxNum = Long.MIN_VALUE;
    private long minNum = Long.MAX_VALUE;

    private MultipleOutputs<Text,Text> outputs = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        outputs = new MultipleOutputs<>(context);


    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        if(key.toString().equals("max")){
            values.forEach((vv)->{
                String[] split = vv.toString().split("##");
                long num = Long.parseLong(split[1]);
                if(num>maxNum){
                    maxNum = num;
                    v.set(vv);
                }
            });
            context.write(key,v);

            outputs.write(key,v,"maxOut/max");
        }else {
            values.forEach((vv)->{
                String[] split = vv.toString().split("##");
                long num = Long.parseLong(split[1]);
                if(num<minNum){
                    minNum = num;
                    v.set(vv);
                }
            });
            context.write(key,v);

            outputs.write(key,v,"minOut/min");
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        outputs.close();
    }
}