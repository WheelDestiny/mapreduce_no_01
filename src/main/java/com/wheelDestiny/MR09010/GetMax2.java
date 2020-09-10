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
public class GetMax2 extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new GetMax2(),args));

    }
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf,"DistinctMR");

        job.setJarByClass(GetMax2.class);

        job.setMapperClass(GetMax2Mapper.class);
        job.setReducerClass(GetMax2Reducer.class);

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

class GetMax2Mapper extends Mapper<LongWritable,Text,Text,Text>{
    private Text k = new Text("max");
    private Text v = new Text();
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
            v.set(word+"##"+maxNum);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(k,v);
    }
}
class GetMax2Reducer extends Reducer<Text,Text,Text,Text>{
    private Text v = new Text();
    long maxNum = Long.MIN_VALUE;
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        values.forEach((vv)->{
            String[] split = vv.toString().split("##");
            long num = Long.parseLong(split[1]);
            if(num>maxNum){
                maxNum = num;
                v.set(vv);
            }
        });
        context.write(key,v);
    }

}