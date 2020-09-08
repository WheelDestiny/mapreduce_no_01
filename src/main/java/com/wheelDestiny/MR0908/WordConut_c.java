package com.wheelDestiny.MR0908;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordConut_c {
    public static class WordCountMapper extends Mapper<LongWritable, Text,Text,LongWritable> {

        private Text k = new Text();
        private LongWritable v = new LongWritable(1);
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //统计map方法总共执行了多少次
            context.getCounter("wheelDestiny","MapNum").increment(1L);
            String[] s = value.toString().split(" ");

            for (String ss : s) {
//                Random random = new Random();
//                int i = random.nextInt(10);

                k.set(ss);
                context.write(k,v);
            }

        }
    }
    public static class WordCountRedece extends Reducer<Text,LongWritable,Text,LongWritable>{
        private LongWritable v = new LongWritable();
        private long sum = 0;
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            //统计reduce方法一共执行了多少次
            context.getCounter("wheelDestiny","ReduceNum").increment(1L);
            sum =0;
            values.forEach((vv)->{
                sum+=vv.get();
            });
            v.set(sum);
            context.write(key,v);

        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        //map输出压缩
        configuration.set("mapreduce.map.output.compress","true");
        configuration.set("mapreduce.map.output.compress.codec", SnappyCodec.class.getName());


        Job job = Job.getInstance(configuration,"WordCount");

        job.setJarByClass(WordConut_c.class);

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountRedece.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setCombinerClass(WordCountRedece.class);

        FileInputFormat.setInputPaths(job,new Path("D:\\input"));

        FileSystem fs = FileSystem.get(configuration);
        Path out = new Path("D:\\output");
        if(fs.exists(out)){
            //递归删除
            fs.delete(out,true);
        }

        FileOutputFormat.setOutputPath(job,out);

        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);

    }
}