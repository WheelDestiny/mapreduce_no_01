package com.wheelDestiny.MR0908;

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

public class WordConut_d extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new WordConut_d(),args));

    }
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf,"WordCount");

        job.setJarByClass(WordConut_d.class);

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountRedece.class);

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

    public static class WordCountMapper extends Mapper<LongWritable, Text,Text,LongWritable> {

        private Text k = new Text();
        private LongWritable v = new LongWritable(1);
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //统计map方法总共执行了多少次
            context.getCounter("wheelDestiny","MapNum").increment(1L);
            String[] s = value.toString().split(" ");

            for (String ss : s) {
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
            context.getCounter("wheelDestiny","ReduceNum").increment(1L);
            sum =0;
            values.forEach((vv)->{
                sum+=vv.get();
            });
            v.set(sum);
            context.write(key,v);
        }
    }
}