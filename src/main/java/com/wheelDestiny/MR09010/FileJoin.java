package com.wheelDestiny.MR09010;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FileJoin extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new FileJoin(),args));

    }
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf,"DistinctMR");

        job.setJarByClass(FileJoin.class);

        job.setMapperClass(FileJoinMapper.class);
        job.setReducerClass(FileJoinReducer.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPaths(job,"D:\\input/hello11.txt,D:\\input/hello22.txt");

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
class FileJoinMapper extends Mapper<LongWritable, Text,LongWritable,Text>{
    private LongWritable k = new LongWritable();
    private Text v = new Text();
    private String pathMark;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit split =(FileSplit) context.getInputSplit();
        String path = split.getPath().toString();
        if (path.contains("11")){
            pathMark = "hello11";
        }else {
            pathMark = "hello22";
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        long id = Long.parseLong(split[0]);
        String str = split[1];

        k.set(id);
        v.set(str+"##"+pathMark);
        context.write(k,v);

    }
}
class FileJoinReducer extends Reducer<LongWritable,Text,LongWritable,Text>{
    private Text v = new Text();

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String> list1 = new ArrayList<>();
        List<String> list2 = new ArrayList<>();
        values.forEach((t)->{
            String[] split = t.toString().split("##");
            String s1 = split[0];
            String s2 = split[1];
            if (s2.equals("hello11")){
                list1.add(s1);
            }else {
                list2.add(s1);
            }
        });
        for (String s1:list1){
            for (String s2 : list2) {
                v.set(s1+"\t"+s2);
                context.write(key,v);
            }
        }
    }
}