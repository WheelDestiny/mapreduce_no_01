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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class MapJoin extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new MapJoin(),args));
    }
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf,"DistinctMR");

        job.setJarByClass(MapJoin.class);

        job.setMapperClass(MapJoinMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(0);
        job.addCacheFile(URI.create("file:///d:/input/hello11.txt"));

        FileInputFormat.setInputPaths(job,new Path("D:\\input\\hello22.txt"));

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

class MapJoinMapper extends Mapper<LongWritable, Text,LongWritable,Text> {
    private Map<Long,String> cacheMap = new HashMap<>();
    private LongWritable k = new LongWritable();
    private Text v = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles();
        String path = cacheFiles[0].getPath().toString();
        BufferedReader reader = new BufferedReader(new FileReader(path));
        String line = "";
        while ((line = reader.readLine())!=null){
            String[] lines = line.split("\t");
            long id = Long.parseLong(lines[0]);
            String name = lines[1];
            cacheMap.put(id,name);
        }
        reader.close();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] vs = value.toString().split("\t");
        long id = Long.parseLong(vs[0]);
        String age = vs[1];
        String name = cacheMap.get(id);
        if(name != null){
            k.set(id);
            v.set(name+"##"+age);
            context.write(k,v);
        }
    }
}


