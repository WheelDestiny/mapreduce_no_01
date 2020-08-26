package com.wheelDestiny.MapJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;

public class MJDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(MJDriver.class);

        job.setMapperClass(MJMapper.class);
        //将ReduceTask设为0，可以不经过shuffle和reducer
        //好处是完全杜绝了数据倾斜
        job.setNumReduceTasks(0);

        //加入缓存文件
        job.addCacheFile(URI.create("file:///d:/input/pd.txt"));

        FileInputFormat.setInputPaths(job,new Path("D:\\input\\order.txt"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\output"));

        boolean b = job.waitForCompletion(true);

        System.exit(b?0:1);


    }

}
