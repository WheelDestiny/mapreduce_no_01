package com.wheelDestiny.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

public class MyRecordWriter extends RecordWriter<LongWritable, Text> {
    //分别写入两个地址，需要两个本地流
    private FSDataOutputStream baidu;
    private FSDataOutputStream google;
    /**
     * 初始化方法，需要自己调用
     */
    public void initialize(TaskAttemptContext job) throws IOException {
        String outdir = job.getConfiguration().get(FileOutputFormat.OUTDIR);
        FileSystem fileSystem = FileSystem.get(job.getConfiguration());
        baidu = fileSystem.create(new Path(outdir+"/baidu.log"));
        google = fileSystem.create(new Path(outdir+"/google.log"));

    }

    /**
     * 将K,V写出，每对K,V调用一次该方法
     * @param key
     * @param value
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void write(LongWritable key, Text value) throws IOException, InterruptedException {
        String out = value.toString() + "\n";
        if(out.contains("baidu")){
            baidu.write(out.getBytes());
        }else {
            google.write(out.getBytes());
        }

    }

    /**
     * 关闭资源
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(baidu);
        IOUtils.closeStream(google);
    }
}
