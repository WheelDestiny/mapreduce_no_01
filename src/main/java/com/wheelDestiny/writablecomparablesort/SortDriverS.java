package com.wheelDestiny.writablecomparablesort;

import com.wheelDestiny.writablecomparable.FlowBean;
import com.wheelDestiny.writablecomparable.SortMapper;
import com.wheelDestiny.writablecomparable.SortReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SortDriverS {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(SortDriverS.class);
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);

        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        job.setNumReduceTasks(5);
        job.setPartitionerClass(MyPartitionerS.class);

        FileInputFormat.setInputPaths(job,new Path(""));
        FileOutputFormat.setOutputPath(job,new Path(""));

        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);
    }
}
