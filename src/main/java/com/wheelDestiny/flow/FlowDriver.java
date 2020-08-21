package com.wheelDestiny.flow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1,获取Job实例
        Job job = Job.getInstance(new Configuration());

        //2,设置类路径
        job.setJarByClass(FlowDriver.class);

        //3,设置Mapper，Reducer
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        //4,设置输入输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        /*
        * 导包的时候要注意，hadoop有两代jar，切近引用关系层级多的（路径长的）
        * 为什么有两套的原因是为了升级后兼容之前的代码
        * */
        //5,设置输入输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //6,提交
        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);
    }
}
