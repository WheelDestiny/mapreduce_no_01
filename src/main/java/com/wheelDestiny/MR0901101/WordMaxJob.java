package com.wheelDestiny.MR0901101;

import com.wheelDestiny.MR09011.WordConut;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

import java.io.IOException;
import java.util.List;

public class WordMaxJob extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new WordMaxJob(), args));
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();

        //获取任务链对象
        JobControl jobControl = new JobControl("WordMaxJob");

        //创建任务链上运行的任务所需要的ControlledJob对象
        //wordcount的ControlledJob对象
        ControlledJob WcJob = getWordCountControlledJob(conf,args);
        //maxword的ControlledJob对象
        ControlledJob MWJob = getMaxWordControlledJob(conf,args);

        //设置ControlledJob对象间的关系
        MWJob.addDependingJob(WcJob);

        //把ControlledJob对象添加到任务链上
        jobControl.addJob(WcJob);
        jobControl.addJob(MWJob);

        //开启线程来监控任务链的运行情况，如果任务链都运行完，就停止任务链
        new Thread(()->{
            long startTime = System.currentTimeMillis();
            //循环等待任务链上的所有任务运行完成
            while (!jobControl.allFinished()){
                try {
                    Thread.sleep(200L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            long endTime = System.currentTimeMillis();
            long runTime = endTime-startTime;

            System.out.println("任务链运行时长："+runTime/1000+"秒");

            //运行失败的任务
            List<ControlledJob> failedJobList = jobControl.getFailedJobList();
            if(failedJobList.isEmpty()){
                //全部任务运行成功
                System.out.println("All job successful");
            }else {
                System.out.println("任务链上运行的任务有部分失败，失败任务如下：");
                for (ControlledJob job : failedJobList) {
                    String jobName = job.getJobName();
                    System.out.println("fail job name ==>"+jobName);
                }
            }
            //关闭任务链
            jobControl.stop();
        }).start();

        //运行任务链，任务完成，任务链不会停，所以需要创建一个监控线程去关闭他
        jobControl.run();
        return 0;
    }

    private ControlledJob getMaxWordControlledJob(Configuration conf, String[] args) throws IOException {
        //创建ControlledJob对象
        ControlledJob cjob = new ControlledJob(conf);
        //创建WordCount运行的job对象和配置job参数

        //加载mapreduce用的配置，生成mapreduce的Job对象
        Job job = Job.getInstance(conf, "MaxWord");

        job.setJarByClass(MaxWord1.class);

        job.setMapperClass(MaxWord1.MaxWord1Mapper.class);
        job.setReducerClass(MaxWord1.MaxWord1Reducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job,new Path(args[1]));
        Path outPath = new Path(args[2]);

        FileSystem fileSystem = FileSystem.get(conf);
        if(fileSystem.exists(outPath)){
            fileSystem.delete(outPath,true);
        }

        FileOutputFormat.setOutputPath(job,outPath);

        cjob.setJob(job);
        return cjob;
    }

    private ControlledJob getWordCountControlledJob(Configuration conf, String[] args) throws IOException {
        // 创建ControlledJob对象
        ControlledJob cjob = new ControlledJob(conf);
        // 创建Wordcount运行的job对象和配置job参数
        // 加载 mapreduce用的配置，生成mapreduce的Job的对象
        Job job = Job.getInstance(conf, "wordcount");

        job.setJarByClass(WordCount.class);
        job.setMapperClass(WordCount.WordCountMapper.class);

        job.setReducerClass(WordCount.WordCountReducer.class);
        job.setMapOutputKeyClass(Text.class);

        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));

        Path outputPath = new Path(args[1]);
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(outputPath)){
            fs.delete(outputPath, true);
            System.out.println("delete outputpath==> " + outputPath.toString() + " success!");
        }
        FileOutputFormat.setOutputPath(job, outputPath);

        cjob.setJob(job);
        return cjob;
    }
}
