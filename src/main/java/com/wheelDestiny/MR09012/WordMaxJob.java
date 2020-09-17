package com.wheelDestiny.MR09012;

import com.wheelDestiny.MR09012.Util.JobRunResult;
import com.wheelDestiny.MR09012.Util.JobRunUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.ArrayList;
import java.util.List;

public class WordMaxJob extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new WordMaxJob(), args));
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        //获取任务链对象
        JobControl jobControl = new JobControl("WordMaxJob");

        //创建任务链上运行的任务所需要的ControlledJob对象
        WordCount wordCount = new WordCount();
        //只需要加载一次
        wordCount.setConf(conf);
        ControlledJob WcJob = wordCount.getControlledJob();
        //maxword的ControlledJob对象
        MaxWord1 maxWord1 = new MaxWord1();
        ControlledJob MWJob =maxWord1.getControlledJob();

        //设置ControlledJob对象间的关系
        MWJob.addDependingJob(WcJob);

        //把ControlledJob对象添加到任务链上
        jobControl.addJob(WcJob);
        jobControl.addJob(MWJob);

        JobRunResult runResult = JobRunUtil.run(jobControl);
        runResult.print(true);

//        //开启线程来监控任务链的运行情况，如果任务链都运行完，就停止任务链
//        new Thread(()->{
//            long startTime = System.currentTimeMillis();
//            //循环等待任务链上的所有任务运行完成
//            while (!jobControl.allFinished()){
//                try {
//                    Thread.sleep(200L);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//            }
//            long endTime = System.currentTimeMillis();
//            long runTime = endTime-startTime;
//
//            System.out.println("任务链运行时长："+runTime/1000+"秒");
//
//            //运行失败的任务
//            List<ControlledJob> failedJobList = jobControl.getFailedJobList();
//            if(failedJobList.isEmpty()){
//                //全部任务运行成功
//                System.out.println("All job successful");
//            }else {
//                System.out.println("任务链上运行的任务有部分失败，失败任务如下：");
//                for (ControlledJob job : failedJobList) {
//                    String jobName = job.getJobName();
//                    System.out.println("fail job name ==>"+jobName);
//                }
//            }
//            //关闭任务链
//            jobControl.stop();
//        }).start();
//
//
//
//        //运行任务链，任务完成，任务链不会停，所以需要创建一个监控线程去关闭他
//        jobControl.run();
        return 0;
    }

}
