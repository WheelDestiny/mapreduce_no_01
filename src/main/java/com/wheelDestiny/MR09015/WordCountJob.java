package com.wheelDestiny.MR09015;

import com.wheelDestiny.MR09015.Util.JobRunResult;
import com.wheelDestiny.MR09015.Util.JobRunUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCountJob extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new WordCountJob(), args));
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        //获取任务链对象
        JobControl jobControl = new JobControl("WordCountJob");

        //创建任务链上运行的任务所需要的ControlledJob对象
        WordCount wordCount = new WordCount();
        //只需要加载一次
        wordCount.setConf(conf);
        ControlledJob WcJob = wordCount.getControlledJob();


        //把ControlledJob对象添加到任务链上
        jobControl.addJob(WcJob);
        JobRunResult runResult = JobRunUtil.run(jobControl);
        runResult.print(true);
        return 0;
    }

}
