package com.wheelDestiny.MR09015.MRDriver;

import com.wheelDestiny.MR09012.WordMaxJob;
import com.wheelDestiny.MR09015.WordCountJob;
import org.apache.hadoop.util.ProgramDriver;

/**
 * 定义一个driver
 */
public class MRDriver {
    public static void main(String[] args) {
        ProgramDriver programDriver = new ProgramDriver();
        try {
            //运行参数wordMax ，根据这个参数找到运行的主类
            programDriver.addClass("WordCountJob", WordCountJob.class,"求...");
            System.exit(programDriver.run(args));
        }catch (Throwable e){
            e.printStackTrace();
        }

    }
}
