package com.wheelDestiny.findfriend;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FFMapper1 extends Mapper<LongWritable, Text,Text,Text> {
    private Text k = new Text();
    private Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split1 = value.toString().split(":");
        v.set(split1[0]);

        //k，v互换
        String[] split2 = split1[1].split(",");
        for (String s : split2) {
            k.set(s);
            context.write(k,v);
        }

    }
}
