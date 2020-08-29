package com.wheelDestiny.findfriend;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FFMapper2 extends Mapper<LongWritable, Text,Text,Text> {
    private Text k = new Text();
    private Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split1 = value.toString().split("\t");
        v.set(split1[0]);
        String[] split2 = split1[1].split(",");
        for (int i = 0; i < split2.length; i++) {
            for (int j = i+1; j < split2.length; j++) {
                if(split2[i].compareTo(split2[j])>0){
                    k.set(split2[j]+"-"+split2[i]);
                }else {
                    k.set(split2[i]+"-"+split2[j]);
                }
                context.write(k,v);
            }
        }
    }
}
