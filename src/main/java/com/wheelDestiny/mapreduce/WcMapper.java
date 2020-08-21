package com.wheelDestiny.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//KEYIN     LongWritable    offset  行首偏移量，标志当前行到文件开始的距离
//VALUEIN   Text            当前行的内容
//KEYOUT    Text            输出文件的内容
//VALUEOUT  IntWritable     当前数据几次
public class WcMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
    //该方法会多次调用，尽量减少对象的创建，大量的对象创建会频繁地触发GC导致更多的占用机器的资源
    private Text text = new Text();
    private IntWritable intWritable = new IntWritable(1);

    //Context   当前任务，整个计算流程算为一次任务
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //拿到当前行数据
        String line = value.toString();

        //按照空格切分
        String[] words = line.split(" ");


        //遍历数组，把单子变成（word，1）的形式交给框架
        for (String word:words) {
            text.set(word);
            context.write(text,intWritable);
        }
    }
}