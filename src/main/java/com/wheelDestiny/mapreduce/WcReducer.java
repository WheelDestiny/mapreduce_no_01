package com.wheelDestiny.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


//KEYIN     Reduce输入的泛型来自map输出的泛型
//VALUEIN   同上
//KEYOUT    Text
//VALUEOUT  IntWriteable
public class WcReducer extends Reducer<Text, IntWritable,Text,IntWritable> {
    private int sum = 0;
    private IntWritable total= new IntWritable();
    //两个参数分别是word，以及其对应的多组values
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        values.forEach((intWritable)->{
            sum+= intWritable.get();
        });

        //累加
//        for (IntWritable intWritable:values) {
//            sum+= intWritable.get();
//        }
        //包装结果并把结果写回给context
        total.set(sum);
        context.write(key,total);

    }
}
