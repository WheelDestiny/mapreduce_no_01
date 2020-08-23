package com.wheelDestiny.ReduceJoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class RJMapper extends Mapper<LongWritable, Text,OrderBean, NullWritable> {
    private  OrderBean orderBean = new OrderBean();
    private String fileName; 

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //获取切片信息，获取文件名
        FileSplit fs = (FileSplit)context.getInputSplit();
        fileName = fs.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        //根据不同的文件，分情况写入对象。
        if(fileName.equals("order.txt")){
            orderBean.setId(fields[0]);
            orderBean.setPid(fields[1]);
            orderBean.setAmount(Integer.parseInt(fields[2]));
            //因为orderBean容器是要反复使用的，所以一定要覆盖上一次的属性值
            orderBean.setPname("");
        }else {
            orderBean.setPid(fields[0]);
            orderBean.setPname(fields[1]);
            orderBean.setAmount(0);
            orderBean.setId("");
        }
        context.write(orderBean,NullWritable.get());
    }
    
}
