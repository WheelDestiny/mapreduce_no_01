package com.wheelDestiny.etl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ETLMapper extends Mapper<LongWritable, Text,Text, NullWritable> {
    private Text k = new Text();
    private StringBuilder stringBuilder = new StringBuilder();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        String res = handleLine(line);

        if(res == null){
            context.getCounter("ETL","False").increment(1);
        }else {
            k.set(res);
            context.getCounter("ETL","True").increment(1);

            context.write(k,NullWritable.get());
        }


    }

    /**
     * 处理掉不和长度的数据
     * @param line  输入行
     * @return  处理后的结果
     */
    private String handleLine(String line) {
        String[] split = line.split("\t");
        if(split.length<9){
            return null;
        }
        stringBuilder.delete(0,stringBuilder.length());

        split[3] = split[3].replace(" ","");

        for (int i = 0; i < split.length; i++) {
            if(i==split.length-1){
                stringBuilder.append(split[i]);
            }else if(i<9){
                stringBuilder.append(split[i]).append("\t");
            }else {
                stringBuilder.append(split[i]).append("&");
            }
        }
        return stringBuilder.toString();
    }
}
