package com.wheelDestiny.invertIndex;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IIReducer2 extends Reducer<Text,Text,Text,Text> {
    private Text v = new Text();
    private StringBuilder stringBuilder = new StringBuilder();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        stringBuilder.delete(0,stringBuilder.length());
        values.forEach((i)->{
            stringBuilder.append(i.toString()).append(" ");
        });
        v.set(stringBuilder.toString());
        context.write(key,v);
    }
}
