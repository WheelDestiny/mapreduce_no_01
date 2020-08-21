package com.wheelDestiny.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text,FlowBean,Text,FlowBean> {

    private FlowBean sumFlow = new FlowBean();
    private long sumUpFlow = 0;
    private long sumDownFlow = 0;

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        values.forEach((flowBean)->{
            sumUpFlow += flowBean.getUpFlow();
            sumDownFlow += flowBean.getDownFlow();
        });
        sumFlow.set(sumUpFlow,sumDownFlow);
        context.write(key,sumFlow);
    }
}
