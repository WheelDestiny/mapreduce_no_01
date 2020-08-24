package com.wheelDestiny.writablecomparable;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

//排序实现接口WritableComparable
public class FlowBean implements WritableComparable<FlowBean> {
    private long upFlow;
    private long downFlow;
    private long sumFlow;

    public void set(long upFlow,long downFlow){
        this.downFlow = downFlow;
        this.upFlow = upFlow;
        this.sumFlow = downFlow+upFlow;
    }

    @Override
    public String toString() {
        return "FlowBean{" +
                "upFlow=" + upFlow +
                ", downFlow=" + downFlow +
                ", sumFlow=" + sumFlow +
                '}';
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }


    /**
     * 序列化方法
     * @param dataOutput    框架提供的数据出口
     * @throws IOException
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(sumFlow);
    }

    /*
    *******************序列化和反序列化的参数赋值顺序必须一致！！！！！！*****************************
     */

    /**
     *反序列化方法
     * @param dataInput     框架提供的数据来源
     * @throws IOException
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        upFlow = dataInput.readLong();
        downFlow  = dataInput.readLong();
        sumFlow  = dataInput.readLong();

    }

    @Override
    public int compareTo(FlowBean o) {
        return o.getSumFlow()<sumFlow?-1:((o.getSumFlow()==sumFlow?0:1));
    }

}
