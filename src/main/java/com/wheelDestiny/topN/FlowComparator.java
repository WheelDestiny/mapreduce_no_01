package com.wheelDestiny.topN;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

//使所有数据分到一组
public class FlowComparator extends WritableComparator {

    public FlowComparator() {
        super(FlowBean.class,true);
    }

    //三个同名方法，不同参数，一定要重写这个方法
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        return 0;
    }
}
