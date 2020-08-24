package com.wheelDestiny.ReduceJoin;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class RJComparator extends WritableComparator {

    //传递一个容器进去
    public RJComparator() {
        super(OrderBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean oa = (OrderBean)a;
        OrderBean ob = (OrderBean)b;

        //按照Pid进行排序，可以使相同Pid的数据进入同一组
        return oa.getPid().compareTo(ob.getPid());
    }
}
