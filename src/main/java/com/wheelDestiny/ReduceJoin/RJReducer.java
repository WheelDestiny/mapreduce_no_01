package com.wheelDestiny.ReduceJoin;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class RJReducer extends Reducer<OrderBean, NullWritable,OrderBean,NullWritable> {

    //每一对K,V都会执行一次reduce，所以可以获取到所有的K,V值
    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        //获取整个组的数据集
        Iterator<NullWritable> iterator = values.iterator();
        //获取第一条数据，可以得到Pname
        iterator.next();
        String pname = key.getPname();
        //遍历剩下的数据，给剩余数据的Pname赋值
        while (iterator.hasNext()){
            iterator.next();
            key.setPname(pname);
            context.write(key,NullWritable.get());
        }
    }
}
