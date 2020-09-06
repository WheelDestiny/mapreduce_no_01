package com.wheelDestiny.groupingcomparator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class OrderReducer extends Reducer<OrderBean, NullWritable,OrderBean,NullWritable> {

    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<NullWritable> iterator = values.iterator();

        for (int i = 0; i <2 ; i++) {
            if (iterator.hasNext()) {
                //即使根据key分区，但是实际上迭代器中value对应的key其实还是原来对应的key，所以可以根据迭代器获取同一分区中所有的key
                context.write(key,iterator.next());
            }
        }
    }
}
