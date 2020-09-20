package com.wheelDestiny.MR09011;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SortBean implements WritableComparable<SortBean> {

    private int num = 0;

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }

    @Override
    public int compareTo(SortBean o) {
        return o.num-this.num;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(num);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        num = in.readInt();

    }
}
