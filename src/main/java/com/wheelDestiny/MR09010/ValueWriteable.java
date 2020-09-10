package com.wheelDestiny.MR09010;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ValueWriteable implements Writable {

    private Text name = new Text();
    private long num = 0L;

    public Text getName() {
        return name;
    }

    public void setName(Text name) {
        this.name = name;
    }

    public long getNum() {
        return num;
    }

    public void setNum(long num) {
        this.num = num;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(name.toString());
        out.writeLong(num);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        name.set(in.readUTF());
        num = in.readLong();

    }

    @Override
    public String toString() {
        return "ValueWriteable{" +
                "name=" + name +
                ", num=" + num +
                '}';
    }
}
