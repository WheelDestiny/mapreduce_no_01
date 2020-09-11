package com.wheelDestiny.MR09011;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SecondSortBean implements WritableComparable<SecondSortBean> {
    private String word = "";
    private int num = 0;

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(word);
        out.writeInt(num);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        word = in.readUTF();
        num = in.readInt();
    }

    @Override
    public int compareTo(SecondSortBean o) {
        int i = this.word.compareTo(o.word);
        if (i==0){
            return o.num-this.num;
        }
        return i;
    }

    @Override
    public String toString() {
        return "SecondSortBean{" +
                "word='" + word + '\'' +
                ", num=" + num +
                '}';
    }
}
