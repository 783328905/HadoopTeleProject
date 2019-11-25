package com.ctillnow.com.ctillnow.kv;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CountDurationValue extends BaseValue {

    private int count;
    private int duration;

    public CountDurationValue() {
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public int getDuration() {
        return duration;
    }

    public void setDuration(int duration) {
        this.duration = duration;
    }

    @Override
    public String toString() {
        return count + "\t" + duration;
    }


    public void write(DataOutput out) throws IOException {
        out.writeInt(this.count);
        out.writeInt(this.duration);
    }


    public void readFields(DataInput in) throws IOException {
        this.count = in.readInt();
        this.duration = in.readInt();
    }
}
