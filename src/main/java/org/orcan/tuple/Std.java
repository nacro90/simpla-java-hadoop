package org.orcan.tuple;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Std implements Writable {
    private double sd;
    private double median;

    public double getSd() {
        return sd;
    }

    public void setSd(double sd) {
        this.sd = sd;
    }

    public double getMedian() {
        return median;
    }

    public void setMedian(double median) {
        this.median = median;
    }

    public void readFields(DataInput in) throws IOException {
        sd = in.readDouble();
        median = in.readDouble();
    }

    public void write(DataOutput out) throws IOException {
        out.writeDouble(getSd());
        out.writeDouble(getMedian());
    }

    @Override
    public String toString() {
        return sd + "\t" + median;
    }

}