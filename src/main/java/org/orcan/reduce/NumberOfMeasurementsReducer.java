package org.orcan.reduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class NumberOfMeasurementsReducer extends Reducer<Text, IntWritable, Text, LongWritable> {
    private final LongWritable number = new LongWritable(0);

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        number.set(0);
        for (IntWritable ignored : values) {
            number.set(number.get() + 1);
        }
        context.write(key, number);
    }
}
