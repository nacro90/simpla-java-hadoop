package org.orcan.reduce;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.orcan.tuple.MinMax;

import java.io.IOException;

public class MaxPrecipReducer extends Reducer<LongWritable, FloatWritable, Text, FloatWritable> {
    private final FloatWritable max = new FloatWritable(0);

    public void reduce(LongWritable key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        for (FloatWritable val : values) {
            if (val.get() > max.get()) {
                max.set(val.get());
            }
        }
        context.write(new Text("Max precipitation"), max);
    }
}
