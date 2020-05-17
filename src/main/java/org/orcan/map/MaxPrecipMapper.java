package org.orcan.map;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.orcan.tuple.MinMax;

import java.io.IOException;

public class MaxPrecipMapper extends Mapper<LongWritable, Text, LongWritable, FloatWritable> {
    private final FloatWritable precip = new FloatWritable();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String data = value.toString();
        String[] field = data.split(",", -1);
        if (field.length == 13 && !field[8].isEmpty()) {
            float precipValue = Float.parseFloat(field[8]);
            precip.set(precipValue);

            context.write(new LongWritable(0), precip);
        }
    }
}
