package org.orcan.reduce;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.orcan.tuple.Average;

import java.io.IOException;

public class AverageTemperatureReducer extends Reducer<Text, Average, Text, DoubleWritable> {
    private final DoubleWritable result = new DoubleWritable();

    public void reduce(Text key, Iterable<Average> values, Context context) throws IOException, InterruptedException {

        long count = 0;
        double sum = 0;
        for (Average val : values) {
            sum += val.getAverage() * val.getCount();
            count += val.getCount();
        }
        result.set(sum / count);
        context.write(key, result);
    }
}
