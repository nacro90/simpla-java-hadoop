package org.orcan.reduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.orcan.tuple.MinMax;

public class MinMaxHumidityReducer extends Reducer<Text, MinMax, Text, MinMax> {
    private final MinMax result = new MinMax(Double.MAX_VALUE, Double.MIN_VALUE, 0);

    public void reduce(Text key, Iterable<MinMax> values, Context context) throws IOException, InterruptedException {
        long sum = 0;
        for (MinMax val : values) {
            if (result.getMax() == null || (val.getMax() > result.getMax())) {
                result.setMax(val.getMax());
            }
            if (result.getMin() == null || (val.getMin() < result.getMin())) {
                result.setMin(val.getMin());
            }
            sum = sum + val.getCount();
            result.setCount(sum);
        }
        context.write(new Text(key.toString()), result);
    }
}
