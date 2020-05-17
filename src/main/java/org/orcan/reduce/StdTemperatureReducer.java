package org.orcan.reduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.orcan.tuple.Std;

public class StdTemperatureReducer extends Reducer<Text, DoubleWritable, Text, Std> {
    public List<Double> list = new ArrayList<>();
    public Std std = new Std();

    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double sum = 0;
        double count = 0;
        list.clear();
        std.setMedian(0);
        std.setSd(0);
        for (DoubleWritable doubleWritable : values) {
            sum = sum + doubleWritable.get();
            count = count + 1;
            list.add(doubleWritable.get());
        }
        Collections.sort(list);
        int length = list.size();
        double median;
        if (length == 2) {
            double medianSum = list.get(0) + list.get(1);
            median = medianSum / 2;
        } else if (length % 2 == 0) {
            double medianSum = list.get((length / 2) - 1) + list.get(length / 2);
            median = medianSum / 2;
        } else {
            median = list.get(length / 2);
        }
        std.setMedian(median);
        double mean = sum / count;
        double sumOfSquares = 0;
        for (double doubleWritable : list) {
            sumOfSquares += (doubleWritable - mean) * (doubleWritable - mean);
        }
        std.setSd(Math.sqrt(sumOfSquares / (count - 1)));
        context.write(key, std);
    }
}