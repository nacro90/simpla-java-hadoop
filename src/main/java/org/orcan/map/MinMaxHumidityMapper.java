package org.orcan.map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.orcan.tuple.MinMax;

import java.io.IOException;
public class MinMaxHumidityMapper extends Mapper<LongWritable, Text, Text, MinMax> {
    private final MinMax minMax = new MinMax();
    private final Text station = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String data = value.toString();
        String[] field = data.split(",", -1);
        if (field.length == 13 && !field[4].isEmpty()) {
            double height = Double.parseDouble(field[4]);
            minMax.setMin(height);
            minMax.setMax(height);
            minMax.setCount(1);
            context.write(new Text("minmax"), minMax);
        }
    }
}
