package org.orcan.map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.orcan.tuple.Average;

import java.io.IOException;

public class AverageTemperatureMapper extends Mapper<LongWritable, Text, Text, Average> {
    private static final String DELIMITER = ",";
    private final Average average = new Average(0, 1);
    private final Text stationNumber = new Text();

    /*
     * Map method of Mapper class, takes input as one line of input file at a time
     * and cleans it. Gives output as key-value pair, where key is cleaned line and
     * value is number of errors in that line.
     */
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] split = value.toString().split(DELIMITER);
        if (split.length >= 11) {
            String tempString = split[11];
            String stationString = split[1];

            if (tempString != null && !tempString.isEmpty() && stationString != null && !stationString.isEmpty()) {
                try {
                    float temperature = Float.parseFloat(tempString);
                    average.setAverage(temperature);
                    stationNumber.set(stationString);
                    context.write(stationNumber, average);
                } catch (NumberFormatException ignored) {
                }
            }
        }
    }
}
