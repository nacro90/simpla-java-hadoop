package org.orcan.map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.orcan.tuple.Average;

import java.io.IOException;

public class NumberOfMeasurementsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final String DELIMITER = ",";
    private final Text date = new Text();
    private final IntWritable exists = new IntWritable(1);

    /*
     * Map method of Mapper class, takes input as one line of input file at a time
     * and cleans it. Gives output as key-value pair, where key is cleaned line and
     * value is number of errors in that line.
     */
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] split = value.toString().split(DELIMITER);
        if (split.length >= 5) {
            String dateString = split[5];

            if (dateString != null && !dateString.isEmpty()) {
                try {
                    date.set(dateString);
                    context.write(date, exists);
                } catch (NumberFormatException ignored) {
                }
            }
        }
    }
}
