package org.orcan.reduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class CsvCleanReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();
    private MultipleOutputs<Text, IntWritable> mos;
    private int sum = 0;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        mos = new MultipleOutputs<Text, IntWritable>(context);
    }

    /* Reduce method of reducer class, adds error count and also
     * writes the cleaned input CSV file lines to the new file
     * on HDFS.
     */
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        for (IntWritable val : values) {
            sum += val.get();
        }
        mos.write("clean", key, NullWritable.get());
    }

    /* cleanup method writes the total error count
     * to the new file on the HDFS.
     */
    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        result.set(sum);
        mos.write("totalerrors", result, NullWritable.get());
        mos.close();
    }
}
