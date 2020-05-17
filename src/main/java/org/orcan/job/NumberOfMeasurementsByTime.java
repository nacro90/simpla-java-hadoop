package org.orcan.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.orcan.map.NumberOfMeasurementsMapper;
import org.orcan.reduce.NumberOfMeasurementsReducer;

import java.util.Arrays;

public class NumberOfMeasurementsByTime {
    public static void main(String[] args) throws Exception {

        System.setProperty("HADOOP_USER_NAME", "root");

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Number of measurements in each timestamp");
        job.setJarByClass(NumberOfMeasurementsByTime.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(NumberOfMeasurementsMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(NumberOfMeasurementsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        System.out.println(Arrays.toString(args));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
