package org.orcan.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.orcan.map.AverageTemperatureMapper;
import org.orcan.reduce.AverageTemperatureReducer;
import org.orcan.tuple.Average;

import java.util.Arrays;

public class AverageTemperatureByStation {
    public static void main(String[] args) throws Exception {

        System.setProperty("HADOOP_USER_NAME", "root");

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Average of all temperature values by stations in kelvin");
        job.setJarByClass(AverageTemperatureByStation.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(AverageTemperatureMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Average.class);

        job.setReducerClass(AverageTemperatureReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        System.out.println(Arrays.toString(args));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
