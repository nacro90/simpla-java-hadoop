package org.orcan.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.orcan.map.StdTemperatureMapper;
import org.orcan.reduce.AverageTemperatureReducer;
import org.orcan.reduce.StdTemperatureReducer;
import org.orcan.tuple.Average;
import org.orcan.tuple.Std;

import java.util.Arrays;

public class StdTemperatureByStation {
    public static void main(String[] args) throws Exception {

        System.setProperty("HADOOP_USER_NAME", "root");

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Standard deviation of all temperature values of each station in kelvin");
        job.setJarByClass(StdTemperatureByStation.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(StdTemperatureMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setReducerClass(StdTemperatureReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Std.class);

        System.out.println(Arrays.toString(args));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
