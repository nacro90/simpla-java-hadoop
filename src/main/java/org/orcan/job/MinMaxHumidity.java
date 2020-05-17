package org.orcan.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.orcan.map.MinMaxHumidityMapper;
import org.orcan.reduce.MinMaxHumidityReducer;
import org.orcan.tuple.MinMax;

import java.security.PrivilegedExceptionAction;

public class MinMaxHumidity {
    public static void main(final String[] args) throws Exception {
        /* set the hadoop system parameter */
//        System.setProperty("hadoop.home.dir", "Replace this string with hadoop home directory location");
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Minimum and Maximum Humidity values by station");
        job.setJarByClass(MinMaxHumidity.class);

        job.setMapperClass(MinMaxHumidityMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MinMax.class);

        job.setReducerClass(MinMaxHumidityReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MinMax.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
