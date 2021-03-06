package org.orcan.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.orcan.map.MaxPrecipMapper;
import org.orcan.reduce.MaxPrecipReducer;

public class MaxPrecip {
    public static void main(final String[] args) throws Exception {
        /* set the hadoop system parameter */
//        System.setProperty("hadoop.home.dir", "Replace this string with hadoop home directory location");
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Max precipitation");
        job.setJarByClass(MaxPrecip.class);

        job.setMapperClass(MaxPrecipMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(FloatWritable.class);

        job.setReducerClass(MaxPrecipReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
