package org.orcan.dfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class Uploader {

    public static final String DEFAULT_HDFS_PORT = "hdfs://localhost:9000";
    public static final String DEFAULT_FS_NAME = "fs.defaultFS";


    public static boolean upload(String localPath, String containerPath) {
        System.setProperty("HADOOP_USER_NAME", "root");

        Configuration conf = new Configuration();
        conf.set(DEFAULT_FS_NAME, DEFAULT_HDFS_PORT);
        try {
            FileSystem fs = FileSystem.get(conf);
            fs.copyFromLocalFile(new Path(localPath), new Path(containerPath));
            System.out.println("DATA UPLOADED!!!");
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("PRBLEMMM!!!");
            return false;
        }

    }
}
