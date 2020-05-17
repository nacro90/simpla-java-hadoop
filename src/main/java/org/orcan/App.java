package org.orcan;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.orcan.gui.AppWindow;

import java.io.IOException;


/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) throws IOException {
        new AppWindow().setVisible(true);
    }
}
