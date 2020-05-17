package org.orcan.dfs;

import org.orcan.job.Job;

import java.awt.*;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

public class Shell {

    public static final String MASTER_NODE_NAME = "namenode";

    public static void runMR(Job job, String input, String output) {
        ProcessBuilder builder = new ProcessBuilder();
        builder.command(
                "docker", "exec", MASTER_NODE_NAME, "hadoop",
                "jar", createJarPath(job), job.getClassName(), input, output);

        try {

            Process process = builder.start();

            StringBuilder result = new StringBuilder();

            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));

            String line;
            while ((line = reader.readLine()) != null) {
                result.append(line).append("\n");
            }

            int exitVal = process.waitFor();
            if (exitVal == 0) {
                System.out.println("Success!");
                System.out.println(result.toString());
                openWebpage(URI.create("http://localhost:9870/explorer.html#" + output));
            }

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static String createJarPath(Job job) {
        return "/jobs/" + job.getJarName() + '/' + job.getJarName() + ".jar";
    }

    public static boolean openWebpage(URI uri) {
        Desktop desktop = Desktop.isDesktopSupported() ? Desktop.getDesktop() : null;
        if (desktop != null && desktop.isSupported(Desktop.Action.BROWSE)) {
            try {
                desktop.browse(uri);
                return true;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return false;
    }

    public static boolean openWebpage(URL url) {
        try {
            return openWebpage(url.toURI());
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        return false;
    }
}
