package org.orcan.gui;

import java.awt.Point;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;

import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.border.EmptyBorder;

import org.orcan.dfs.FileSystemManager;
import org.orcan.dfs.Shell;
import org.orcan.job.Job;

public class AppWindow extends JFrame {

    private final JButton fileSelectorButton = new JButton("Select data");
    private final JButton uploadButton = new JButton("Upload Data");
    private final JButton startButton = new JButton("Start");

    private final JComboBox<String> selector = new JComboBox<>(
            Arrays.stream(Job.values()).map(Job::getDisplayName).toArray(String[]::new));
    private final JTextField hdfsInputPath = new JTextField("/inputs/weather.csv");
    private final JTextField hdfsOutputPath = new JTextField("/outputs/");
    private final JTextField datasetPath = new JTextField("/home/orcan/dataset/weather-fix.csv");
    private final JTextField containerDatasetPath = new JTextField("/inputs/");

    private final JLabel jobSelectorLabel = new JLabel("MapReduce Job:");
    private final JLabel inputDataLabel = new JLabel("Input Data:");
    private final JLabel outputPathLabel = new JLabel("Output Path:");
    private final JLabel sourcePathLabel = new JLabel("Source Path:");
    private final JLabel destinationPathLabel = new JLabel("Destination Path:");
    private final JLabel infoLabel = new JLabel("More info @ http://localhost:9870");

    public AppWindow() {
        setTitle("Big Data Term Project");
        setSize(548, 560);
        setLocation(new Point(300, 200));
        setLayout(null);

        initializeComponent();
        initializeEvent();
    }

    private void initializeComponent() {

        jobSelectorLabel.setBounds(20, 10, 200, 30);
        selector.setBounds(128, 10, 400, 30);
        add(selector);

        inputDataLabel.setBounds(20, 35, 200, 30);
        hdfsInputPath.setBounds(128, 35, 400, 30);

        outputPathLabel.setBounds(20, 60, 200, 30);
        hdfsOutputPath.setBounds(128, 60, 400, 30);

        startButton.setBounds(128, 90, 400, 30);
        add(startButton);

        sourcePathLabel.setBounds(20, 140, 200, 30);
        datasetPath.setBounds(128, 140, 400, 30);

        fileSelectorButton.setBounds(128, 170, 400, 30);
        add(fileSelectorButton);

        destinationPathLabel.setBounds(20, 200, 200, 30);
        containerDatasetPath.setBounds(128, 200, 400, 30);

        uploadButton.setBounds(128, 230, 400, 30);
        infoLabel.setBounds(20, 280, 400, 30);
        add(uploadButton);

        add(jobSelectorLabel);
        add(inputDataLabel);
        add(outputPathLabel);
        add(sourcePathLabel);
        add(destinationPathLabel);
        add(infoLabel);

        add(hdfsInputPath);
        add(hdfsOutputPath);
        add(datasetPath);
        add(containerDatasetPath);
    }

    private void initializeEvent() {

        this.addWindowListener(new WindowAdapter() {
            public void windowClosing(WindowEvent e) {
                System.exit(1);
            }
        });

        uploadButton.addActionListener(e -> {
            uploadButton.setEnabled(false);
            uploadButton.setText("Uploading...");
            new Thread(() -> {
                if (FileSystemManager.upload(datasetPath.getText(), containerDatasetPath.getText())) {
                    uploadButton.setText("Uploaded successfully");
                } else {
                    uploadButton.setText("Upload failed!");
                }
                uploadButton.setEnabled(true);
            }).start();
        });

        fileSelectorButton.addActionListener(e -> {
            uploadButton.setEnabled(false);
            new Thread(() -> {
                datasetPath.setText(new FileChooser().selectFile());
                uploadButton.setEnabled(true);
            }).start();
        });

        startButton.addActionListener(e -> {
            startButton.setEnabled(false);
            startButton.setText("Running...");
            new Thread(this::btnStartClick).start();
        });
    }

    private void btnStartClick() {
        Shell.runMR(Job.values()[selector.getSelectedIndex()], hdfsInputPath.getText(), hdfsOutputPath.getText());
        startButton.setEnabled(true);
        String localTemp = "/tmp/ORCAN/";
        FileSystemManager.download(localTemp, hdfsOutputPath.getText());
        String hdfsPath = hdfsOutputPath.getText().toString().substring(1);
        hdfsPath = hdfsPath.substring(hdfsPath.indexOf('/') + 1);
        File file = new File(localTemp + hdfsPath + "/part-r-00000");
        try {
            FileInputStream fis = new FileInputStream(file);
            byte[] data = new byte[(int) file.length()];
            fis.read(data);
            fis.close();

            String str = new String(data, "UTF-8");
            JFrame f = new JFrame("RESULT of " + hdfsPath);
            JTextArea t = new JTextArea();

            t.setEditable(false);
            t.setCursor(null);
            t.setOpaque(false);
            t.setFocusable(false);
            t.setBorder(new EmptyBorder(5, 5, 5, 5));
            t.setAlignmentY(JLabel.CENTER_ALIGNMENT);

            JScrollPane s = new JScrollPane(t, JScrollPane.VERTICAL_SCROLLBAR_ALWAYS,
                    JScrollPane.HORIZONTAL_SCROLLBAR_ALWAYS);

            f.getContentPane().add(s);
            f.setSize(300, 300);
            t.setText(str);

            startButton.setText("Start");
            f.setVisible(true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        new AppWindow().setVisible(true);
    }
}
