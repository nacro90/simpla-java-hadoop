package org.orcan.gui;

import org.orcan.job.Job;

import javax.swing.DefaultListModel;
import javax.swing.JFrame;
import javax.swing.JList;
import javax.swing.JScrollPane;
import javax.swing.JTextField;
import javax.swing.ListModel;
import javax.swing.SwingUtilities;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import java.awt.*;

public class JobList {

    private JList jList = createJList();

    public JobList() {
        JFrame frame = new JFrame();
        frame.add(new JScrollPane(jList));
        frame.add(createTextField(), BorderLayout.PAGE_END);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.pack();
        frame.setLocationRelativeTo(null);
        frame.setVisible(true);
    }

    private JTextField createTextField() {
        final JTextField field = new JTextField(15);
        field.getDocument().addDocumentListener(new DocumentListener(){
            @Override public void insertUpdate(DocumentEvent e) { filter(); }
            @Override public void removeUpdate(DocumentEvent e) { filter(); }
            @Override public void changedUpdate(DocumentEvent e) {}
            private void filter() {
                String filter = field.getText();
                filterModel((DefaultListModel<String>)jList.getModel(), filter);
            }
        });
        return field;
    }

    private JList createJList() {
        JList list = new JList(createDefaultListModel());
        list.setVisibleRowCount(6);
        return list;
    }

    private ListModel<String> createDefaultListModel() {
        DefaultListModel<String> model = new DefaultListModel<>();
        for (Job j : Job.values()) {
            model.addElement(j.getDisplayName());
        }
        return model;
    }

    public void filterModel(DefaultListModel<String> model, String filter) {
        for (Job j : Job.values()) {
            if (!j.getDisplayName().startsWith(filter)) {
                if (model.contains(j.getDisplayName())) {
                    model.removeElement(j.getDisplayName());
                }
            } else {
                if (!model.contains(j)) {
                    model.addElement(j.getDisplayName());
                }
            }
        }
    }

    public static void main(String[] args) {
        SwingUtilities.invokeLater(new Runnable(){
            public void run() {
                new JobList();
            }
        });
    }
}