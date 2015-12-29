/*
 * Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package com.pivotal.gemfirexd.tools.gfxdtop;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.GridLayout;
import java.io.IOException;
import java.net.MalformedURLException;
import java.text.NumberFormat;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutionException;

import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.swing.JComponent;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.SwingUtilities;
import javax.swing.SwingWorker;
import javax.swing.border.EmptyBorder;
import javax.swing.table.DefaultTableCellRenderer;

/**
 * GfxdTop is a JPanel to display all SQL statements activity in the Gfxd cluster
 * Deried from JTop
 * @author tushark
 */

/*-
 *  TODO : 1. Expand Statement Definition
 *         2. Column Sorting
 *         3. By Schema/Table or String filter (regex)
 *         4. Restrict count : Top x by numExecution
 *         5. Detect stats enablement
 *         6. Allow to enable/disable stats
 *         7. Allow other readOnly Operations
 */

public class GfxdTop extends JPanel {
    private MBeanServerConnection server;
    private StatementTableModel tmodel;
    private ClusterStatistics queryStats;
    
    public GfxdTop() {
        super(new GridLayout(1,0));

        tmodel = new StatementTableModel();
        queryStats = new ClusterStatistics();
        
        JTable table = new JTable(tmodel);
        table.setPreferredScrollableViewportSize(new Dimension(500, 300));

        table.setDefaultRenderer(Double.class, new DoubleRenderer());
        table.setIntercellSpacing(new Dimension(6,3));
        table.setRowHeight(table.getRowHeight() + 4);

        JScrollPane scrollPane = new JScrollPane(table);

        add(scrollPane);
    }

    public void setMBeanServerConnection(MBeanServerConnection mbs) {
        this.server = mbs;
        try {
            queryStats.setMbs(mbs);
        } finally{
          //
        }
    }

    /**
     * Format Double with 4 fraction digits
     */ 
    static class DoubleRenderer extends DefaultTableCellRenderer {
        /**
       * 
       */
      private static final long serialVersionUID = 1L;
        NumberFormat formatter;
        public DoubleRenderer() { 
            super();
            setHorizontalAlignment(JLabel.RIGHT);
        }
    
        public void setValue(Object value) {
            if (formatter==null) {
                formatter = NumberFormat.getInstance();
                formatter.setMinimumFractionDigits(4);
            }
            setText((value == null) ? "" : formatter.format(value));
        }
    }

    class Worker extends SwingWorker<Map<String, Statement>,Object> {
        private StatementTableModel tmodel;
        Worker(StatementTableModel tmodel) {
            this.tmodel = tmodel;
        }

        public Map<String, Statement> doInBackground() {
            return queryStats.getGfxdStatements();
        }
                                                                                
        protected void done() {
            try {
              Map<String, Statement> statementMap = get();
                tmodel.setStatementList(statementMap.values());
                tmodel.fireTableDataChanged();
            } catch (InterruptedException e) {
            } catch (ExecutionException e) {
            }
        }
    }

    public SwingWorker<?,?> newSwingWorker() {
        return new Worker(tmodel);
    }

  public static void main(String[] args) throws Exception {
    
    if (args.length != 1) {
      usage();
    }

    String[] arg2 = args[0].split(":");
    if (arg2.length != 2) {
      usage();
    }
    String hostname = arg2[0];
    int port = -1;
    try {
      port = Integer.parseInt(arg2[1]);
    } catch (NumberFormatException x) {
      usage();
    }
    if (port < 0) {
      usage();
    }

    
    final GfxdTop gfxdTop = new GfxdTop();
    
    MBeanServerConnection server = connect(hostname, port);
    gfxdTop.setMBeanServerConnection(server);

    
    TimerTask timerTask = new TimerTask() {
      public void run() {
        gfxdTop.newSwingWorker().execute();
      }
    };

    SwingUtilities.invokeAndWait(new Runnable() {
      public void run() {
        createAndShowGUI(gfxdTop);
      }
    });

    Timer timer = new Timer("GfxdTop Sampling thread");
    timer.schedule(timerTask, 0, 2000);

  }

    private static MBeanServerConnection connect(String hostname, int port) {
        String urlPath = "/jndi/rmi://" + hostname + ":" + port + "/jmxrmi";
        MBeanServerConnection server = null;        
        try {
            JMXServiceURL url = new JMXServiceURL("rmi", "", 0, urlPath);
            JMXConnector jmxc = JMXConnectorFactory.connect(url);
            server = jmxc.getMBeanServerConnection();
        } catch (MalformedURLException e) {
            // should not reach here
        } catch (IOException e) {
            System.err.println("\nCommunication error: " + e.getMessage());
            System.exit(1);
        }
        return server;
    }

    private static void usage() {
        System.out.println("Usage: java GfxdTop <hostname>:<port>");
        System.exit(1);
    }
    
    private static void createAndShowGUI(JPanel jtop) {
        JFrame frame = new JFrame("GfxdTop");
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

        JComponent contentPane = (JComponent) frame.getContentPane();
        contentPane.add(jtop, BorderLayout.CENTER);
        contentPane.setOpaque(true);
        contentPane.setBorder(new EmptyBorder(12, 12, 12, 12));
        frame.setContentPane(contentPane);

        frame.pack();
        frame.setVisible(true);
    }

}
