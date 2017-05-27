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
package com.pivotal.gemfirexd.internal.engine.hadoop.mapreduce;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

import com.gemstone.gemfire.internal.AvailablePort;
import com.pivotal.gemfirexd.FabricServer;
import com.pivotal.gemfirexd.FabricServiceManager;
import com.pivotal.gemfirexd.hadoop.mapreduce.Key;
import com.pivotal.gemfirexd.hadoop.mapreduce.RowOutputFormat;
import com.pivotal.gemfirexd.jdbc.JdbcTestBase;

public class GfxdOutputFormatTest extends JdbcTestBase {
  /**
   * Tests whether writing data through output format creates data in gemfirexd
   * instance
   */
  public void testMR2OutputWriter() throws Exception {
    FabricServer server = FabricServiceManager.getFabricServerInstance();
    Properties props = new Properties();
    int mcastPort = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
    props.setProperty("mcast-port", String.valueOf(mcastPort));
    server.start(props);
    int clientPort = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
    server.startNetworkServer("localhost", clientPort, props);
    
    Connection conn;
    Statement st;
    conn = DriverManager.getConnection("jdbc:gemfirexd://localhost:" + clientPort + "/");
    st = conn.createStatement();
    st.execute("create schema emp");
    st.execute("set schema emp");
    st.execute("create table emp.usrtable (col1 int primary key, col2 varchar(100))");
    
    class DataObject {
      int col1;
      String col2;
      public DataObject(int col1, String col2) {
        this.col1 = col1;
        this.col2 = col2;
      }
      
      public void setCol1(int index, PreparedStatement ps) throws SQLException {
        ps.setInt(index, col1);
      }

      public void setCol2(int i, PreparedStatement ps) throws SQLException {
        ps.setString(i, col2);
      }
      
      public String toString() {
        return col1 + "-" + col2;
      }
    }

    Configuration conf = new Configuration();
    conf.set(RowOutputFormat.OUTPUT_TABLE, "emp.usrtable");
    conf.set(RowOutputFormat.OUTPUT_URL, "jdbc:gemfirexd://localhost:" + clientPort + "/");

    RowOutputFormat<DataObject> format = new RowOutputFormat<DataObject>();
    TaskAttemptContextImpl task = new TaskAttemptContextImpl(conf, new TaskAttemptID());
    format.checkOutputSpecs(task);
    RecordWriter<Key, DataObject> writer = format.getRecordWriter(task);
    writer.write(new Key(), new DataObject(1, "1"));
    writer.write(new Key(), new DataObject(2, "2"));
    writer.close(task);

    ResultSet rs =  st.executeQuery("select * from emp.usrtable");
    ArrayList<String> rows = new ArrayList<String>();
    while(rs.next()) {
      rows.add(rs.getInt(1) + "-" + rs.getString(2));
    }
    
    assertEquals(2, rows.size());
    assertTrue(rows.contains("1-1"));
    assertTrue(rows.contains("2-2"));
    
    st.close();
    conn.close();
    server.stop(props);
  }

  /**
   * Tests whether writing data through output format creates data in gemfirexd
   * instance
   */
  public void testMR1OutputWriter() throws Exception {
    FabricServer server = FabricServiceManager.getFabricServerInstance();
    Properties props = new Properties();
    int mcastPort = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
    props.setProperty("mcast-port", String.valueOf(mcastPort));
    server.start(props);
    int clientPort = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
    server.startNetworkServer("localhost", clientPort, props);
    
    Connection conn;
    Statement st;
    conn = DriverManager.getConnection("jdbc:gemfirexd://localhost:" + clientPort + "/");
    st = conn.createStatement();
    st.execute("create schema emp");
    st.execute("set schema emp");
    st.execute("create table emp.usrtable (col1 int, col2 varchar(100))");
    
    class DataObject {
      int col1;
      String col2;
      public DataObject(int col1, String col2) {
        this.col1 = col1;
        this.col2 = col2;
      }
      
      public void setCol1(int index, PreparedStatement ps) throws SQLException {
        ps.setInt(index, col1);
      }

      public void setCol2(int i, PreparedStatement ps) throws SQLException {
        ps.setString(i, col2);
      }
      
      public String toString() {
        return col1 + "-" + col2;
      }
    }

    com.pivotal.gemfirexd.hadoop.mapred.RowOutputFormat<DataObject> mr1Instance; 
    mr1Instance = new com.pivotal.gemfirexd.hadoop.mapred.RowOutputFormat<DataObject>();
    
    JobConf conf = new JobConf();    
    FileSystem fs = FileSystem.get(conf);
    
    conf.set(mr1Instance.OUTPUT_TABLE, "emp.usrtable");
    conf.set(mr1Instance.OUTPUT_URL, "jdbc:gemfirexd://localhost:" + clientPort + "/");
    
    mr1Instance.checkOutputSpecs(FileSystem.get(conf), conf);
    org.apache.hadoop.mapred.RecordWriter<Key, DataObject> writer; 
    writer = mr1Instance.getRecordWriter(fs, conf, "name", null);
    writer.write(new Key(), new DataObject(1, "1"));
    writer.write(new Key(), new DataObject(2, "2"));
    writer.close(null);

    ResultSet rs =  st.executeQuery("select * from emp.usrtable");
    ArrayList<String> rows = new ArrayList<String>();
    while(rs.next()) {
      rows.add(rs.getInt(1) + "-" + rs.getString(2));
    }
    
    assertEquals(2, rows.size());
    assertTrue(rows.contains("1-1"));
    assertTrue(rows.contains("2-2"));
    
    st.close();
    conn.close();
    server.stop(props);
  }
  
  /*
   * Test for batched execution
   */
  public void testMR1BatchWriter() throws Exception {
    FabricServer server = FabricServiceManager.getFabricServerInstance();
    Properties props = new Properties();
    int mcastPort = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
    props.setProperty("mcast-port", String.valueOf(mcastPort));
    server.start(props);
    int clientPort = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
    server.startNetworkServer("localhost", clientPort, props);
    
    Connection conn;
    Statement st;
    conn = DriverManager.getConnection("jdbc:gemfirexd://localhost:" + clientPort + "/");
    st = conn.createStatement();
    st.execute("create schema emp");
    st.execute("set schema emp");
    st.execute("create table emp.usrtable (col1 int, col2 varchar(100))");
    
    class DataObject {
      int col1;
      String col2;
      public DataObject(int col1, String col2) {
        this.col1 = col1;
        this.col2 = col2;
      }
      
      public void setCol1(int index, PreparedStatement ps) throws SQLException {
        ps.setInt(index, col1);
      }
      
      public void setCol2(int i, PreparedStatement ps) throws SQLException {
        ps.setString(i, col2);
      }
      
      public String toString() {
        return col1 + "-" + col2;
      }
    }
    
    com.pivotal.gemfirexd.hadoop.mapred.RowOutputFormat<DataObject> mr1Instance; 
    mr1Instance = new com.pivotal.gemfirexd.hadoop.mapred.RowOutputFormat<DataObject>();
    
    JobConf conf = new JobConf();    
    FileSystem fs = FileSystem.get(conf);
    
    conf.set(mr1Instance.OUTPUT_TABLE, "emp.usrtable");
    conf.set(mr1Instance.OUTPUT_URL, "jdbc:gemfirexd://localhost:" + clientPort + "/");
        
    mr1Instance.checkOutputSpecs(FileSystem.get(conf), conf);
    org.apache.hadoop.mapred.RecordWriter<Key, DataObject> writer; 
    writer = mr1Instance.getRecordWriter(fs, conf, "name", null);
    for(int i = 1; i <= 20005; i++) {
      writer.write(new Key(), new DataObject(i, "" + i));
      if ( i % 10000 == 0) {
        assertEquals(10000, OutputFormatUtil.resultCountTest);
      } else {
        assertEquals(0, OutputFormatUtil.resultCountTest);
      }
    }
    writer.close(null);
    assertEquals(5, OutputFormatUtil.resultCountTest);
    
    ResultSet rs =  st.executeQuery("select * from emp.usrtable");
    ArrayList<String> rows = new ArrayList<String>();
    while(rs.next()) {
      rows.add(rs.getInt(1) + "-" + rs.getString(2));
    }
    assertEquals(20005, rows.size());
    
    conf.set(mr1Instance.OUTPUT_BATCH_SIZE, "10");    
    writer = mr1Instance.getRecordWriter(fs, conf, "name", null);
    for(int i = 50001; i <= 50025; i++) {
      writer.write(new Key(), new DataObject(i, "" + i));
      if ( i % 10 == 0) {
        assertEquals(10, OutputFormatUtil.resultCountTest);
      } else {
        assertEquals(0, OutputFormatUtil.resultCountTest);
      }
    }
    writer.close(null);
    assertEquals(5, OutputFormatUtil.resultCountTest);
    
    rs =  st.executeQuery("select * from emp.usrtable");
    rows = new ArrayList<String>();
    while(rs.next()) {
      rows.add(rs.getInt(1) + "-" + rs.getString(2));
    }
    assertEquals(20030, rows.size());
    
    st.close();
    conn.close();
    server.stop(props);
  }

  
  /**
   * Tests user's class parsing and query creation
   */
  public void testCreateQueryFromObj() throws Exception {
    class DataObject {
      public void setCol1(int index, PreparedStatement ps) {
      }

      public void setA(int i, PreparedStatement ps) {
      }

      // none of the following
      private void setPrivate(int i, PreparedStatement ps) {
      }

      private void SetCol2(int i, PreparedStatement ps) {
      }

      public void set(int i, PreparedStatement ps) {
      }

      public void no(int i, PreparedStatement ps) {
      }

      public void setNotThis1(PreparedStatement ps, int i) {
      }

      public void setNotThis1(int i, PreparedStatement ps, Object o) {
      }
    }

    Configuration conf = new Configuration();
    conf.set(RowOutputFormat.OUTPUT_TABLE, "table");

    DataObject obj = new DataObject();
    OutputFormatUtil util = new OutputFormatUtil();
    List<Method> columns = util.spotTableColumnSetters(obj);;

    assertEquals(2, columns.size());
    assertTrue(columns.contains(DataObject.class.getMethod("setCol1",
        int.class, PreparedStatement.class)));
    assertTrue(columns.contains(DataObject.class.getMethod("setA", int.class,
        PreparedStatement.class)));

    String query = util.createQuery("table", columns);
    // the order of columns seems to be non-deterministic in different runs
    if (!query.equals("PUT INTO table(col1, a) VALUES (?, ?);") &&
        !query.equals("PUT INTO table(a, col1) VALUES (?, ?);")) {
      fail("unexpected query string: " + query);
    }
  }

  public GfxdOutputFormatTest(String name) {
    super(name);
  }
}
