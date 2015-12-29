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
package com.pivotal.gemfirexd.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.CallableStatement;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;

import junit.framework.TestSuite;
import junit.textui.TestRunner;

import com.gemstone.gemfire.internal.AvailablePort;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.jdbc.JdbcTestBase;
import com.pivotal.gemfirexd.jdbc.SimpleAppTest;

public class DataAwareProcedureTest extends JdbcTestBase{

  private volatile boolean exceptionOccured = false;

  public static void main(String[] args) {
    TestRunner.run(new TestSuite(SimpleAppTest.class));
  }

  public DataAwareProcedureTest(String name) {
    super(name);
  }

  @Override
  protected String reduceLogging() {
    return "config";
  }

  private void populateTable(Connection conn) throws Exception {
    String INSERTER = "insert into hourly (name, date, value) values (?, ?, ?)";
    int BATCHSIZE = 1000;
    int RECORDS = 10000;
    Calendar cal = Calendar.getInstance();
    Timestamp currentDate = new Timestamp(cal.getTimeInMillis());
    String name = "randomName";
    int i = 0;
    int j = 0;
    int t = 0;
    
    PreparedStatement preppedStmt = conn.prepareStatement(INSERTER);
    while (i < (RECORDS / BATCHSIZE)) {
      while (j < BATCHSIZE) {
        // If daily rollups don't equal 11.5 we've lost data.
        int value = (t % 24);
        preppedStmt.setString(1, name);
        preppedStmt.setTimestamp(2, currentDate);
        preppedStmt.setFloat(3, value);
        preppedStmt.addBatch();
        j = j + 1;
        t = t + 1;
        cal.add(Calendar.HOUR_OF_DAY, 1);
        currentDate.setTime(cal.getTimeInMillis()); // adds one hour
      }
      preppedStmt.executeBatch();
      i = i + 1;
      j = 0;
    }
    preppedStmt.close();
  }
  
  public void testBug46553() throws Exception {
    Properties props = new Properties();
    int mcastPort = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
    props.put("mcast-port", String.valueOf(mcastPort));
    Connection conn = TestUtil.getConnection(props);
    Statement st = conn.createStatement();
    st.execute("CREATE TABLE HOURLY (" +
    		"NAME VARCHAR(20), " +
    		"DATE TIMESTAMP, " +
    		"VALUE FLOAT) " +
    		"PARTITION BY COLUMN (NAME)");
    
    st.execute("CREATE TABLE DAILY (" +
    		"NAME VARCHAR(20), " +
    		"DATE TIMESTAMP, " +
    		"VALUE FLOAT, " +
    		"MAXV FLOAT, " +
    		"MINV FLOAT) " +
    		"PARTITION BY COLUMN (NAME) COLOCATE with (HOURLY)");

    populateTable(conn);
    
    st.execute("CREATE PROCEDURE ROLLUP() " +
    		"LANGUAGE JAVA " +
    		"PARAMETER STYLE JAVA " +
    		"MODIFIES SQL DATA " +
    		"DYNAMIC RESULT SETS 1 " +
    		"EXTERNAL NAME 'com.pivotal.gemfirexd.jdbc.Rollup.Test1'");
    
    CallableStatement cs = conn.prepareCall("CALL ROLLUP() ON TABLE HOURLY");
    cs.execute();
  }
  
  public void testBug46554() throws Exception {
    Properties props = new Properties();
    int mcastPort = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
    props.put("mcast-port", String.valueOf(mcastPort));
    Connection conn = TestUtil.getConnection(props);
    Statement st = conn.createStatement();
    st.execute("CREATE TABLE HOURLY (" +
                "NAME VARCHAR(20), " +
                "DATE TIMESTAMP, " +
                "VALUE FLOAT) " +
                "PARTITION BY COLUMN (NAME)");
    
    st.execute("CREATE TABLE DAILY (" +
                "NAME VARCHAR(20), " +
                "DATE TIMESTAMP, " +
                "VALUE FLOAT, " +
                "MAXV FLOAT, " +
                "MINV FLOAT) " +
                "PARTITION BY COLUMN (NAME) COLOCATE with (HOURLY)");

    populateTable(conn);
    
    st.execute("CREATE PROCEDURE ROLLUP() " +
                "LANGUAGE JAVA " +
                "PARAMETER STYLE JAVA " +
                "MODIFIES SQL DATA " +
                "DYNAMIC RESULT SETS 1 " +
                "EXTERNAL NAME 'com.pivotal.gemfirexd.jdbc.Rollup.Test2'");
    
    CallableStatement cs = conn.prepareCall("CALL ROLLUP() ON TABLE HOURLY");
    cs.execute();
  }
  
  public void testBug46556() throws Exception {
    Properties props = new Properties();
    int mcastPort = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
    props.put("mcast-port", String.valueOf(mcastPort));
    Connection conn = TestUtil.getConnection(props);
    Statement st = conn.createStatement();
    
    st.execute("CREATE TABLE HOURLY (" +
                "NAME VARCHAR(20), " +
                "DATE TIMESTAMP, " +
                "VALUE FLOAT) " +
                "PARTITION BY COLUMN (NAME)");
    
    st.execute("CREATE TABLE DAILY (" +
                "NAME VARCHAR(20), " +
                "DATE TIMESTAMP, " +
                "VALUE FLOAT, " +
                "MAXV FLOAT, " +
                "MINV FLOAT) " +
                "PARTITION BY COLUMN (NAME) COLOCATE with (HOURLY)");

    populateTable(conn);
    
    st.execute("CREATE PROCEDURE ROLLUP() " +
                "LANGUAGE JAVA " +
                "PARAMETER STYLE JAVA " +
                "MODIFIES SQL DATA " +
                "DYNAMIC RESULT SETS 1 " +
                "EXTERNAL NAME 'com.pivotal.gemfirexd.jdbc.Rollup.Test3'");
    
    CallableStatement cs = conn.prepareCall("CALL ROLLUP() ON TABLE HOURLY");
    cs.execute();
  }
  
  public void testBug49947() throws Exception {
    Properties props = new Properties();
    int mcastPort = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
    props.put("mcast-port", String.valueOf(mcastPort));
    Connection conn = TestUtil.getConnection(props);
    Statement st = conn.createStatement();

    st.execute("CREATE TABLE HOURLY (" + "NAME VARCHAR(20), "
        + "DATE TIMESTAMP, " + "VALUE FLOAT) " + "PARTITION BY COLUMN (NAME)");

    st.execute("CREATE TABLE DAILY (" + "NAME VARCHAR(20), "
        + "DATE TIMESTAMP, " + "VALUE FLOAT, " + "MAXV FLOAT, "
        + "MINV FLOAT) " + "PARTITION BY COLUMN (NAME) COLOCATE with (HOURLY)");

    st.execute("CREATE PROCEDURE ROLLUP() " + "LANGUAGE JAVA "
        + "PARAMETER STYLE JAVA " + "MODIFIES SQL DATA "
        + "DYNAMIC RESULT SETS 1 "
        + "EXTERNAL NAME 'com.pivotal.gemfirexd.jdbc.Rollup.Test3'");

    st.execute("CREATE TRIGGER mt AFTER UPDATE OF value ON daily REFERENCING NEW AS updatedrow FOR EACH ROW CALL rollup() ON TABLE hourly");

    shutDown();

    conn = TestUtil.getConnection();
  
  }
}

