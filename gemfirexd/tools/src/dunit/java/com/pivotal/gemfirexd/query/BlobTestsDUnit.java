/*
 
 Derby - Class org.apache.derbyTesting.functionTests.tests.jdbc4.BlobSetMethodsTest
 
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at
 
 http://www.apache.org/licenses/LICENSE-2.0
 
 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 
 */

/*
 * Changes for GemFireXD distributed data platform.
 *
 * Portions Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
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
package com.pivotal.gemfirexd.query;

import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import com.pivotal.gemfirexd.DistributedSQLTestBase;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.jdbc.BlobTest;

/**
 * tests set methods of blob and other blob dunit tests
 */
@SuppressWarnings("serial")
public class BlobTestsDUnit extends DistributedSQLTestBase {

  private static int BUFFER_SIZE = 1024;

  private static int UPDATE_SIZE = 100;

  public BlobTestsDUnit(String name) {
    super(name);
  }

  @Override
  protected String reduceLogging() {
    // these tests generate lots of logs, so reducing them
    return "config";
  }

  /**
   * Tests large blob (more than 4k) to ensure LOBStreamControl uses file.
   */
  public void testSetBytesLargeBlob() throws Exception {
    startVMs(1, 4);

    Connection con = TestUtil.getConnection();
    Statement stmt = con.createStatement();
    stmt.execute("create table blobtest (id integer, data Blob)");
    stmt.close();

    con.setAutoCommit(false);
    PreparedStatement pstmt = con.prepareStatement("insert into "
        + "blobtest (id, data) values (?,?)");
    Blob blob = con.createBlob();
    byte[] data = new byte[BUFFER_SIZE];
    for (int i = 0; i < BUFFER_SIZE; i++) {
      data[i] = (byte)(i % 255);
    }
    // now add more than 4k so file get in use
    for (int i = 0; i < 5; i++)
      blob.setBytes(i * BUFFER_SIZE + 1, data);
    assertEquals(BUFFER_SIZE * 5, blob.length());
    // update blob in the middle
    byte[] data1 = new byte[UPDATE_SIZE];
    for (int i = 0; i < UPDATE_SIZE; i++)
      data1[i] = 120;// just any value
    blob.setBytes(BUFFER_SIZE + 1, data1);
    blob.setBytes(BUFFER_SIZE * 5 + 1, data1);
    assertEquals(5 * BUFFER_SIZE + UPDATE_SIZE, blob.length());
    // insert it into table
    pstmt.setInt(1, 3);
    pstmt.setBlob(2, blob);
    pstmt.executeUpdate();

    stmt = con.createStatement();
    ResultSet rs = stmt.executeQuery("select data from blobtest where "
        + "id = 3");
    assertEquals(true, rs.next());
    blob = rs.getBlob(1);
    byte[] data2 = blob.getBytes(BUFFER_SIZE + 1, UPDATE_SIZE);
    assertEquals(5 * BUFFER_SIZE + UPDATE_SIZE, blob.length());
    for (int i = 0; i < UPDATE_SIZE; i++)
      assertEquals(data1[i], data2[i]);
    data2 = blob.getBytes(5 * BUFFER_SIZE + 1, UPDATE_SIZE);
    for (int i = 0; i < UPDATE_SIZE; i++)
      assertEquals(data1[i], data2[i]);
    // test truncate
    blob.truncate(BUFFER_SIZE);
    assertEquals("truncate failed", BUFFER_SIZE, blob.length());
    rs.close();
    con.commit();
    stmt.close();
    pstmt.close();
  }

  /**
   * tests set bytes method of blob in memory only mode (less than 4k)
   */
  public void testSetBytesSmallBlob() throws Exception {
    startVMs(1, 4);

    Connection con = TestUtil.getConnection();
    Statement stmt = con.createStatement();
    stmt.execute("create table blobtest (id integer, data Blob)");
    stmt.close();

    con.setAutoCommit(false);
    PreparedStatement pstmt = con.prepareStatement("insert into "
        + "blobtest (id, data) values (?,?)");
    pstmt.setInt(1, 1);
    Blob blob = con.createBlob();
    // add 1024 bytes
    byte[] data = new byte[BUFFER_SIZE];
    for (int i = 0; i < BUFFER_SIZE; i++) {
      data[i] = (byte)(i % 255);
    }
    blob.setBytes(1, data);
    assertEquals(BUFFER_SIZE, blob.length());
    pstmt.setBlob(2, blob);
    pstmt.executeUpdate();

    stmt = con.createStatement();
    ResultSet rs = stmt.executeQuery("select data from blobtest where id = 1");
    assertEquals(true, rs.next());
    blob = rs.getBlob(1);
    assertEquals(BUFFER_SIZE, blob.length());
    // update blob in the middle
    byte[] data1 = new byte[UPDATE_SIZE];
    for (int i = 0; i < UPDATE_SIZE; i++)
      data1[i] = 120;// just any value
    blob.setBytes(UPDATE_SIZE, data1);
    byte[] data2 = blob.getBytes(100, UPDATE_SIZE);
    for (int i = 0; i < UPDATE_SIZE; i++)
      assertEquals(data1[i], data2[i]);
    // update it at the end
    blob.setBytes(BUFFER_SIZE + 1, data1);
    assertEquals(BUFFER_SIZE + UPDATE_SIZE, blob.length());
    data2 = blob.getBytes(BUFFER_SIZE + 1, UPDATE_SIZE);
    for (int i = 0; i < UPDATE_SIZE; i++)
      assertEquals(data1[i], data2[i]);
    // insert the blob and test again
    pstmt.setInt(1, 2);
    pstmt.setBlob(2, blob);
    pstmt.executeUpdate();
    rs = stmt.executeQuery("select data from blobtest where id = 2");
    assertEquals(true, rs.next());
    blob = rs.getBlob(1);
    assertEquals(BUFFER_SIZE + UPDATE_SIZE, blob.length());
    data2 = blob.getBytes(100, UPDATE_SIZE);
    for (int i = 0; i < UPDATE_SIZE; i++)
      assertEquals(data1[i], data2[i]);
    data2 = blob.getBytes(BUFFER_SIZE + 1, UPDATE_SIZE);
    for (int i = 0; i < UPDATE_SIZE; i++)
      assertEquals(data1[i], data2[i]);

    // test truncate on small size blob
    blob = con.createBlob();
    data = new byte[100];
    for (int i = 0; i < 100; i++) {
      data[i] = (byte)i;
    }
    blob.setBytes(1, data);
    assertEquals(blob.length(), 100);
    blob.truncate(50);
    assertEquals(blob.length(), 50);
    blob.setBytes(1, data);
    assertEquals("set failed", blob.length(), 100);
    blob.truncate(50);
    assertEquals("truncation failed", blob.length(), 50);
    rs.close();
    con.commit();
    stmt.close();
    pstmt.close();

    stmt = con.createStatement();
    stmt.execute("drop table blobtest");
    stmt.close();
  }

  /** Test for projection on LOB/non-LOB columns */
  public void testBug43623() throws Exception {
    startVMs(2, 2);
    final int netPort = startNetworkServer(2, null, null);

    // first check with embedded connection
    Connection conn = TestUtil.getConnection();
    runTests_43623(conn);
    conn.close();

    // next with network connection
    conn = TestUtil.getNetConnection(netPort, null, null);
    conn.createStatement().execute("drop table QUARTZ_TRIGGERS");
    runTests_43623(conn);
    conn.close();
  }
  
  private void runTests_43623(final Connection conn) throws Exception {

    final String tableDDL = "create table QUARTZ_TRIGGERS (TRIGGER_NAME "
        + "varchar(80) not null, TRIGGER_GROUP varchar(80) not null, "
        + "JOB_NAME varchar(80) not null, JOB_GROUP varchar(80) not null, "
        + "IS_VOLATILE smallint, DESCRIPTION varchar(120), "
        + "NEXT_FIRE_TIME bigint, PREV_FIRE_TIME bigint,  PRIORITY integer, "
        + "TRIGGER_STATE varchar(16) not null, TRIGGER_TYPE varchar(8), "
        + "START_TIME bigint, END_TIME bigint, CALENDAR_NAME varchar(80), "
        + "MISFIRE_INSTR integer, JOB_DATA blob, "
        + "primary key(TRIGGER_NAME, TRIGGER_GROUP))";
    final Statement stmt = conn.createStatement();
    stmt.execute(tableDDL);

    // perform some inserts into the table
    final byte[] jobData = new byte[10000000];
    BlobTest.insertData_43623(conn, jobData);
    // now fire some selects (get convertible, non-get convertible)
    BlobTest.checkQueries_43623(conn, jobData);

    // now do the same for partitioned table on pk
    stmt.execute("drop table QUARTZ_TRIGGERS");
    stmt.execute(tableDDL + " partition by primary key redundancy 1");
    BlobTest.insertData_43623(conn, jobData);
    // now fire some selects (get convertible, non-get convertible)
    BlobTest.checkQueries_43623(conn, jobData);

    // now do the same for partitioned table on non-pk
    stmt.execute("drop table QUARTZ_TRIGGERS");
    stmt.execute(tableDDL + " partition by column (JOB_NAME) redundancy 1");
    BlobTest.insertData_43623(conn, jobData);
    // now fire some selects (get convertible, non-get convertible)
    BlobTest.checkQueries_43623(conn, jobData);

    // now the same as above in a transaction
    stmt.execute("drop table QUARTZ_TRIGGERS");
    stmt.execute(tableDDL);
    BlobTest.insertData_43623(conn, jobData);
    // now fire some selects (get convertible, non-get convertible)
    // checking for transactional data
    BlobTest.checkQueries_43623(conn, jobData);
    conn.commit();
    // now fire some selects (get convertible, non-get convertible)
    // after commit
    BlobTest.checkQueries_43623(conn, jobData);
    conn.commit();

    stmt.execute("drop table QUARTZ_TRIGGERS");
    stmt.execute(tableDDL + " partition by primary key redundancy 1");
    BlobTest.insertData_43623(conn, jobData);
    // now fire some selects (get convertible, non-get convertible)
    // checking for transactional data
    BlobTest.checkQueries_43623(conn, jobData);
    conn.commit();
    // now fire some selects (get convertible, non-get convertible)
    // after commit
    BlobTest.checkQueries_43623(conn, jobData);
    conn.commit();

    // now do the same for partitioned table on non-pk
    stmt.execute("drop table QUARTZ_TRIGGERS");
    stmt.execute(tableDDL + " partition by column (JOB_NAME) redundancy 1");
    BlobTest.insertData_43623(conn, jobData);
    // now fire some selects (get convertible, non-get convertible)
    // checking for transactional data
    BlobTest.checkQueries_43623(conn, jobData);
    conn.commit();
    // now fire some selects (get convertible, non-get convertible)
    // after commit
    BlobTest.checkQueries_43623(conn, jobData);
    conn.commit();
  }
}
