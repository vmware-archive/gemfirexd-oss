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
package com.gemstone.gemfire.internal.jta;

import java.sql.Connection;
import java.sql.ResultSet;
//import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.datasource.GemFireTransactionDataSource;
import com.gemstone.gemfire.internal.jta.UserTransactionImpl;
import javax.naming.Context;
import javax.sql.DataSource;
import javax.transaction.*;

import com.gemstone.gemfire.internal.shared.NativeCalls;
import junit.framework.TestCase;
import java.io.*;

public class TransactionTimeOutTest extends TestCase {

  private static Properties props = null;
  private static DistributedSystem ds1 = null;
  private static Cache cache = null;
  static {
    try {
      props = new Properties();
//      props.setProperty("mcast-port", "33405");
      int pid = NativeCalls.getInstance().getProcessId();
      String path = System.getProperty("JTESTS") + "/lib/dunit-cachejta_" + pid + ".xml";
      String file_as_str = readFile(System.getProperty("JTAXMLFILE"));
      String modified_file_str= file_as_str.replaceAll("newDB", "newDB_" + pid);
      FileOutputStream fos = new FileOutputStream(path);
      BufferedWriter wr = new BufferedWriter(new OutputStreamWriter(fos));
      wr.write(modified_file_str);
      wr.flush();
      wr.close();
      props.setProperty("cache-xml-file", path);
      // props.setProperty("cache-xml-file","D:\\projects\\JTA\\cachejta.xml");
      ds1 = DistributedSystem.connect(props);
      cache = CacheFactory.create(ds1);
    } catch (Exception e) {
      fail("Exception occured in creation of ds and cache due to " + e);
      e.printStackTrace();
    }
  }

  protected void setUp() throws Exception {
    super.setUp();
  }

  protected void tearDown() throws Exception {
    super.tearDown();
  }

  public TransactionTimeOutTest(String arg0) {
    super(arg0);
  }

  public void testOne() {
    try {
      UserTransaction utx = new UserTransactionImpl();
      utx.begin();
      Thread.sleep(2000);
      utx.setTransactionTimeout(2);
      utx.setTransactionTimeout(200);
      utx.setTransactionTimeout(3);
      Thread.sleep(5000);
      //utx.commit();
    } catch (Exception e) {
      fail("Exception in TestSetTransactionTimeOut due to " + e);
    }
  }

  public void testSetTransactionTimeOut() {
    try {
      UserTransaction utx = new UserTransactionImpl();
      utx.begin();
      utx.setTransactionTimeout(2);
      System.out.println("Going to sleep");
      Thread.sleep(6000);
      utx.begin();
      utx.commit();
    } catch (Exception e) {
      fail("Exception in TestSetTransactionTimeOut due to " + e);
    }
  }

  public void testExceptionOnCommitAfterTimeOut() {
    UserTransaction utx;
    boolean exceptionOccured = true;
    try {
      utx = new UserTransactionImpl();
      utx.setTransactionTimeout(2);
      utx.begin();
      Thread.sleep(4000);
      try {
        utx.commit();
      } catch (Exception e) {
        exceptionOccured = false;
      }
      if (exceptionOccured) {
        fail("TimeOut did not rollback the transaction");
      }
    } catch (Exception e) {
      fail("Exception in testExceptionOnCommitAfterTimeOut() due to " + e);
    }
  }

  public void testMultipleSetTimeOuts() {
    UserTransaction utx;
    boolean exceptionOccured = true;
    try {
      utx = new UserTransactionImpl();
      utx.setTransactionTimeout(10);
      utx.begin();
      utx.setTransactionTimeout(8);
      utx.setTransactionTimeout(6);
      utx.setTransactionTimeout(2);
      Thread.sleep(6000);
      try {
        utx.commit();
      } catch (Exception e) {
        exceptionOccured = false;
      }
      if (exceptionOccured) {
        fail("TimeOut did not rollback the transaction");
      }
    } catch (Exception e) {
      fail("Exception in testExceptionOnCommitAfterTimeOut() due to " + e);
    }
  }

  public void testTimeOutBeforeBegin() {
    boolean exceptionOccured = true;
    try {
      UserTransaction utx = new UserTransactionImpl();
      utx.setTransactionTimeout(4);
      utx.begin();
      Thread.sleep(6000);
      try {
        utx.commit();
      } catch (Exception e) {
        exceptionOccured = false;
      }
      if (exceptionOccured) {
        fail("TimeOut did not rollback the transaction");
      }
    } catch (Exception e) {
      fail("Exception in testExceptionOnCommitAfterTimeOut() due to " + e);
    }
  }

  public void testCommitBeforeTimeOut() {
//    boolean exceptionOccured = true;
    try {
      UserTransaction utx = new UserTransactionImpl();
      utx.begin();
      utx.setTransactionTimeout(6);
      Thread.sleep(2000);
      try {
        utx.commit();
      } catch (Exception e) {
        fail("Transaction failed to commit although TimeOut was not exceeded due to "
            + e);
      }
    } catch (Exception e) {
      fail("Exception in testExceptionOnCommitAfterTimeOut() due to " + e);
    }
  }

  public void _testCommit() {
    try {
      Context ctx = cache.getJNDIContext();
      DataSource ds2 = (DataSource) ctx.lookup("java:/SimpleDataSource");
      ds2.getConnection();
      GemFireTransactionDataSource ds = (GemFireTransactionDataSource) ctx
          .lookup("java:/XAPooledDataSource");
      UserTransaction utx = (UserTransaction) ctx
          .lookup("java:/UserTransaction");
      utx.begin();
      Connection conn = ds.getConnection();
      String sql = "create table newTable1 (id integer)";
      Statement sm = conn.createStatement();
      sm.execute(sql);
      utx.setTransactionTimeout(30);
      Thread.sleep(5000);
      utx.setTransactionTimeout(20);
      utx.setTransactionTimeout(10);
      sql = "insert into newTable1  values (1)";
      sm.execute(sql);
      utx.commit();
      sql = "select * from newTable1 where id = 1";
      ResultSet rs = sm.executeQuery(sql);
      if (!rs.next()) fail("Transaction not committed");
      sql = "drop table newTable1";
      sm.execute(sql);
      sm.close();
      conn.close();
    } catch (Exception e) {
      fail("Exception occured in test Commit due to " + e);
      e.printStackTrace();
    }
  }

  public void _testCommitAfterTimeOut() {
    try {
      boolean exceptionOccured = false;
      Context ctx = cache.getJNDIContext();
      DataSource ds2 = (DataSource) ctx.lookup("java:/SimpleDataSource");
      ds2.getConnection();
      GemFireTransactionDataSource ds = (GemFireTransactionDataSource) ctx
          .lookup("java:/XAPooledDataSource");
      UserTransaction utx = (UserTransaction) ctx
          .lookup("java:/UserTransaction");
      utx.begin();
      Connection conn = ds.getConnection();
      String sql = "create table newTable2 (id integer)";
      Statement sm = conn.createStatement();
      sm.execute(sql);
      utx.setTransactionTimeout(30);
      sql = "insert into newTable2  values (1)";
      sm.execute(sql);
      sql = "select * from newTable2 where id = 1";
      ResultSet rs = sm.executeQuery(sql);
      if (!rs.next()) fail("Transaction not committed");
      sql = "drop table newTable2";
      sm.execute(sql);
      sm.close();
      conn.close();
      utx.setTransactionTimeout(1);
      Thread.sleep(3000);
      try {
        utx.commit();
      } catch (Exception e) {
        exceptionOccured = true;
      }
      if (!exceptionOccured) {
        fail("exception did not occur on commit although transaction timed out");
      }
    } catch (Exception e) {
      fail("Exception occured in test Commit due to " + e);
      e.printStackTrace();
    }
  }

  public void testRollbackAfterTimeOut() {
    try {
      boolean exceptionOccured = false;
      Context ctx = cache.getJNDIContext();
      DataSource ds2 = (DataSource) ctx.lookup("java:/SimpleDataSource");
      ds2.getConnection();
      GemFireTransactionDataSource ds = (GemFireTransactionDataSource) ctx
          .lookup("java:/XAPooledDataSource");
      UserTransaction utx = (UserTransaction) ctx
          .lookup("java:/UserTransaction");
      utx.begin();
      Connection conn = ds.getConnection();
      String sql = "create table newTable3 (id integer)";
      Statement sm = conn.createStatement();
      sm.execute(sql);
      utx.setTransactionTimeout(30);
      sql = "insert into newTable3  values (1)";
      sm.execute(sql);
      sql = "select * from newTable3 where id = 1";
      ResultSet rs = sm.executeQuery(sql);
      if (!rs.next()) fail("Transaction not committed");
      sql = "drop table newTable3";
      sm.execute(sql);
      sm.close();
      conn.close();
      utx.setTransactionTimeout(1);
      Thread.sleep(3000);
      try {
        utx.rollback();
      } catch (Exception e) {
        exceptionOccured = true;
      }
      if (!exceptionOccured) {
        fail("exception did not occur on rollback although transaction timed out");
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail("Exception occured in test Commit due to " + e);
    }
  }

  private static String readFile(String filename) throws IOException {
    BufferedReader br = new BufferedReader(new FileReader(filename));
    String nextLine = "";
    StringBuffer sb = new StringBuffer();
    while ((nextLine = br.readLine()) != null) {
      sb.append(nextLine);
      //
      // note:
      //   BufferedReader strips the EOL character.
      //
      //    sb.append(lineSep);
    }
    return sb.toString();
  }

}
