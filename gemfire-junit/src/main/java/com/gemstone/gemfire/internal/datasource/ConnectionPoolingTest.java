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
package com.gemstone.gemfire.internal.datasource;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import javax.sql.DataSource;
import javax.transaction.UserTransaction;

import junit.framework.*;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.distributed.DistributedSystem;

import io.snappydata.test.dunit.DistributedTestBase;

import javax.naming.Context;
import javax.naming.NamingException;

/**
 * @author Mitul
 * 
 *  
 */
public class ConnectionPoolingTest extends TestCase {

  private Thread ThreadA;
  protected Thread ThreadB;
//  private Thread ThreadC;
  protected static int maxPoolSize = 7;
  protected static DataSource ds = null;
  private static Properties props = null;
  private static DistributedSystem ds1 = null;
  protected static Cache cache = null;
  protected boolean encounteredException = false;
  static {
    try {
      props = new Properties();
//      props.setProperty("mcast-port", "33405");
      String path = System.getProperty("JTAXMLFILE");
      props.setProperty("cache-xml-file", path);
      ds1 = DistributedSystem.connect(props);
      cache = CacheFactory.create(ds1);
    }
    catch (Exception e) {
      fail("Exception occured in creation of ds and cache due to " + e);
      e.printStackTrace();
    }
  }

  public void testGetSimpleDataSource() throws Exception {
    try {
      Context ctx = cache.getJNDIContext();
      GemFireBasicDataSource ds = (GemFireBasicDataSource) ctx.lookup("java:/SimpleDataSource");
      Connection conn = ds.getConnection();
      if (conn == null)
          fail("DataSourceFactoryTest-testGetSimpleDataSource() Error in creating the GemFireBasicDataSource");
    }
    catch (Exception e) {
      fail("Exception occured in testGetSimpleDataSource due to " + e);
      e.printStackTrace();
    }
  }

  public ConnectionPoolingTest() {
  }

  public ConnectionPoolingTest(java.lang.String testName) {
    super(testName);
  }

  public void setup() {    
    encounteredException = false;
  }

  public void teardown() {
  }

  /*
   * public static void main(String str[]) { ConnectionPoolingTest test = new
   * ConnectionPoolingTest(); test.setup(); try {
   * test.testGetSimpleDataSource(); } catch (Exception e) { // TODO
   * Auto-generated catch block e.printStackTrace(); fail(); }
   * test.testConnectionPoolFunctions(); test.teardown();
   *  
   */
  public void testConnectionPoolFunctions() {
    try {
      Context ctx = cache.getJNDIContext();
      ds = (GemFireConnPooledDataSource) ctx.lookup("java:/PooledDataSource");
      PoolClient_1 clientA = new PoolClient_1();
      ThreadA = new Thread(clientA, "ThreadA");
      PoolClient_2 clientB = new PoolClient_2();
      ThreadB = new Thread(clientB, "ThreadB");
      // ThreadA.setDaemon(true);
      //ThreadB.setDaemon(true);
      ThreadA.start();
    }
    catch (Exception e) {
      fail("Exception occured in testConnectionPoolFunctions due to " + e);
      e.printStackTrace();
    }
  }

  public void testXAPoolLeakage()
  {
    final String tableName = "testXAPoolLeakage";
    //initialize cache and get user transaction
    try {
      int numThreads = 10;
      final int LOOP_COUNT = 10;

      //System.out.println ("Table name: " + tableName);
      Context ctx = cache.getJNDIContext();
      DataSource ds = (DataSource)ctx.lookup("java:/SimpleDataSource");

      //String sql = "create table " + tableName + " (id number primary key,
      // name varchar2(50))";
      //String sql = "create table " + tableName + " (id integer primary key,
      // name varchar(50))";
      String sql = "create table "
          + tableName
          + " (id varchar(50) NOT NULL, name varchar(50), CONSTRAINT the_key PRIMARY KEY(id))";
      System.out.println(sql);
      Connection conn = ds.getConnection();
      Statement sm = conn.createStatement();
      sm.execute(sql);
      sm.close();
      conn.close();
      Thread th[] = new Thread[numThreads];
      for (int i = 0; i < numThreads; ++i) {
        final int threadID = i;
        th[i] = new Thread(new Runnable() {
          private int key = threadID;

          public void run()
          {
            try {
              Context ctx = cache.getJNDIContext();
              //          Operation with first XA Resource
              DataSource da1 = (DataSource)ctx
                  .lookup("java:/XAMultiThreadedDataSource");
               int val=0;
              for (int j = 0; j < LOOP_COUNT; ++j) {
                UserTransaction ta = null;
                try {
                  ta = (UserTransaction)ctx.lookup("java:/UserTransaction");

                }
                catch (NamingException e) {
                  encounteredException = true;
                  break;
                }

                try {
                  //Begin the user transaction
                  ta.begin();
                  for (int i = 1; i <= 50; i++) {
                    Connection conn = da1.getConnection();
                    Statement sm = conn.createStatement();

                    String sql = "insert into " + tableName + " values (" + "'"
                        + key + "X" + ++val + "','name" + i + "')";                    
                    sm.execute(sql);
                 
                    sm.close();
                    conn.close();
                  }
                  if(j%2 == 0) {
                    ta.commit();
                    System.out.println("Committed successfully for thread with id ="+key);
                  }else{
                    ta.rollback();
                    System.out.println("Rolled back successfully for thread with id ="+key);
                  }
                }
                catch (Exception e) {
                  e.printStackTrace();
                  encounteredException = true;
                  break;
                }

              }
            }
            catch (Exception e) {
              e.printStackTrace();
              encounteredException = true;
            }
          }
        });
      }

      for (int i = 0; i < th.length; ++i) {
        th[i].start();
      }

      for (int i = 0; i < th.length; ++i) {
        DistributedTestBase.join(th[i], 90 * 1000, null);
      }
      assertFalse(encounteredException);

    }
    catch (Exception e) {
      e.printStackTrace();      
      fail("Test failed bcoz ofexception =" + e);
    }finally{
      System.out.println("Destroying table: " + tableName);
      try {
        Context ctx = cache.getJNDIContext();
        DataSource ds = (DataSource)ctx.lookup("java:/SimpleDataSource");
        Connection conn = ds.getConnection();
        System.out.println (" trying to drop table: " + tableName);
        String sql = "drop table " + tableName;
        Statement sm = conn.createStatement();
        sm.execute(sql);
        conn.close();
        System.out.println (" Dropped table: " + tableName);
      }
      catch (Exception e1) {
        e1.printStackTrace();
        fail(e1.toString());
      }
      
    }

  }

  class PoolClient_1 implements Runnable
  {

    public void run() {
      String threadName = Thread.currentThread().getName();
      System.out.println(" Inside Run method of " + threadName);
      int numConn = 0;
      Object[] connections = new Object[maxPoolSize];
//      Statement stmt = null;
      while (numConn < maxPoolSize) {
        try {
          System.out.println(" Getting a connection from " + threadName);
          //	ds.getConnection();
          connections[numConn] = ds.getConnection();
          //   Thread.sleep(500);
          numConn++;
          //if (numConn == 5)
          //ThreadB.start();
          System.out.println(" Got connection " + numConn + "from " + threadName);
        }
        catch (Exception ex) {
          ex.printStackTrace();
        }
      }
      if (numConn != maxPoolSize) fail("#### Error in filling the the connection pool from " + threadName);
      ThreadB.start();
      System.out.println(" AFTER starting THREADB");
      int numC = 0;
      int display = 0;
//      long birthTime = 0;
//      long newTime = 0;
//      long duration = 0;
      Connection conn;
      try {
        Thread.sleep(500);
      }
      catch (InterruptedException e) {
        e.printStackTrace();
        fail("interrupted");
      }
      for (numC = 0; numC < numConn; numC++) {
        try {
          display = numC + 1;
          System.out.println(" Returning connection " + display + "from " + threadName);
          conn = (Connection) connections[numC];
          System.out.println(" ************************************" + conn);
          System.out.println(" !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! The connection is of type "
              + conn.getClass());
          //   goahead = false;
          // conn.close();
          System.out.println(" Returned connection " + display + "from " + threadName);
        }
        catch (Exception ex) {
          fail("Exception occured in trying to returnPooledConnectiontoPool due to " + ex);
          ex.printStackTrace();
        }
      }
      if (numC != maxPoolSize) fail("#### Error in returning all the connections to the  pool from " + threadName);
      System.out.println(" ****************Returned all connections " + threadName + "***********");
    }
  }

  class PoolClient_2 implements Runnable {

    List poolConnlist = new ArrayList();

    public void run() {
      try {
        String threadName = Thread.currentThread().getName();
        System.out.println(" Inside Run method of " + threadName);
        int numConn2 = 0;
//        int display = 0;
        Object[] connections = new Object[maxPoolSize];
        while (numConn2 < maxPoolSize) {
          try {
            System.out.println(" _______________________________________________________________ " + numConn2);
            numConn2++;
            System.out.println(" ********** Before getting " + numConn2 + "from" + threadName);
            connections[numConn2 - 1] = ds.getConnection();
            System.out.println(" ********** Got connection " + numConn2 + "from" + threadName);
          }
          catch (Exception ex) {
            ex.printStackTrace();
          }
        }
        numConn2 = 0;
        while (numConn2 < maxPoolSize) {
          try {
            ((Connection) connections[numConn2]).close();
            numConn2++;
          }
          catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
        }
        if (numConn2 != maxPoolSize) fail("#### Error in getting all connections from the " + threadName);
        System.out.println(" ****************GOT ALL connections " + threadName + "***********");
      }
      catch (Exception e1) {
        e1.printStackTrace();
        fail();
      }
    }
  }
}
