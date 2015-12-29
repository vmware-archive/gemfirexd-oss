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
package com.gemstone.gemfire.internal.jta.dunit;

import dunit.*;
//import java.io.*;
import java.util.*;
//import java.net.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.*;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
//import java.util.Hashtable;
//import javax.naming.InitialContext;
import javax.naming.Context;
import javax.sql.*;
import javax.transaction.*;
import java.sql.*;
//import java.lang.Exception.*;
//import java.lang.RuntimeException;
//import java.sql.SQLException.*;
import javax.naming.NamingException;
//import javax.naming.NoInitialContextException;
//import javax.transaction.SystemException;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.jta.CacheUtils;
import com.gemstone.gemfire.internal.jta.JTAUtils;
import java.io.*;

public class JTADUnitTest extends DistributedTestCase {

  public JTADUnitTest(String name) {
    super(name);
  }

  public void setUp() throws java.lang.Exception {
  }

  public void tearDown2() throws java.lang.Exception {
  }

  public DistributedSystem setProperty()
  	{
	  Properties props = new Properties();
          int pid = hydra.ProcessMgr.getProcessId();
          String path = System.getProperty("JTESTS") + "/lib/dunit-cachejta_" + pid + ".xml";
          try {
	    String file_as_str = readFile(System.getProperty("JTESTS")+"/lib/cachejta.xml");	
            String modified_file_str = file_as_str.replaceAll("newDB", "newDB_" + pid);
            FileOutputStream fos = new FileOutputStream(path);
            BufferedWriter wr = new BufferedWriter(new OutputStreamWriter(fos));
            wr.write(modified_file_str);
            wr.flush();
            wr.close();
          } catch (IOException ioe ) {
            fail("Failed while modifing cachejta.xml!" + ioe);
          }

	  props.setProperty("cache-xml-file",path);
	  return getSystem(props); 
  	}

  public static void test1() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    getLogWriter().fine("test1 is getting executed");
    vm0.invoke(JTADUnitTest.class, "Scenario1");
  }

  public static void Scenario1() throws Exception {
    Region currRegion = null;
    Cache cache;
    int tblIDFld;
    String tblNameFld;
    String tblName;
    String tableName = null;
//    boolean to_continue = true;
    //call to init method
    try {
      DistributedSystem ds = (new JTADUnitTest("temp")).setProperty();
      tableName = CacheUtils.init(ds,"CacheTest");
      getLogWriter().fine("Table name: " + tableName);
      tblName = tableName;
      if (tableName == null || tableName.equals("")) {
//        to_continue = false;
        fail(" table name not created, Aborting test...");
      }
    }
    catch (Exception e) {
//      to_continue = false;
      fail(" Aborting test at set up...[" + e + "]");
    }
    getLogWriter().fine("init for testscenario 1 Successful!");
    //test task
    cache = CacheUtils.getCache();
    tblName = tableName;
    currRegion = cache.getRegion("/root");
    tblIDFld = 1;
    tblNameFld = "test1";
    JTAUtils jtaObj = new JTAUtils(cache, currRegion);
    jtaObj.deleteRows(tblName);
    Context ctx = cache.getJNDIContext();
    UserTransaction ta = null;
    Connection conn = null;
    try {
      ta = (UserTransaction) ctx.lookup("java:/UserTransaction");
    }
    catch (NamingException e) {
      fail(" fail in user txn lookup " + e);
    }
    try {
      ta.begin();
      DataSource da = (DataSource) ctx.lookup("java:/XAPooledDataSource");
      conn = da.getConnection();
      Statement stmt = conn.createStatement();
      String sqlSTR = "insert into " + tblName + " values (" + tblIDFld + ","
          + "'" + tblNameFld + "'" + ")";
      stmt.executeUpdate(sqlSTR);
      stmt.close();
      ta.commit();
      conn.close();
    }
    catch (NamingException e) {
      fail(" failed " + e);
      ta.rollback();
    }
    catch (SQLException e) {
      fail(" failed " + e);
      ta.rollback();
    }
    catch (Exception e) {
      fail(" failed " + e);
      ta.rollback();
    }
    finally {
      if (conn != null) try {
        conn.close();
      }
      catch (SQLException ex) {
        fail("SQL exception: " + ex);
      }
    }
    getLogWriter().fine("test task for testscenario1 Succeessful");
    //destroying table
    try {
      getLogWriter().fine("Destroying table: " + tblName);
      CacheUtils.destroyTable(tblName);
      getLogWriter().fine("Closing cache...");
      //CacheUtils.closeCache();
      //CacheUtils.ds.disconnect();
      getLogWriter().fine("destroyTable for testscenario 1 Successful!");
    }
    catch (Exception e) {
      fail(" failed during tear down of this test..." + e);
    }
    finally {
      CacheUtils.closeCache();
//      CacheUtils.ds.disconnect();
      DistributedTestCase.disconnectFromDS();
    }
  }//end of test scenario 1

  /**
   * Test of testScenario2 method, of class com.gemstone.gemfire.internal.jta.functional.CacheTest1.
   * Tests a simple User Transaction with Cache lookup but closing the Connection object before
   * committing the Transaction.
   */
  public static void test2() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    getLogWriter().fine("test2 is getting executed");
    vm0.invoke(JTADUnitTest.class, "Scenario2");
  }

  public static void Scenario2() throws Exception {
    Region currRegion = null;
    Cache cache;
    int tblIDFld;
    String tblNameFld;
    String tblName = null;
    String tableName = null;
//    boolean to_continue = true;
    //call to init method
    try {
      DistributedSystem ds = (new JTADUnitTest("temp")).setProperty();
      tableName = CacheUtils.init(ds,"CacheTest");
      getLogWriter().fine("Table name: " + tableName);
      tblName = tableName;
      if (tableName == null || tableName.equals("")) {
//        to_continue = false;
        fail(" table name not created, Aborting test...");
      }
    }
    catch (Exception e) {
//      to_continue = false;
      fail(" Aborting test at set up...[" + e + "]");
    }
    getLogWriter().fine("init for testscenario 2 Successful!");
    //test task2
    cache = CacheUtils.getCache();
    currRegion = cache.getRegion("/root");
    tblIDFld = 2;
    tblNameFld = "test2";
    boolean rollback_chances = true;
    JTAUtils jtaObj = new JTAUtils(cache, currRegion);
    jtaObj.deleteRows(tblName);
    Context ctx = cache.getJNDIContext();
    getLogWriter().fine(" looking up UserTransaction... ");
    UserTransaction ta = null;
    Connection conn = null;
    try {
      ta = (UserTransaction) ctx.lookup("java:/UserTransaction");
    }
    catch (NamingException e) {
      fail(" user txn lookup failed " + e);
    }
    try {
      ta.begin();
      DataSource da = (DataSource) ctx.lookup("java:/XAPooledDataSource");
      conn = da.getConnection();
      Statement stmt = conn.createStatement();
      String sqlSTR = "insert into " + tblName + " values (" + tblIDFld + ","
          + "'" + tblNameFld + "'" + ")";
      stmt.executeUpdate(sqlSTR);
      stmt.close();
      conn.close();
      ta.commit();
      rollback_chances = false;
      int ifAnyRows = jtaObj.getRows(tblName);
      if (ifAnyRows == 0) {
        // no rows are there !!! failure :)
        fail(" no rows retrieved even after txn commit after conn close.");
      }
    }
    catch (NamingException e) {
      fail(" failed " + e);
      if (rollback_chances) ta.rollback();
    }
    catch (SQLException e) {
      fail(" failed " + e);
      if (rollback_chances) ta.rollback();
    }
    catch (Exception e) {
      fail(" failed " + e);
      ta.rollback();
    }
    finally {
      if (conn != null) try {
        conn.close();
      }
      catch (SQLException e) {
      }
    }
    getLogWriter().fine("test task for testscenario2 Successful!");
    //destroying table
    try {
      getLogWriter().fine("Destroying table: " + tblName);
      CacheUtils.destroyTable(tblName);
      getLogWriter().fine("Closing cache...");
      //CacheUtils.closeCache();
      //CacheUtils.ds.disconnect();
      getLogWriter().fine("destroyTable for testscenario 2 Successful!");
    }
    catch (Exception e) {
      fail(" failed during tear down of this test..." + e);
    }
    finally {
      CacheUtils.closeCache();
      //CacheUtils.ds.disconnect();
      DistributedTestCase.disconnectFromDS();
    }
  }//end of test scenario 2

  /**
   * Test of testScenario3 method, of class com.gemstone.gemfire.internal.jta.functional.CacheTest1.
   * Tests whether a user transaction with cache lookup and with XAPooledDataSOurce supports Cache 
   * put and get operations accordingly. Put and get are done within the transaction block and 
   * also db updates are done. After committing we check whether commit is proper in db and also
   * in Cache.
   */
  public static void test3() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    getLogWriter().fine("test3 is getting executed");
    vm0.invoke(JTADUnitTest.class, "Scenario3");
  }

  public static void Scenario3() throws Exception {
    Region currRegion = null;
    Cache cache;
    int tblIDFld;
    String tblNameFld;
    String tblName = null;
    String tableName = null;
//    boolean to_continue = true;
    //call to init method
    try {
      DistributedSystem ds = (new JTADUnitTest("temp")).setProperty();
      tableName = CacheUtils.init(ds,"CacheTest");
      getLogWriter().fine("Table name: " + tableName);
      tblName = tableName;
      if (tableName == null || tableName.equals("")) {
//        to_continue = false;
        fail(" table name not created, Aborting test...");
      }
    }
    catch (Exception e) {
//      to_continue = false;
      fail(" Aborting test at set up...[" + e + "]");
    }
    getLogWriter().fine("init for testscenario 3 Successful!");
    //testTask3
    cache = CacheUtils.getCache();
    currRegion = cache.getRegion("/root");
    tblIDFld = 3;
    tblNameFld = "test3";
    boolean rollback_chances = true;
    final String DEFAULT_RGN = "root";
    JTAUtils jtaObj = new JTAUtils(cache, currRegion);
    jtaObj.deleteRows(tblName);
    Context ctx = cache.getJNDIContext();
    UserTransaction ta = null;
    Connection conn = null;
    try {
      ta = (UserTransaction) ctx.lookup("java:/UserTransaction");
    }
    catch (NamingException e) {
      fail(" user txn lookup failed " + e);
    }
    try {
      /** begin the transaction **/
      ta.begin();
      String current_region = jtaObj.currRegion.getName();
      assertEquals("the default region is not root", DEFAULT_RGN,
          current_region);
      jtaObj.getRegionFromCache("region1");
      String current_fullpath = jtaObj.currRegion.getFullPath();
      assertEquals("failed retrieving current region fullpath", "/"
          + DEFAULT_RGN + "/region1", current_fullpath);
      jtaObj.put("key1", "value1");
      String str = jtaObj.get("key1");
      String tok = jtaObj.parseGetValue(str);
      assertEquals("get failed for corresponding put", "\"value1\"", tok);
      current_fullpath = jtaObj.currRegion.getFullPath();
      assertEquals("failed retrieving current region fullpath", "/"
          + DEFAULT_RGN + "/region1", current_fullpath);
      DataSource da = (DataSource) ctx.lookup("java:/XAPooledDataSource");
      conn = da.getConnection();
      Statement stmt = conn.createStatement();
      String sqlSTR = "insert into " + tblName + " values (" + tblIDFld + ","
          + "'" + tblNameFld + "'" + ")";
      stmt.executeUpdate(sqlSTR);
      stmt.close();
      ta.commit();
      conn.close();
      rollback_chances = false;
      current_fullpath = jtaObj.currRegion.getFullPath();
      assertEquals(
          "failed retrieving current region fullpath after txn commit", "/"
              + DEFAULT_RGN + "/region1", current_fullpath);
      int ifAnyRows = jtaObj.getRows(tblName);
      assertEquals("rows retrieved is:" + ifAnyRows, 1, ifAnyRows);
      /*if (ifAnyRows == 0) {
       fail (" DB FAILURE: no rows retrieved even after txn commit.");
       }*/
      // after jdbc commit cache value in region1 for key1 must retain...
      str = jtaObj.get("key1");
      tok = jtaObj.parseGetValue(str);
      assertEquals("cache put didn't commit, value retrieved is: " + tok,
          "\"value1\"", tok);
    }
    catch (CacheExistsException e) {
      fail(" test 3 failed ");
      ta.rollback();
    }
    catch (NamingException e) {
      fail(" test 3 failed " + e);
      if (rollback_chances) ta.rollback();
    }
    catch (SQLException e) {
      fail(" test 3 failed " + e);
      if (rollback_chances) ta.rollback();
    }
    catch (Exception e) {
      fail(" test 3 failed " + e);
      if (rollback_chances) ta.rollback();
    }
    finally {
      if (conn != null) try {
        conn.close();
      }
      catch (SQLException e) {
      }
    }
    getLogWriter().fine("test task for testscenarion3 Successful!");
    //call to destroyTable
    try {
      getLogWriter().fine("Destroying table: " + tblName);
      CacheUtils.destroyTable(tblName);
      getLogWriter().fine("Closing cache...");
      //CacheUtils.closeCache();
      //CacheUtils.ds.disconnect();
      getLogWriter().fine("destroyTable for testscenario 3 Successful!");
    }
    catch (Exception e) {
      fail(" failed during tear down of this test..." + e);
    }
    finally {
      CacheUtils.closeCache();
//      CacheUtils.ds.disconnect();
      DistributedTestCase.disconnectFromDS();
    }
  }//testscenarion3 end

  /**
   * Test of testScenario4 method, of class com.gemstone.gemfire.internal.jta.functional.CacheTest1.
   * Tests whether a user transaction with cache lookup and with XAPooledDataSOurce supports Cache
   * put and get operations accordingly along with JTA behavior. Put and get are done within the 
   * transaction block and also db updates are done. After rollback the transaction explicitly 
   * we check whether it was proper in db and also in Cache, which should also rollback.
   */
  // Please remove the prefix '_' from the method name to execute this particular test.
  // This particular test fails as of now as the cache put operation doesn't get rolledback
  // along with the JDBC rollback.
  public static void test4() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    getLogWriter().fine("test4 is getting executed");
    vm0.invoke(JTADUnitTest.class, "Scenario4");
  }

  public static void Scenario4() throws Exception {
    Region currRegion = null;
    Cache cache;
    int tblIDFld;
    String tblNameFld;
    String tblName = null;
    String tableName = null;
//    boolean to_continue = true;
    //call to init method
    try {
      DistributedSystem ds = (new JTADUnitTest("temp")).setProperty();
      tableName = CacheUtils.init(ds,"CacheTest");
      getLogWriter().fine("Table name: " + tableName);
      tblName = tableName;
      if (tableName == null || tableName.equals("")) {
//        to_continue = false;
        fail(" table name not created, Aborting test...");
      }
    }
    catch (Exception e) {
//      to_continue = false;
      fail(" Aborting test at set up...[" + e + "]");
    }
    getLogWriter().fine("init for testscenario 4 Successful!");
    //testTask4
    cache = CacheUtils.getCache();
    currRegion = cache.getRegion("/root");
    tblIDFld = 4;
    tblNameFld = "test4";
    boolean rollback_chances = true;
    final String DEFAULT_RGN = "root";
    JTAUtils jtaObj = new JTAUtils(cache, currRegion);
    jtaObj.deleteRows(tblName);
    Context ctx = cache.getJNDIContext();
    UserTransaction ta = null;
    Connection conn = null;
    try {
      ta = (UserTransaction) ctx.lookup("java:/UserTransaction");
    }
    catch (NamingException e) {
      fail(" user txn lookup failed " + e);
    }
    try {
      String current_region = jtaObj.currRegion.getName();
      assertEquals("default region is not root", DEFAULT_RGN, current_region);
      jtaObj.getRegionFromCache("region1");
      String current_fullpath = jtaObj.currRegion.getFullPath();
      assertEquals(
          "failed retrieving current region fullpath after doing getRegionFromCache(region1)",
          "/" + DEFAULT_RGN + "/region1", current_fullpath);
      jtaObj.put("key1", "test");
      ta.begin();
      jtaObj.put("key1", "value1");
      String str = jtaObj.get("key1");
      String tok = jtaObj.parseGetValue(str);
      assertEquals("get value do not match with the put", "\"value1\"", tok);
      current_fullpath = jtaObj.currRegion.getFullPath();
      assertEquals("failed retrieving current region fullpath", "/"
          + DEFAULT_RGN + "/region1", current_fullpath);
      DataSource da = (DataSource) ctx.lookup("java:/XAPooledDataSource");
      conn = da.getConnection();
      Statement stmt = conn.createStatement();
      String sqlSTR = "insert into " + tblName + " values (" + tblIDFld + ","
          + "'" + tblNameFld + "'" + ")";
      stmt.executeUpdate(sqlSTR);
      stmt.close();
      ta.rollback();
      conn.close();
      rollback_chances = false;
      current_fullpath = jtaObj.currRegion.getFullPath();
      assertEquals(
          "failed retirieving current region fullpath after txn rollback", "/"
              + DEFAULT_RGN + "/region1", current_fullpath);
      int ifAnyRows = jtaObj.getRows(tblName);
      assertEquals("rows retrieved is: " + ifAnyRows, 0, ifAnyRows);
      /*if (ifAnyRows != 0) {
       fail (" DB FAILURE");
       }*/
      str = jtaObj.get("key1");
      tok = jtaObj.parseGetValue(str);
      assertEquals("value existing in cache is: " + tok, "\"test\"", tok);
    }
    catch (CacheExistsException e) {
      ta.rollback();
      fail(" failed " + e);
    }
    catch (NamingException e) {
      if (rollback_chances) ta.rollback();
      fail(" failed " + e);
    }
    catch (SQLException e) {
      if (rollback_chances) ta.rollback();
      fail(" failed " + e);
    }
    catch (Exception e) {
      if (rollback_chances) ta.rollback();
      fail(" failed " + e);
    }
    finally {
      if (conn != null) try {
        conn.close();
      }
      catch (SQLException e) {
      }
    }
    getLogWriter().fine("testtask for testscenario 4 Successful!");
    //call to destroyTable
    try {
      getLogWriter().fine("Destroying table: " + tblName);
      CacheUtils.destroyTable(tblName);
      getLogWriter().fine("Closing cache...");
      //CacheUtils.closeCache();
      //CacheUtils.ds.disconnect();
      getLogWriter().fine("destroyTable for testscenario 4 Successful!");
    }
    catch (Exception e) {
      fail(" failed during tear down of this test..." + e);
    }
    finally {
      CacheUtils.closeCache();
//      CacheUtils.ds.disconnect();
      DistributedTestCase.disconnectFromDS();
    }
  }//testscenario4 ends

  /**
   * Test of testScenario5 method, of class com.gemstone.gemfire.internal.jta.functional.CacheTest1.
   * Tests whether a user transaction with cache lookup and with XAPooledDataSOurce supports Cache
   * put and get operations accordingly along with JTA behavior. Put and get are done within the
   * transaction block and also db updates are done. We try to rollback the transaction by violating
   * primary key constraint in db table, nad then we check whether rollback was proper in db and also in Cache, 
   * which should also rollback.
   */
  // Please remove the prefix '_' from the method name to execute this particular test.
  // This particular test fails as of now as the cache put operation doesn't get rolledback
  // along with the JDBC rollback.
  public static void test5() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    getLogWriter().fine("test5 is getting executed");
    vm0.invoke(JTADUnitTest.class, "Scenario5");
  }

  public static void Scenario5() throws Exception {
    Region currRegion = null;
    Cache cache;
    int tblIDFld;
    String tblNameFld;
    String tblName = null;
    String tableName = null;
//    boolean to_continue = true;
    //call to init method
    try {
      DistributedSystem ds = (new JTADUnitTest("temp")).setProperty();
      tableName = CacheUtils.init(ds,"CacheTest");
      getLogWriter().fine("Table name: " + tableName);
      tblName = tableName;
      if (tableName == null || tableName.equals("")) {
//        to_continue = false;
        fail(" table name not created, Aborting test...");
      }
    }
    catch (Exception e) {
//      to_continue = false;
      fail(" Aborting test at set up...[" + e + "]");
    }
    getLogWriter().fine("init for testscenario 5 Successful!");
    //test task5
    cache = CacheUtils.getCache();
    currRegion = cache.getRegion("/root");
    tblIDFld = 5;
    tblNameFld = "test5";
    boolean rollback_chances = false;
    final String DEFAULT_RGN = "root";
    JTAUtils jtaObj = new JTAUtils(cache, currRegion);
    jtaObj.deleteRows(tblName);
    Context ctx = cache.getJNDIContext();
    UserTransaction ta = null;
    Connection conn = null;
    try {
      ta = (UserTransaction) ctx.lookup("java:/UserTransaction");
    }
    catch (NamingException e) {
      fail(" failed in user txn lookup " + e);
    }
    try {
      String current_region = jtaObj.currRegion.getName();
      assertEquals("default region is not root", DEFAULT_RGN, current_region);
      // trying to create a region 'region1' under /root, if it doesn't exist. And point to the new region...
      jtaObj.getRegionFromCache("region1");
      // now current region should point to region1, as done from within getRegionFromCache method...
      String current_fullpath = jtaObj.currRegion.getFullPath();
      assertEquals("failed retirieving current fullpath", "/" + DEFAULT_RGN
          + "/region1", current_fullpath);
      jtaObj.put("key1", "test");
      ta.begin();
      jtaObj.put("key1", "value1");
      String str = jtaObj.get("key1");
      String tok = jtaObj.parseGetValue(str);
      assertEquals("get value mismatch with put", "\"value1\"", tok);
      current_fullpath = jtaObj.currRegion.getFullPath();
      assertEquals("failed retrieving current fullpath, current fullpath: "
          + current_fullpath, "/" + DEFAULT_RGN + "/region1", current_fullpath);
      DataSource da = (DataSource) ctx.lookup("java:/XAPooledDataSource");
      conn = da.getConnection();
      Statement stmt = conn.createStatement();
      String sqlSTR = "insert into " + tblName + " values (" + tblIDFld + ","
          + "'" + tblNameFld + "'" + ")";
      stmt.executeUpdate(sqlSTR);
      rollback_chances = true;
      // capable of throwing SQLException during insert operation
      sqlSTR = "insert into " + tblName + " values (" + tblIDFld + "," + "'"
          + tblNameFld + "'" + ")";
      stmt.executeUpdate(sqlSTR);
      stmt.close();
      ta.commit();
      conn.close();
    }
    catch (CacheExistsException e) {
      fail("test failed " + e);
      ta.rollback();
    }
    catch (NamingException e) {
      fail("test failed " + e);
      ta.rollback();
    }
    catch (SQLException e) {
      // exception thrown during inserts are handeled as below by rollback
      if (rollback_chances) {
        try {
          ta.rollback();
        }
        catch (Exception ex) {
          fail("failed: " + ex);
        }
        // intended error is checked w.r.t database first
        int ifAnyRows = 0;
        try {
          ifAnyRows = jtaObj.getRows(tblName);
        }
        catch (Exception ex) {
          fail(" failed: " + ex);
        }
        assertEquals("rows found after rollback is: " + ifAnyRows, 0, ifAnyRows);
        /*if (ifAnyRows != 0) {
         fail (" DB FAILURE");
         }*/
        // intended error is checked w.r.t. cache second
        String current_fullpath = jtaObj.currRegion.getFullPath();
        assertEquals(
            "failed retrieving current fullpath after rollback, fullpath is: "
                + current_fullpath, "/" + DEFAULT_RGN + "/region1",
            current_fullpath);
        // after jdbc rollback, cache value in region1 for key1 must vanish...
        String str1 = null;
        try {
          str1 = jtaObj.get("key1");
        }
        catch (CacheException ex) {
          fail("failed getting value for 'key1': " + ex);
        }
        String tok1 = jtaObj.parseGetValue(str1);
        assertEquals("value found in cache: " + tok1 + ", after rollback",
            "\"test\"", tok1);
      }
      else {
        fail(" failed: " + e);
        ta.rollback();
      }
    }
    catch (Exception e) {
      fail(" failed: " + e);
      ta.rollback();
    }
    finally {
      if (conn != null) try {
        conn.close();
      }
      catch (SQLException ex) {
        fail("Exception: " + ex);
      }
    }
    getLogWriter().fine("test task for testscenario 5 Successful");
    //call to destroTable
    try {
      getLogWriter().fine("Destroying table: " + tblName);
      CacheUtils.destroyTable(tblName);
      getLogWriter().fine("Closing cache...");
      //CacheUtils.closeCache();
      //CacheUtils.ds.disconnect();
      getLogWriter().fine("destroyTable for testscenario5 Successful!");
    }
    catch (Exception e) {
      fail(" failed during tear down of this test..." + e);
    }
    finally {
      CacheUtils.closeCache();
      //CacheUtils.ds.disconnect();
      DistributedTestCase.disconnectFromDS();
    }
  }//testscenario5 ends

  /**
   * Test of testScenario7 method, of class com.gemstone.gemfire.internal.jta.functional.CacheTest1.
   * Tests a user transaction with cache lookup and with XAPooledDataSource lookup. It does get and
   * put operations on the cache within the transaction block and checks whether these operations are
   * committed once the transaction is committed.
   */
  public static void test7() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    getLogWriter().fine("test7 is getting executed");
    vm0.invoke(JTADUnitTest.class, "Scenario7");
  }

  public static void Scenario7() throws Exception {
    Region currRegion = null;
    Cache cache;
//    int tblIDFld;
//    String tblNameFld;
    String tblName = null;
    String tableName = null;
//    boolean to_continue = true;
    //call to init method
    try {
      DistributedSystem ds = (new JTADUnitTest("temp")).setProperty();
      tableName = CacheUtils.init(ds,"CacheTest");
      getLogWriter().fine("Table name: " + tableName);
      tblName = tableName;
      if (tableName == null || tableName.equals("")) {
//        to_continue = false;
        fail(" table name not created, Aborting test...");
      }
    }
    catch (Exception e) {
//      to_continue = false;
      fail(" Aborting test at set up...[" + e + "]");
    }
    getLogWriter().fine("init for testscenario 7 Successful!");
    //test task 7
    cache = CacheUtils.getCache();
    currRegion = cache.getRegion("/root");
//    tblIDFld = 7;
//    tblNameFld = "test7";
    boolean rollback_chances = true;
    final String DEFAULT_RGN = "root";
    JTAUtils jtaObj = new JTAUtils(cache, currRegion);
    jtaObj.deleteRows(tblName);
    Context ctx = cache.getJNDIContext();
    UserTransaction ta = null;
    Connection conn = null;
    try {
      ta = (UserTransaction) ctx.lookup("java:/UserTransaction");
    }
    catch (NamingException e) {
      fail(" user txn lookup failed: " + e);
    }
    try {
      ta.begin();
      String current_region = jtaObj.currRegion.getName();
      assertEquals("default region is not root", DEFAULT_RGN, current_region);
      jtaObj.getRegionFromCache("region1");
      // now current region should point to region1, as done from within getRegionFromCache method...
      String current_fullpath = jtaObj.currRegion.getFullPath();
      assertEquals("failed retrieving the current region fullpath", "/"
          + DEFAULT_RGN + "/region1", current_fullpath);
      jtaObj.put("key1", "value1");
      // try to get value1 from key1 in the same region
      String str = jtaObj.get("key1");
      String tok = jtaObj.parseGetValue(str);
      assertEquals("get value mismatch with put", "\"value1\"", tok);
      current_fullpath = jtaObj.currRegion.getFullPath();
      assertEquals("failed retrieving current region fullpath", "/"
          + DEFAULT_RGN + "/region1", current_fullpath);
      DataSource da = (DataSource) ctx.lookup("java:/XAPooledDataSource");
      conn = da.getConnection();
      ta.commit();
      conn.close();
      rollback_chances = false;
      current_fullpath = jtaObj.currRegion.getFullPath();
      assertEquals(
          "failed retrieving current region fullpath after txn commit, fullpath is: "
              + current_region, "/" + DEFAULT_RGN + "/region1",
          current_fullpath);
      str = jtaObj.get("key1");
      tok = jtaObj.parseGetValue(str);
      assertEquals("cache value found is: " + tok, "\"value1\"", tok);
    }
    catch (CacheExistsException e) {
      fail(" failed due to: " + e);
      if (rollback_chances) ta.rollback();
    }
    catch (NamingException e) {
      fail(" failed due to: " + e);
      ta.rollback();
    }
    catch (SQLException e) {
      fail(" failed due to: " + e);
      ta.rollback();
    }
    catch (Exception e) {
      fail(" failed due to: " + e);
      if (rollback_chances) ta.rollback();
    }
    finally {
      if (conn != null) try {
        conn.close();
      }
      catch (Exception e) {
      }
    }
    getLogWriter().fine("test task for tes scenario 7 Successful!");
    //call to destroyTable
    try {
      getLogWriter().fine("Destroying table: " + tblName);
      CacheUtils.destroyTable(tblName);
      getLogWriter().fine("Closing cache...");
      //CacheUtils.closeCache();
      //CacheUtils.ds.disconnect();
      getLogWriter().fine("destroyTable for testscenario 7 Successful!");
    }
    catch (Exception e) {
      fail(" failed during tear down of this test..." + e);
    }
    finally {
      CacheUtils.closeCache();
//      CacheUtils.ds.disconnect();
      DistributedTestCase.disconnectFromDS();
    }
  }//testscenario 7 ends

  /**
   * Test of testScenario9 method, of class com.gemstone.gemfire.internal.jta.functional.CacheTest1.
   * Tests a user transaction with cache lookup and with XAPooledDataSource lookup performing a db
   * update within the transaction block and committing the transaction. Then agin we perform another
   * db update with the same DataSource and finally close the Connection. The first update only should
   * be committed in db and the second will not participate in transaction throwing an SQLException.
   */
  public static void test9() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    getLogWriter().fine("test9 is getting executed");
    vm0.invoke(JTADUnitTest.class, "Scenario9");
  }

  public static void Scenario9() throws Exception {
    Region currRegion = null;
    Cache cache;
    int tblIDFld;
    String tblNameFld;
    String tblName = null;
    String tableName = null;
//    boolean to_continue = true;
    //call to init method
    try {
      DistributedSystem ds = (new JTADUnitTest("temp")).setProperty();
      tableName = CacheUtils.init(ds,"CacheTest");
      getLogWriter().fine("Table name: " + tableName);
      tblName = tableName;
      if (tableName == null || tableName.equals("")) {
//        to_continue = false;
        fail(" table name not created, Aborting test...");
      }
    }
    catch (Exception e) {
//      to_continue = false;
      fail(" Aborting test at set up...[" + e + "]");
    }
    getLogWriter().fine("init for testscenario 9 Successful!");
    //test  task 9
    cache = CacheUtils.getCache();
    currRegion = cache.getRegion("/root");
    tblIDFld = 9;
    tblNameFld = "test9";
    boolean rollback_chances = true;
    int first_field = tblIDFld;
    JTAUtils jtaObj = new JTAUtils(cache, currRegion);
    // delete the rows inserted from CacheUtils createTable, otherwise conflict in PK's
    jtaObj.deleteRows(tblName);
    Context ctx = cache.getJNDIContext();
    UserTransaction ta = null;
    Connection conn = null;
    try {
      ta = (UserTransaction) ctx.lookup("java:/UserTransaction");
    }
    catch (NamingException e) {
      fail(" user txn lookup failed " + e);
    }
    try {
      ta.begin();
      DataSource da = (DataSource) ctx.lookup("java:/XAPooledDataSource");
      conn = da.getConnection();
      Statement stmt = conn.createStatement();
      String sqlSTR = "insert into " + tblName + " values (" + tblIDFld + ","
          + "'" + tblNameFld + "'" + ")";
      stmt.executeUpdate(sqlSTR);
      ta.commit();
      rollback_chances = false;
      // intended test for failure-- capable of throwing SQLException
      tblIDFld += 1;
      sqlSTR = "insert into " + tblName + " values (" + tblIDFld + "," + "'"
          + tblNameFld + "'" + ")";
      stmt.executeUpdate(sqlSTR);
      stmt.close();
      conn.close();
      rollback_chances = true;
    }
    catch (NamingException e) {
      fail(" failed due to: " + e);
      ta.rollback();
    }
    catch (SQLException e) {
      if (!rollback_chances) {
        int ifAnyRows = jtaObj.getRows(tblName);
        assertEquals("rows found is: " + ifAnyRows, 1, ifAnyRows);
        boolean matched = jtaObj.checkTableAgainstData(tblName, first_field
            + ""); // first field must be there.
        assertEquals("first entry to db is not found", true, matched);
      }
      else {
        ta.rollback();
        fail(" failed due to: " + e);
      }
    }
    catch (Exception e) {
      fail(" failed due to: " + e);
      if (rollback_chances) {
        ta.rollback();
      }
    }
    finally {
      if (conn != null) try {
        conn.close();
      }
      catch (SQLException e) {
      }
    }
    getLogWriter().fine("test task for testscenario 9 Successful");
    //destroyTable method call	
    try {
      getLogWriter().fine("Destroying table: " + tblName);
      CacheUtils.destroyTable(tblName);
      getLogWriter().fine("Closing cache...");
      //CacheUtils.closeCache();
      //CacheUtils.ds.disconnect();
      getLogWriter().fine("destroyTable for testscenario 9 Successful!");
    }
    catch (Exception e) {
      fail(" failed during tear down of this test..." + e);
    }
    finally {
      CacheUtils.closeCache();
      //CacheUtils.ds.disconnect();
      DistributedTestCase.disconnectFromDS();
    }
  }//testscenario 9 ends

  /**
   * Test of testScenario10 method, of class com.gemstone.gemfire.internal.jta.functional.CacheTest1.
   * Test a user transaction with cache lookup and with XAPooledDataSource for multiple JDBC Connections.
   * This should not be allowed for Facets datasource. For other relational DB the behaviour will be DB specific. 
   * For Oracle DB, n connections in a tranxn can be used provided , n-1 connections are closed before 
   * opening nth connection.
   */
  public static void test10() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    getLogWriter().fine("test10 is getting executed");
    vm0.invoke(JTADUnitTest.class, "Scenario10");
  }

  public static void Scenario10() throws Exception {
    Region currRegion = null;
    Cache cache;
    int tblIDFld;
    String tblNameFld;
    String tblName = null;
    String tableName = null;
//    boolean to_continue = true;
    //call to init method
    try {
      DistributedSystem ds = (new JTADUnitTest("temp")).setProperty();
      tableName = CacheUtils.init(ds,"CacheTest");
      getLogWriter().fine("Table name: " + tableName);
      tblName = tableName;
      if (tableName == null || tableName.equals("")) {
//        to_continue = false;
        fail(" table name not created, Aborting test...");
      }
    }
    catch (Exception e) {
//      to_continue = false;
      fail(" Aborting test at set up...[" + e + "]");
    }
    getLogWriter().fine("init for testscenario 10 Successful!");
    //test task 10
    cache = CacheUtils.getCache();
    currRegion = cache.getRegion("/root");
    tblIDFld = 10;
    tblNameFld = "test10";
    int rows_inserted = 0;
    int ifAnyRows = 0;
    int field1 = tblIDFld, field2 = 0;
    JTAUtils jtaObj = new JTAUtils(cache, currRegion);
    jtaObj.deleteRows(tblName);
    Context ctx = cache.getJNDIContext();
    UserTransaction ta = null;
    Connection conn1 = null;
    Connection conn2 = null;
    try {
      ta = (UserTransaction) ctx.lookup("java:/UserTransaction");
    }
    catch (NamingException e) {
      fail(" user lookup failed: " + e);
    }
    try {
      ta.begin();
      DataSource da = (DataSource) ctx.lookup("java:/XAPooledDataSource");
      conn1 = da.getConnection(); // the first Connection
      Statement stmt = conn1.createStatement();
      String sqlSTR = "insert into " + tblName + " values (" + tblIDFld + ","
          + "'" + tblNameFld + "'" + ")";
      stmt.executeUpdate(sqlSTR);
      rows_inserted += 1;
      conn1.close();
      conn2 = da.getConnection(); // the second Connection
      stmt = conn2.createStatement();
      tblIDFld += 1;
      field2 = tblIDFld;
      sqlSTR = "insert into " + tblName + " values (" + tblIDFld + "," + "'"
          + tblNameFld + "'" + ")";
      stmt.executeUpdate(sqlSTR);
      rows_inserted += 1;
      stmt.close();
      conn2.close();
      ta.commit();
      // if we reach here check for proper entries in db
      ifAnyRows = jtaObj.getRows(tblName);
      if (ifAnyRows == rows_inserted) {
        boolean matched1 = jtaObj.checkTableAgainstData(tblName, (field1 + ""));
        boolean matched2 = jtaObj.checkTableAgainstData(tblName, (field2 + ""));
        if (matched1)
          getLogWriter().fine("(PK " + field1 + "found ");
        else
          getLogWriter().fine("(PK " + field1 + "not found ");
        if (matched2)
          getLogWriter().fine("PK " + field2 + "found)");
        else
          getLogWriter().fine("PK " + field2 + "not found)");
        if (matched1 & matched2) {
          getLogWriter().fine("ok");
        }
        else {
          fail(" inserted data not found in DB !... failed");
        }
      }
      else {
        fail(" test interrupted, rows found=" + ifAnyRows + ", rows inserted="
            + rows_inserted);
      }
    }
    catch (NamingException e) {
      ta.rollback();
    }
    catch (SQLException e) {
      ta.rollback();
    }
    catch (Exception e) {
      ta.rollback();
    }
    finally {
      if (conn1 != null) try {
        conn1.close();
      }
      catch (SQLException e) {
      }
      if (conn2 != null) try {
        conn2.close();
      }
      catch (SQLException e) {
      }
    }
    getLogWriter().fine("test task for testscenario 10 Successful!");
    //destroyTable method call
    try {
      getLogWriter().fine("Destroying table: " + tblName);
      CacheUtils.destroyTable(tblName);
      getLogWriter().fine("Closing cache...");
      //CacheUtils.closeCache();
      //CacheUtils.ds.disconnect();
      getLogWriter().fine("destroyTable for testscenario 10 Successful!");
    }
    catch (Exception e) {
      fail(" failed during tear down of this test..." + e);
    }
    finally {
      CacheUtils.closeCache();
      //CacheUtils.ds.disconnect();
      DistributedTestCase.disconnectFromDS();
    }
  }//testscenario 10 ends

  /**
   * Test of testScenario11 method, of class com.gemstone.gemfire.internal.jta.functional.CacheTest1.
   * Tests Simple DataSource lookup within a transaction. Things should not participate in the transaction.
   */
  public static void test11() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    getLogWriter().fine("test11 is getting executed");
    vm0.invoke(JTADUnitTest.class, "Scenario11");
  }

  public static void Scenario11() throws Exception {
    Region currRegion = null;
    Cache cache;
    int tblIDFld;
    String tblNameFld;
    String tblName = null;
    String tableName = null;
//    boolean to_continue = true;
    //call to init method
    try {
      DistributedSystem ds = (new JTADUnitTest("temp")).setProperty();
      tableName = CacheUtils.init(ds,"CacheTest");
      getLogWriter().fine("Table name: " + tableName);
      tblName = tableName;
      if (tableName == null || tableName.equals("")) {
//        to_continue = false;
        fail(" table name not created, Aborting test...");
      }
    }
    catch (Exception e) {
//      to_continue = false;
      fail(" Aborting test at set up...[" + e + "]");
    }
    getLogWriter().fine("init for testscenario 11 Successful!");
    //test task 11
    cache = CacheUtils.getCache();
    currRegion = cache.getRegion("/root");
    tblIDFld = 11;
    tblNameFld = "test11";
    boolean rollback_chances = false;
    JTAUtils jtaObj = new JTAUtils(cache, currRegion);
    jtaObj.deleteRows(tblName);
    Context ctx = cache.getJNDIContext();
    UserTransaction ta = null;
    Connection conn = null;
    try {
      ta = (UserTransaction) ctx.lookup("java:/UserTransaction");
    }
    catch (NamingException e) {
      fail("failed in user txn look up: " + e);
    }
    try {
      ta.begin();
      DataSource da = (DataSource) ctx.lookup("java:/SimpleDataSource");
      conn = da.getConnection();
      Statement stmt = conn.createStatement();
      String sqlSTR = "insert into " + tblName + " values (" + tblIDFld + ","
          + "'" + tblNameFld + "'" + ")";
      stmt.executeUpdate(sqlSTR);
      rollback_chances = true;
      // try insert the same data once more and see if all info gets rolled back...
      // capable of throwing SQLException
      sqlSTR = "insert into " + tblName + " values (" + tblIDFld + "," + "'"
          + tblNameFld + "'" + ")";
      stmt.executeUpdate(sqlSTR);
      stmt.close();
      ta.commit();
      conn.close();
    }
    catch (NamingException e) {
      ta.rollback();
    }
    catch (SQLException e) {
      if (rollback_chances) {
        try {
          ta.rollback();
        }
        catch (Exception ex) {
          fail("failed due to : " + ex);
        }
        // try to check in the db whether any rows (the first one) are there now...
        int ifAnyRows = jtaObj.getRows(tblName);
        assertEquals("first row not found in case of Simple Datasource", 1,
            ifAnyRows); // one row-- the first one shud be there.
        boolean matched = jtaObj.checkTableAgainstData(tblName, tblIDFld + ""); // checking the existence of first row
        assertEquals("first row PK didn't matched", true, matched);
      }
      else {
        ta.rollback();
      }
    }
    catch (Exception e) {
      fail(" failed due to: " + e);
      ta.rollback();
    }
    finally {
      if (conn != null) try {
        conn.close();
      }
      catch (SQLException e) {
      }
    }
    getLogWriter().fine("test task for testscenario 11 Successful!");
    //destroyTable method call
    try {
      getLogWriter().fine("Destroying table: " + tblName);
      CacheUtils.destroyTable(tblName);
      getLogWriter().fine("Closing cache...");
      //CacheUtils.closeCache();
      //CacheUtils.ds.disconnect();
      getLogWriter().fine("destroyTable for testscenario 11 Successful!");
    }
    catch (Exception e) {
      fail(" failed during tear down of this test..." + e);
    }
    finally {
      CacheUtils.closeCache();
//      CacheUtils.ds.disconnect();
      DistributedTestCase.disconnectFromDS();
    }
  }//testscenario 11 ends

  /**
   * Test of testScenario14 method, of class com.gemstone.gemfire.internal.jta.functional.CacheTest1.
   * Tests a local Cache Loader with XADataSource lookup to get the connection. The Connection should
   * not participate in the transaction and commit/rollback should take affect accordingly along with
   * cache.
   */
  // Please remove the prefix '_' from the method name to execute this particular test.
  // This particular test fails as of now as the cache get operation retrieves value
  // even after user transaction rollback.
  public static void test14() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    getLogWriter().fine("test14 is getting executed");
    vm0.invoke(JTADUnitTest.class, "Scenario14");
  }

  public static void Scenario14() throws Exception {
    Region currRegion = null;
    Cache cache;
//    int tblIDFld;
//    String tblNameFld;
    String tblName = null;
    String tableName = null;
//    boolean to_continue = true;
    //call to init method
    try {
      DistributedSystem ds = (new JTADUnitTest("temp")).setProperty();
      tableName = CacheUtils.init(ds,"CacheTest");
      getLogWriter().fine("Table name: " + tableName);
      tblName = tableName;
      if (tableName == null || tableName.equals("")) {
//        to_continue = false;
        fail(" table name not created, Aborting test...");
      }
    }
    catch (Exception e) {
//      to_continue = false;
      fail(" Aborting test at set up...[" + e + "]");
    }
    getLogWriter().fine("init for testscenario 14 Successful!");
    //test task 14	
    cache = CacheUtils.getCache();
    currRegion = cache.getRegion("/root");
    final String TABLEID = "2";
//    final String TABLEFLD = "name2";
    JTAUtils jtaObj = new JTAUtils(cache, currRegion);
    try {
      Context ctx = cache.getJNDIContext();
      UserTransaction utx = (UserTransaction) ctx
          .lookup("java:/UserTransaction");
      utx.begin();
      AttributesFactory fac = new AttributesFactory(currRegion.getAttributes());
      fac.setCacheLoader(new XACacheLoaderTxn(tblName));
      Region re = currRegion.createSubregion("employee", fac
          .create());
      String retVal = (String) re.get(TABLEID); //TABLEID correspondes to "name1".
      if (!retVal.equals("newname"))
          fail("Uncommitted value 'newname' not read by cacheloader name = "
              + retVal);
      utx.rollback();
      DataSource ds = (DataSource) ctx.lookup("java:/XAPooledDataSource");
      Connection conn = ds.getConnection();
      Statement stm = conn.createStatement();
      String str = "select name from " + tblName + "  where id= (2)";
      ResultSet rs = stm.executeQuery(str);
      rs.next();
      String str1 = rs.getString(1);
      if (!str1.equals("name2"))
          fail("Rollback not occured on XAConnection got in a cache loader");
    }
    catch (Exception e) {
      fail(" failed due to :" + e);
    }
    getLogWriter().fine("test task for testscenario 14 Successful!");
    //destroyTable method call
    try {
      getLogWriter().fine("Destroying table: " + tblName);
      CacheUtils.destroyTable(tblName);
      getLogWriter().fine("Closing cache...");
      //CacheUtils.closeCache();
      //CacheUtils.ds.disconnect();
      getLogWriter().fine("destroyTable for testscenario 14 Successful!");
    }
    catch (Exception e) {
      fail(" failed during tear down of this test..." + e);
    }
    finally {
      CacheUtils.closeCache();
//      CacheUtils.ds.disconnect();
      DistributedTestCase.disconnectFromDS();
    }
  }//testscenario 14 ends

  /**
   * Test of testScenario15 method, of class com.gemstone.gemfire.internal.jta.functional.CacheTest1.
   * Tests performing DDL operations by looking up a XAPooledDataSource and without transaction.
   */
  public static void test15() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    getLogWriter().fine("test15 is getting executed");
    vm0.invoke(JTADUnitTest.class, "Scenario15");
  }

  public static void Scenario15() throws Exception {
    Region currRegion = null;
    Cache cache;
//    int tblIDFld;
//    String tblNameFld;
    String tblName = null;
    String tableName = null;
//    boolean to_continue = true;
    //call to init method
    try {
      DistributedSystem ds = (new JTADUnitTest("temp")).setProperty();
      tableName = CacheUtils.init(ds,"CacheTest");
      getLogWriter().fine("Table name: " + tableName);
      tblName = tableName;
      if (tableName == null || tableName.equals("")) {
//        to_continue = false;
        fail(" table name not created, Aborting test...");
      }
    }
    catch (Exception e) {
//      to_continue = false;
      fail(" Aborting test at set up...[" + e + "]");
    }
    getLogWriter().fine("init for testscenario 15 Successful!");
    //test task 15
    cache = CacheUtils.getCache();
    currRegion = cache.getRegion("/root");
//    tblIDFld = 15;
//    tblNameFld = "test15";
    String tbl = "";
    boolean row_num = true;
    int ddl_return = 1;
    JTAUtils jtaObj = new JTAUtils(cache, currRegion);
    // delete the rows inserted from CacheUtils createTable, otherwise conflict in PK's. Basically not needed for this test
    jtaObj.deleteRows(tblName);
    Context ctx = cache.getJNDIContext();
//    UserTransaction ta = null;
    Connection conn = null;
    Statement stmt = null;
    /*try {
     ta = (UserTransaction)ctx.lookup("java:/UserTransaction");

     } catch (NamingException e) {
     fail ("user txn lookup failed: " + e.getMessage ());
     }*/
    try {
      DataSource da = (DataSource) ctx.lookup("java:/XAPooledDataSource");
      conn = da.getConnection();
      stmt = conn.createStatement();
      // Do some DDL stuff
      String time = new Long(System.currentTimeMillis()).toString();
      tbl = "my_table" + time;
      //String sql = "create table " + tbl + " (my_id number primary key, my_name varchar2(50))";
      String sql = "create table "
          + tbl
          + " (my_id integer NOT NULL, my_name varchar(50), CONSTRAINT my_keyx PRIMARY KEY(my_id))";
      ddl_return = stmt.executeUpdate(sql);
      // check whether the table was created properly
      sql = "select * from " + tbl;
      ResultSet rs = null;
      rs = stmt.executeQuery(sql);
      row_num = rs.next();
      rs.close();
      if (ddl_return == 0 && !row_num) {
        sql = "drop table " + tbl;
        try {
          stmt = conn.createStatement();
          stmt.executeUpdate(sql);
        }
        catch (SQLException e) {
          fail(" failed to drop, " + e);
        }
      }
      else
        fail("unable to create table");
      /** Code meant for Oracle DB **/
      /*tbl = tbl.toUpperCase();
       sql = "select * from tab where tname like '%tbl%'";
       ResultSet rs = null;
       rs = stmt.executeQuery(sql);
       rs.close();
       sql = "drop table "+tbl;
       stmt = conn.createStatement();
       stmt.execute(sql);
       getLogWriter().fine (this.space + "ok");
       */
      stmt.close();
      conn.close();
    }
    catch (NamingException e) {
      fail("failed, " + e);
    }
    catch (SQLException e) {
      fail("failed, " + e);
    }
    catch (Exception e) {
      fail("failed, " + e);
    }
    finally {
      if (conn != null) try {
        conn.close();
      }
      catch (SQLException ex) {
        fail("Exception: " + ex);
      }
    }
    getLogWriter().fine("test task for test scenario 15 Successful!");
    //destroyTable method call
    try {
      getLogWriter().fine("Destroying table: " + tblName);
      CacheUtils.destroyTable(tblName);
      getLogWriter().fine("Closing cache...");
      //CacheUtils.closeCache();
      //CacheUtils.ds.disconnect();
      getLogWriter().fine("destroyTable for testscenario 15 Successful!");
    }
    catch (Exception e) {
      fail(" failed during tear down of this test..." + e);
    }
    finally {
      CacheUtils.closeCache();
//      CacheUtils.ds.disconnect();
      DistributedTestCase.disconnectFromDS();
    }
  }//testscenario 15 ends

  /**
   * Test of testScenario16 method, of class com.gemstone.gemfire.internal.jta.functional.CacheTest1.
   * Tests DDL operations on XAPooledDataSOurce lookup, without transaction but making auto commit
   * false just before performing DDL queries and making it true before closing the Connection. The
   * operations should be committed.
   */
  public static void test16() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    getLogWriter().fine("test16 is getting executed");
    vm0.invoke(JTADUnitTest.class, "Scenario16");
  }

  public static void Scenario16() throws Exception {
    Region currRegion = null;
    Cache cache;
//    int tblIDFld;
//    String tblNameFld;
    String tblName = null;
    String tableName = null;
//    boolean to_continue = true;
    //call to init method
    try {
      DistributedSystem ds = (new JTADUnitTest("temp")).setProperty();
      tableName = CacheUtils.init(ds,"CacheTest");
      getLogWriter().fine("Table name: " + tableName);
      tblName = tableName;
      if (tableName == null || tableName.equals("")) {
//        to_continue = false;
        fail(" table name not created, Aborting test...");
      }
    }
    catch (Exception e) {
//      to_continue = false;
      fail(" Aborting test at set up...[" + e + "]");
    }
    getLogWriter().fine("init for testscenario 16  Successful!");
    //test task 16
    cache = CacheUtils.getCache();
    currRegion = cache.getRegion("/root");
//    tblIDFld = 16;
//    tblNameFld = "test16";
    String tbl = "";
    int ddl_return = 1;
    boolean row_num = true;
    JTAUtils jtaObj = new JTAUtils(cache, currRegion);
    jtaObj.deleteRows(tblName);
    Context ctx = cache.getJNDIContext();
//    UserTransaction ta = null;
    Connection conn = null;
    Statement stmt = null;
    /*try {
     ta = (UserTransaction)ctx.lookup("java:/UserTransaction");

     } catch (NamingException e) {
     fail ("failed in User Txn lookup: " + e.getMessage ());
     }*/
    try {
      DataSource da = (DataSource) ctx.lookup("java:/XAPooledDataSource");
      conn = da.getConnection();
      conn.setAutoCommit(false);
      stmt = conn.createStatement();
      String time = new Long(System.currentTimeMillis()).toString();
      tbl = "my_table" + time;
      //String sql = "create table " + tbl + " (my_id number primary key, my_name varchar2(50))";
      String sql = "create table "
          + tbl
          + " (my_id integer NOT NULL, my_name varchar(50), CONSTRAINT my_key PRIMARY KEY(my_id))";
      ddl_return = stmt.executeUpdate(sql);
      sql = "select * from " + tbl;
      ResultSet rs = null;
      rs = stmt.executeQuery(sql);
      row_num = rs.next();
      rs.close();
      if (ddl_return == 0 && !row_num) {
        sql = "drop table " + tbl;
        try {
          stmt = conn.createStatement();
          stmt.executeUpdate(sql);
        }
        catch (SQLException e) {
          fail(" failed to drop, " + e);
        }
      }
      else
        fail("table do not exists");
      /*** Code meant for Oracle DB ***/
      /*tbl = tbl.toUpperCase();
       sql = "select * from tab where tname like '%tbl%'";
       ResultSet rs = null;
       rs = stmt.executeQuery(sql);
       rs.close();
       sql = "drop table "+tbl;
       stmt = conn.createStatement();
       stmt.executeUpdate(sql);
       getLogWriter().fine (this.space + "ok");
       */
      conn.setAutoCommit(true);
      stmt.close();
      conn.close();
    }
    catch (NamingException e) {
      fail("failed, " + e);
    }
    catch (SQLException e) {
      fail("failed, " + e);
    }
    catch (Exception e) {
      fail("failed, " + e);
    }
    finally {
      if (conn != null) try {
        conn.close();
      }
      catch (SQLException e) {
      }
    }
    getLogWriter().fine("test task for testscenario 16 Successful!");
    //destroyTable method call
    try {
      getLogWriter().fine("Destroying table: " + tblName);
      CacheUtils.destroyTable(tblName);
      getLogWriter().fine("Closing cache...");
      //CacheUtils.closeCache();
      //CacheUtils.ds.disconnect();
      getLogWriter().fine("destroyTable for testscenario 16 Successful!");
    }
    catch (Exception e) {
      fail(" failed during tear down of this test..." + e);
    }
    finally {
      CacheUtils.closeCache();
//      CacheUtils.ds.disconnect();
      DistributedTestCase.disconnectFromDS();
    }
  }//testscenario 16 ends

  /**
   * Test of testScenario18 method, of class com.gemstone.gemfire.internal.jta.functional.CacheTest1.
   * Get a connection (conn1) outside a transaction with pooled datasource lookup and another 
   * connection (conn2) within the same transaction with XAPooled datasource lookup. 
   * After making updates we do a rollback on the transaction and close both connections in the order
   * of opening them. The connection opened within transaction will only participate in the transaction.
   */
  public static void test18() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    getLogWriter().fine("test18 is getting executed");
    vm0.invoke(JTADUnitTest.class, "Scenario18");
  }

  public static void Scenario18() throws Exception {
    Region currRegion = null;
    Cache cache;
    int tblIDFld;
    String tblNameFld;
    String tblName = null;
    String tableName = null;
//    boolean to_continue = true;
    //call to init method
    try {
      DistributedSystem ds = (new JTADUnitTest("temp")).setProperty();
      tableName = CacheUtils.init(ds,"CacheTest");
      getLogWriter().fine("Table name: " + tableName);
      tblName = tableName;
      if (tableName == null || tableName.equals("")) {
//        to_continue = false;
        fail(" table name not created, Aborting test...");
      }
    }
    catch (Exception e) {
//      to_continue = false;
      fail(" Aborting test at set up...[" + e + "]");
    }
    getLogWriter().fine("init for testscenario 18 Successful!");
    //test task 18
    cache = CacheUtils.getCache();
    currRegion = cache.getRegion("/root");
    tblIDFld = 18;
    tblNameFld = "test18";
    boolean rollback_chances = true;
    int ifAnyRows = 0;
    JTAUtils jtaObj = new JTAUtils(cache, currRegion);
    jtaObj.deleteRows(tblName);
    Context ctx = cache.getJNDIContext();
    UserTransaction ta = null;
    Connection conn1 = null; // connection outside txn
    Connection conn2 = null; // connection within txn
    Statement stmt = null;
    DataSource da = null;
    try {
      da = (DataSource) ctx.lookup("java:/PooledDataSource");
      conn1 = da.getConnection();
      ta = (UserTransaction) ctx.lookup("java:/UserTransaction");
    }
    catch (NamingException e) {
      fail("failed: " + e);
    }
    catch (SQLException e) {
      fail("failed: " + e);
    }
    try {
      ta.begin();
      stmt = conn1.createStatement();
      String sqlSTR = "insert into " + tblName + " values (" + tblIDFld + ","
          + "'" + tblNameFld + "'" + ")";
      stmt.executeUpdate(sqlSTR);
      stmt.close();
      ifAnyRows = jtaObj.getRows(tblName);
      if (ifAnyRows == 0) {
        fail("failed no rows are there...");
      }
      // tryin a XAPooled lookup for second connection
      da = (DataSource) ctx.lookup("java:/XAPooledDataSource");
      conn2 = da.getConnection();
      stmt = conn2.createStatement();
      tblIDFld += 1;
      sqlSTR = "insert into " + tblName + " values (" + tblIDFld + "," + "'"
          + tblNameFld + "'" + ")";
      stmt.executeUpdate(sqlSTR);
      ta.rollback(); // intensional rollback
      stmt.close();
      conn2.close();
      conn1.close();
      rollback_chances = false;
      // if we reach here check whether the rollback was success for conn1 and conn2
      ifAnyRows = jtaObj.getRows(tblName);
      assertEquals("at least one row not retained after rollback", 1, ifAnyRows);
      boolean matched = jtaObj.checkTableAgainstData(tblName, tblIDFld + ""); // checking conn2's field
      if (matched) { // data is still in db
        fail(", PK " + tblIDFld + " found in db)" + "   "
            + "rollback for conn #2 failed");
      }
    }
    catch (Exception e) {
      fail("failed, " + e);
      if (rollback_chances) ta.rollback();
    }
    finally {
      if (conn1 != null) try {
        conn1.close();
      }
      catch (SQLException ex) {
        fail(" Exception: " + ex);
      }
      if (conn2 != null) try {
        conn2.close();
      }
      catch (SQLException ex) {
        fail(" Exception: " + ex);
      }
    }
    getLogWriter().fine("test task for testscenario 18 Successful!");
    //destroyTable method call
    try {
      getLogWriter().fine("Destroying table: " + tblName);
      CacheUtils.destroyTable(tblName);
      getLogWriter().fine("Closing cache...");
      //CacheUtils.closeCache();
      //CacheUtils.ds.disconnect();
      getLogWriter().fine("destroyTable for testscenario 18 Successful!");
    }
    catch (Exception e) {
      fail(" failed during tear down of this test..." + e);
    }
    finally {
      CacheUtils.closeCache();
//      CacheUtils.ds.disconnect();
      DistributedTestCase.disconnectFromDS();
    }
  }//testscenario 18 ends

  public static void test19() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    getLogWriter().fine("test19 is getting executed");
    vm0.invoke(JTADUnitTest.class, "Scenario19");
  }

  /**
   * This method tests the CacheWriter behaviour of 
   * GemFire in transactional environment.
   * This method makes use of XACacheWriterTxn 
   * (subclass of this class) to demonstrate the 
   * behaviour of CacheWriter. 
   * 
   * @author Nand Kishor
   */
  public static void Scenario19() {
    Region currRegion = null;
    Cache cache;
//    int tblIDFld;
//    String tblNameFld;
    String tblName = null;
    String tableName = null;
//    boolean to_continue = true;
    //call to init method
    try {
      DistributedSystem ds = (new JTADUnitTest("temp")).setProperty();
      tableName = CacheUtils.init(ds,"CacheTest");
      getLogWriter().fine("Table name: " + tableName);
      tblName = tableName;
      if (tableName == null || tableName.equals("")) {
//        to_continue = false;
        fail(" table name not created, Aborting test...");
      }
    }
    catch (Exception e) {
//      to_continue = false;
      fail(" Aborting test at set up...[" + e + "]");
    }
    getLogWriter().fine("init for testscenario 19 Successful!");
    //test task 19	
    cache = CacheUtils.getCache();
    currRegion = cache.getRegion("/root");
    JTAUtils jtaObj = new JTAUtils(cache, currRegion);
    String maxKey = ((new XACacheWriterTxn(tblName))
        .getData("select max(id) from " + tblName));
    int nextKey = Integer.valueOf(maxKey).intValue() + 1;
    try {
      Context ctx = cache.getJNDIContext();
      UserTransaction utx = (UserTransaction) ctx
          .lookup("java:/UserTransaction");
      AttributesFactory fac = new AttributesFactory(currRegion.getAttributes());
      Region nextEmployee = currRegion.createSubregion("nextemployee", fac
          .create());
      fac.setCacheWriter(new XACacheWriterTxn(tblName));
      Region employee = currRegion.createSubregion("employee", fac
          .create());
      // put without transaction
      employee.put("" + nextKey, "name" + nextKey);
      String newMaxKey = (new XACacheWriterTxn(tblName))
          .getData("select max(id) from " + tblName);
      if (!newMaxKey.equals("" + nextKey))
          fail(" Data was not found in Database ");
      String value = (String) nextEmployee.get("" + nextKey);
      if (!value.equals("name" + nextKey))
          fail(" Data was not replicated in nextemployee region ");
      //put with txn and rollback
      nextKey += 1;
      utx.begin();
      employee.put("" + nextKey, "name" + nextKey);
      if (!(employee.get("" + nextKey)).equals("name" + nextKey))
          fail(" Got different value in get operation ");
      utx.rollback();
      newMaxKey = (new XACacheWriterTxn(tblName))
          .getData("select max(id) from " + tblName);
      if (Integer.valueOf(newMaxKey).intValue() - nextKey != -1)
          fail(" Data was not rolledback in Database ");
      value = (String) nextEmployee.get("" + nextKey);
      if (value != null)
          fail(" Data was not rolledback in nextemployee region : value found was : "
              + value);
      //insert a new value inside transaction
      utx.begin();
      employee.put("" + nextKey, "name" + nextKey);
      utx.commit();
      value = (new XACacheWriterTxn(tblName)).getData("select name from "
          + tblName + " where id = " + nextKey);
      if (!value.equals("name" + nextKey))
          fail(" Data was not updated in Database ");
      //check data in nextemployee
      value = (String) nextEmployee.get("" + nextKey);
      if (!value.equals("name" + nextKey))
          fail(" Data was not replicated in nextemployee region ");
      //update the inserted value
      utx.begin();
      employee.put("" + nextKey, "name" + (nextKey + 1));
      utx.commit();
      value = (new XACacheWriterTxn(tblName)).getData("select name from "
          + tblName + " where id = " + nextKey);
      if (!value.equals("name" + (nextKey + 1)))
          fail(" Data was not updated in Database ");
      //    check data in nextemployee
      value = (String) nextEmployee.get("" + nextKey);
      if (!value.equals("name" + (nextKey + 1)))
          fail(" Data was not replicated in nextemployee region ");
      //destroying the updated value
      utx.begin();
      employee.destroy("" + nextKey);
      utx.commit();
      value = (new XACacheWriterTxn(tblName)).getData("select count(id) from "
          + tblName + " where id = " + nextKey);
      if (Integer.valueOf(value).intValue() != 0)
          fail(" Data was not not destroyed in Database ");
      //check data in nextemployee
    }
    catch (Exception e) {
      e.printStackTrace();
      fail(" failed due to :" + e.getMessage());
    }
    getLogWriter().fine("test task for testscenario 19 Successful!");
    //destroyTable method call
    try {
      getLogWriter().fine("Destroying table: " + tblName);
      CacheUtils.destroyTable(tblName);
      getLogWriter().fine("Closing cache...");
      //CacheUtils.closeCache();
      //CacheUtils.ds.disconnect();
      getLogWriter().fine("destroyTable for testscenario 19 Successful!");
    }
    catch (Exception e) {
      fail(" failed during tear down of this test..." + e);
    }
    finally {
      CacheUtils.closeCache();
//      CacheUtils.ds.disconnect();
      DistributedTestCase.disconnectFromDS();
    }
  }//testscenario 19 ends

  public static void testBug46169() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    getLogWriter().fine("testBug46169 is getting executed");
    vm0.invoke(JTADUnitTest.class, "bug46169");
  }

  public static void bug46169() throws Exception {
    Cache cache;
    String tableName = null;
    try {
      DistributedSystem ds = (new JTADUnitTest("temp")).setProperty();
      tableName = CacheUtils.init(ds,"CacheTest");
      getLogWriter().fine("Table name: " + tableName);
      if (tableName == null || tableName.equals("")) {
        fail(" table name not created, Aborting test...");
      }
    }
    catch (Exception e) {
      fail(" Aborting test at set up...[" + e + "]");
    }
    getLogWriter().fine("init for bug46169 Successful!");
    cache = CacheUtils.getCache();
    
    TransactionManager xmanager = (TransactionManager) cache.getJNDIContext().lookup("java:/TransactionManager");
    assertNotNull(xmanager);
    
    Transaction trans = xmanager.suspend();
    assertNull(trans);

    try {
      getLogWriter().fine("Destroying table: " + tableName);
      CacheUtils.destroyTable(tableName);
      getLogWriter().fine("Closing cache...");
      getLogWriter().fine("destroyTable for bug46169 Successful!");
    }
    catch (Exception e) {
      fail(" failed during tear down of this test..." + e);
    }
    finally {
      CacheUtils.closeCache();
      DistributedTestCase.disconnectFromDS();
    }
  }

  public static void testBug46192() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    getLogWriter().fine("testBug46192 is getting executed");
    vm0.invoke(JTADUnitTest.class, "bug46192");
  }

  public static void bug46192() throws Exception {
    Cache cache;
    String tableName = null;
    try {
      DistributedSystem ds = (new JTADUnitTest("temp")).setProperty();
      tableName = CacheUtils.init(ds,"CacheTest");
      getLogWriter().fine("Table name: " + tableName);
      if (tableName == null || tableName.equals("")) {
        fail(" table name not created, Aborting test...");
      }
    }
    catch (Exception e) {
      fail(" Aborting test at set up...[" + e + "]");
    }
    getLogWriter().fine("init for bug46192 Successful!");
    cache = CacheUtils.getCache();
    
    TransactionManager xmanager = (TransactionManager) cache.getJNDIContext().lookup("java:/TransactionManager");
    assertNotNull(xmanager);
    
    try {
      xmanager.rollback();
    } catch (IllegalStateException e) {
      // Do nothing, correct exception
    } catch (Exception e) {
      fail("Wrong exception type when there is no transaction to rollback for current thread.",e);
    }
    
    try {
      xmanager.commit();
    } catch (IllegalStateException e) {
      // Do nothing, correct exception
    } catch (Exception e) {
      fail("Wrong exception type when there is no transaction to commit for current thread.",e);
    }

    try {
      getLogWriter().fine("Destroying table: " + tableName);
      CacheUtils.destroyTable(tableName);
      getLogWriter().fine("Closing cache...");
      getLogWriter().fine("destroyTable for bug46192 Successful!");
    }
    catch (Exception e) {
      fail(" failed during tear down of this test..." + e);
    }
    finally {
      CacheUtils.closeCache();
      DistributedTestCase.disconnectFromDS();
    }
  }

  /* required for test # 14
   /*The following class is made statis because its constroctor is being invoked from static method*/
  static class XACacheLoaderTxn implements CacheLoader {

    String tableName;

    /** Creates a new instance of XACacheLoaderTxn */
    public XACacheLoaderTxn(String str) {
      this.tableName = str;
    }

    public final Object load(LoaderHelper helper) throws CacheLoaderException {
      getLogWriter().fine("In Loader.load for" + helper.getKey());
      return loadFromDatabase(helper.getKey());
    }

    private Object loadFromDatabase(Object ob) {
      Object obj = null;
      try {
        Context ctx = CacheFactory.getAnyInstance().getJNDIContext();
        DataSource ds = (DataSource) ctx.lookup("java:/XAPooledDataSource");
        Connection conn = ds.getConnection();
        Statement stm = conn.createStatement();
        String str = "update " + tableName
            + " set name ='newname' where id = ("
            + (new Integer(ob.toString())).intValue() + ")";
        stm.executeUpdate(str);
        ResultSet rs = stm.executeQuery("select name from " + tableName
            + " where id = (" + (new Integer(ob.toString())).intValue() + ")");
        rs.next();
        obj = rs.getString(1);
        stm.close();
        conn.close();
        return obj;
      }
      catch (Exception e) {
        e.printStackTrace();
      }
      return obj;
    }

    public void close() {
      // TODO Auto-generated method stub
    }
  }

  /**
   * This class is required for Scenario19.
   * Demonstrate the CacheWriter behaviour of 
   * GemFire in transactional environment.
   * 
   * @author Nand Kishor
   *
   */
  static class XACacheWriterTxn implements CacheWriter {

    String tableName;

    /** Creates a new instance of XACacheWriterTxn */
    public XACacheWriterTxn(String str) {
      tableName = str;
    }

    public void beforeUpdate(EntryEvent arg0) throws CacheWriterException {
      String key = (String) arg0.getKey();
      String newValue = (String) arg0.getNewValue();
      String oldValue = (String) arg0.getOldValue();
      String query = "update " + tableName + " set name = '" + newValue
          + "' where id = (" + (new Integer(key)).intValue() + ")";
      executeQuery(query);
      try {
        Region r = arg0.getRegion();
        Region newRegion = r.getParentRegion().getSubregion("nextemployee");
        newRegion.put(key, newValue);
      }
      catch (TimeoutException te) {
        throw new CacheClosedException("Beforecreate timeout exception" + te);
      }
    }

    public void beforeCreate(EntryEvent arg0) throws CacheWriterException {
      String key = (String) arg0.getKey();
      String newValue = (String) arg0.getNewValue();
      String oldValue = (String) arg0.getOldValue();
      String query = "insert into " + tableName + " values ("
          + (new Integer(key)).intValue() + " , '" + newValue + "')";
      executeQuery(query);
      try {
        Region r = arg0.getRegion();
        Region newRegion = r.getParentRegion().getSubregion("nextemployee");
        newRegion.put(key, newValue);
      }
      catch (TimeoutException te) {
        throw new CacheClosedException("Beforecreate timeout exception" + te);
      }
    }

    protected String getData(String query) {
      String returnValue = "";
      Connection conn = null;
      Statement stm = null;
      try {
        Context ctx = CacheFactory.getAnyInstance().getJNDIContext();
        DataSource ds = (DataSource) ctx.lookup("java:/SimpleDataSource");
        conn = ds.getConnection();
        stm = conn.createStatement();
        ResultSet rs = stm.executeQuery(query);
        rs.next();
        returnValue = rs.getString(1);
      }
      catch (Exception e) {
        e.printStackTrace();
      }
      finally {
        try {
          if (stm != null) {
            stm.close();
            stm = null;
          }
          if (conn != null) {
            conn.close();
            conn = null;
          }
        }
        catch (SQLException sq) {
          sq.printStackTrace();
        }
      }
      return returnValue;
    }

    private int executeQuery(String query) {
      int returnValue = 0;
      Connection conn = null;
      Statement stm = null;
      try {
        Context ctx = CacheFactory.getAnyInstance().getJNDIContext();
        DataSource ds = (DataSource) ctx.lookup("java:/XAPooledDataSource");
        conn = ds.getConnection();
        stm = conn.createStatement();
        returnValue = stm.executeUpdate(query);
      }
      catch (Exception e) {
        e.printStackTrace();
      }
      finally {
        try {
          if (stm != null) {
            stm.close();
            stm = null;
          }
          if (conn != null) {
            conn.close();
            conn = null;
          }
        }
        catch (SQLException sq) {
          sq.printStackTrace();
        }
      }
      return returnValue;
    }

    public void beforeDestroy(EntryEvent arg0) throws CacheWriterException {
      String key = (String) arg0.getKey();
      String newValue = (String) arg0.getNewValue();
      String oldValue = (String) arg0.getOldValue();
      String query = "delete from " + tableName + " where id = ("
          + (new Integer(key)).intValue() + ")";
      executeQuery(query);
      try {
        Region r = arg0.getRegion();
        Region newRegion = r.getParentRegion().getSubregion("nextemployee");
        newRegion.destroy(key);
      }
      catch (TimeoutException te) {
        throw new CacheClosedException("Beforecreate timeout exception" + te);
      }
      catch (EntryNotFoundException enfe) {
        throw new CacheClosedException(
            "Beforecreate CapacityControllerException exception" + enfe);
      }
    }

    public void beforeRegionDestroy(RegionEvent arg0)
        throws CacheWriterException {
    }
    public void beforeRegionClear(RegionEvent arg0)
        throws CacheWriterException {
    }

    public void close() {
    }
  }// end XACacheWriterTxn

  private static String readFile(String filename) throws IOException {
//    String lineSep = System.getProperty("\n");
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
    getLogWriter().fine("***********\n " + sb);
    return sb.toString();
  }

  public void testBug43987() {
    InternalDistributedSystem ds = null;
    try {
      ds = getMcastSystem(); // ties us in to the DS owned by DistributedTestCase.
      CacheFactory cf = new CacheFactory(ds.getProperties());
      Cache c = cf.create(); // should just reuse the singleton DS owned by DistributedTestCase.
      RegionFactory<String, String> rf = c.createRegionFactory(RegionShortcut.REPLICATE);
      Region<String, String> r = rf.create("JTA_reg");
      r.put("key", "value");
      c.close();
      Cache cache1 = cf.create();
      RegionFactory<String, String> rf1 = cache1.createRegionFactory(RegionShortcut.REPLICATE);
      Region<String, String> r1 = rf1.create("JTA_reg");
      r1.put("key1", "value");
    } finally {
      if (ds != null) {
        ds.disconnect();
      }
    }
  }
  
}// end of class
