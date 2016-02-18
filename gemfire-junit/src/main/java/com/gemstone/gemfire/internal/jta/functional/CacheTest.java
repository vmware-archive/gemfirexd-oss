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
/*
 * CacheTest.java
 * JUnit based test
 *
 * Created on March 28, 2005, 10:59 AM
 */

package com.gemstone.gemfire.internal.jta.functional;

import junit.framework.TestCase;
//import junit.framework.*;
//import java.io.*;
//import java.util.*;
//import java.net.*;
import com.gemstone.gemfire.cache.*;
//import com.gemstone.gemfire.distributed.*;
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
import com.gemstone.gemfire.internal.jta.CacheUtils;
import com.gemstone.gemfire.internal.jta.JTAUtils;

/**
 * This JUnit version is created from class com.gemstone.gemfire.internal.jta.functional.CacheTest1.
 * Tests functional behavior. Assumes that CacheUtils.java always creates table(s) and inserts data
 * in the form: 1 => name1, 2 => name2, 3 => name3...as the values of ID and name fields respectively.
 * Test # 15 & 16 has the dependency to run on CloudScape database only due to variations in DDL.
 * @author prabir
 */
public class CacheTest extends TestCase {

	 private static Region currRegion;
	 private static Cache cache;
	 private int tblIDFld;
	 private String tblNameFld;
	 private String tblName;

	 private boolean to_continue = false;

	 public CacheTest(String testName) {
		  super(testName);
	 }

	 protected void setUp() throws java.lang.Exception {

		  this.to_continue = true;
		  try {
			   // trying basic setup
			   String tableName = CacheUtils.init("CacheTest");
			   this.tblName = tableName;

			   if (tableName == null || tableName.equals ("")) {
					this.to_continue = false;
					fail (" table name not created, Aborting test...");
			   }
			   else {
					cache = CacheUtils.getCache();
					if (cache == null) {
						 this.to_continue = false;
						 fail (" cache is null, Aborting test...");
					}
					else {
						 currRegion = cache.getRegion("root");
						 if (!currRegion.getFullPath().equals("/root")) {
							  this.to_continue = false;
							  fail (" default cache path not 'root', Aborting test...");
						 }
					}
					this.to_continue = true;
			   }
		  }
		  /*catch (NoInitialContextException e) {
			this.to_continue = false;
			CacheUtils.closeCache ();
			fail (" Aborting test at set up..." + e.getMessage ());
			}*/
		  catch (Exception e) {
			   this.to_continue = false;
			   CacheUtils.closeCache ();
			   fail (" Aborting test at set up...[" + e.getMessage () + "]");
		  }
	 }

	 protected void tearDown() throws java.lang.Exception {
		  if (!this.to_continue) {
			   return;
		  }

		  try {
			   CacheUtils.closeCache ();
			   CacheUtils.destroyTable(this.tblName);
		  }
		  catch (Exception e) {
			   fail (" failed during tear down of this test..." + e.getMessage ());
		  }
		  finally {
			   CacheUtils.closeCache ();
		  }
	 }

	 public static junit.framework.Test suite() {

		  junit.framework.TestSuite suite = new junit.framework.TestSuite(CacheTest.class);

		  return suite;
	 }

	 /**
	  * Test of testScenario1 method, of class com.gemstone.gemfire.internal.jta.functional.CacheTest1.
	  * Tests a simple User Transaction with Cache lookup.
	  */
	 public void testScenario1() throws Exception {

		  if (!this.to_continue) {
			   fail (" test 1" + "FAILED");
			   return;
		  }

		  this.tblIDFld = 1;
		  this.tblNameFld = "test1";
		  JTAUtils jtaObj = new JTAUtils (CacheTest.cache, CacheTest.currRegion);

		  jtaObj.deleteRows(this.tblName);

		  Context ctx = cache.getJNDIContext();
		  UserTransaction ta = null;
		  Connection conn = null;
		  try {
			   ta = (UserTransaction)ctx.lookup("java:/UserTransaction");

		  } catch (NamingException e) {
			   fail (" fail in user txn lookup " + e.getMessage ());
		  }

		  try{
			   ta.begin();

			   DataSource da = (DataSource)ctx.lookup("java:/XAPooledDataSource");

			   conn = da.getConnection();

			   Statement stmt = conn.createStatement();

			   String sqlSTR = "insert into " + this.tblName + " values (" + this.tblIDFld + "," + "'" + this.tblNameFld + "'" + ")" ;
			   stmt.executeUpdate(sqlSTR);

			   stmt.close();
			   ta.commit ();
			   conn.close();
		  }
		  catch (NamingException e){
			   fail (" failed " + e.getMessage ());
			   ta.rollback();
		  }
		  catch (SQLException e) {
			   fail (" failed " + e.getMessage ());
			   ta.rollback();
		  }
		  catch (Exception e){
			   fail (" failed " + e.getMessage ());
			   ta.rollback();
		  }
		  finally
		  {
			   if (conn != null)
					try {
						 conn.close ();
					}
			   catch (SQLException ex) { fail ("SQL exception: " + ex.getMessage ()); }
		  }
	 }


	 /**
	  * Test of testScenario2 method, of class com.gemstone.gemfire.internal.jta.functional.CacheTest1.
	  * Tests a simple User Transaction with Cache lookup but closing the Connection object before
	  * committing the Transaction.
	  */
	 public void testScenario2() throws Exception {

		  if (!this.to_continue) {
			   fail (" test 2" + "FAILED");
			   return;
		  }

		  this.tblIDFld = 2;
		  this.tblNameFld = "test2";
		  boolean rollback_chances = true;

		  JTAUtils jtaObj = new JTAUtils (CacheTest.cache, CacheTest.currRegion);

		  jtaObj.deleteRows(this.tblName);

		  Context ctx = cache.getJNDIContext();
		  System.out.print (" looking up UserTransaction... ");
		  UserTransaction ta = null;
		  Connection conn = null;
		  try {
			   ta = (UserTransaction)ctx.lookup("java:/UserTransaction");
		  } catch (NamingException e) {
			   fail (" user txn lookup failed " + e.getMessage ());
		  }

		  try{
			   ta.begin();

			   DataSource da = (DataSource)ctx.lookup("java:/XAPooledDataSource");

			   conn = da.getConnection();

			   Statement stmt = conn.createStatement();

			   String sqlSTR = "insert into " + this.tblName + " values (" + this.tblIDFld + "," + "'" + this.tblNameFld + "'" + ")" ;
			   stmt.executeUpdate(sqlSTR);

			   stmt.close();
			   conn.close();
			   ta.commit ();

			   rollback_chances = false;

			   int ifAnyRows = jtaObj.getRows(this.tblName);
			   if (ifAnyRows == 0) {
					// no rows are there !!! failure :)
					fail (" no rows retrieved even after txn commit after conn close.");
			   }

		  }
		  catch (NamingException e){
			   fail (" failed " + e.getMessage ());
			   if (rollback_chances)
					ta.rollback();
		  }
		  catch (SQLException e) {
			   fail (" failed " + e.getMessage ());
			   if (rollback_chances)
					ta.rollback();
		  }
		  catch(Exception e){
			   fail (" failed " + e.getMessage ());
			   ta.rollback();
		  }
		  finally
		  {
			   if (conn != null)
					try {
						 conn.close ();
					}
			   catch (SQLException e) {}
		  }
	 }

	 /**
	  * Test of testScenario3 method, of class com.gemstone.gemfire.internal.jta.functional.CacheTest1.
	  * Tests whether a user transaction with cache lookup and with XAPooledDataSOurce supports Cache 
	  * put and get operations accordingly. Put and get are done within the transaction block and 
	  * also db updates are done. After committing we check whether commit is proper in db and also
	  * in Cache.
	  */
	 public void testScenario3() throws Exception {

		  if (!this.to_continue) {
			   fail (" test 3 failed");
			   return;
		  }

		  this.tblIDFld = 3;
		  this.tblNameFld = "test3";
		  boolean rollback_chances = true;
		  final String DEFAULT_RGN = "root";

		  JTAUtils jtaObj = new JTAUtils (CacheTest.cache, CacheTest.currRegion);

		  jtaObj.deleteRows(this.tblName);

		  Context ctx = cache.getJNDIContext();
		  UserTransaction ta = null;
		  Connection conn = null;
		  try {
			   ta = (UserTransaction)ctx.lookup("java:/UserTransaction");

		  } catch (NamingException e) {
			   fail (" user txn lookup failed " + e.getMessage ());
		  }

		  try{
			   /** begin the transaction **/
			   ta.begin();

			   String current_region = jtaObj.currRegion.getName ();
			   assertEquals ("the default region is not root", DEFAULT_RGN, current_region);

			   jtaObj.getRegionFromCache ("region1");

			   String current_fullpath = jtaObj.currRegion.getFullPath();
			   assertEquals ("failed retrieving current region fullpath", "/" + DEFAULT_RGN + "/region1", current_fullpath);

			   jtaObj.put ("key1", "value1");

			   String str = jtaObj.get ("key1");
			   String tok = jtaObj.parseGetValue (str);
			   assertEquals ("get failed for corresponding put", "\"value1\"", tok);

			   current_fullpath = jtaObj.currRegion.getFullPath();
			   assertEquals ("failed retrieving current region fullpath", "/" + DEFAULT_RGN + "/region1", current_fullpath);

			   DataSource da = (DataSource)ctx.lookup("java:/XAPooledDataSource");

			   conn = da.getConnection();

			   Statement stmt = conn.createStatement();

			   String sqlSTR = "insert into " + this.tblName + " values (" + this.tblIDFld + "," + "'" + this.tblNameFld + "'" + ")" ;
			   stmt.executeUpdate(sqlSTR);

			   stmt.close ();
			   ta.commit ();
			   conn.close();

			   rollback_chances = false;

			   current_fullpath = jtaObj.currRegion.getFullPath();
			   assertEquals ("failed retrieving current region fullpath after txn commit", "/" + DEFAULT_RGN + "/region1", current_fullpath);

			   int ifAnyRows = jtaObj.getRows(this.tblName);
			   assertEquals ("rows retrieved is:" + ifAnyRows, 1, ifAnyRows);
			   /*if (ifAnyRows == 0) {
				 fail (" DB FAILURE: no rows retrieved even after txn commit.");
				 }*/
			   // after jdbc commit cache value in region1 for key1 must retain...
			   str = jtaObj.get ("key1");
			   tok = jtaObj.parseGetValue (str);
			   assertEquals ("cache put didn't commit, value retrieved is: "+tok, "\"value1\"", tok);
		  }
		  catch (CacheExistsException e) {
			   fail (" test 3 failed ");
			   ta.rollback();
		  }
		  catch (NamingException e){
			   fail (" test 3 failed " + e.getMessage());
			   if (rollback_chances)
					ta.rollback();
		  }
		  catch (SQLException e) {
			   fail (" test 3 failed " + e.getMessage());
			   if (rollback_chances)
					ta.rollback();
		  }
		  catch(Exception e){
			   fail (" test 3 failed " + e.getMessage());
			   if (rollback_chances)
					ta.rollback();
		  }
		  finally
		  {
			   if (conn != null)
					try {
						 conn.close ();
					}
			   catch (SQLException e) {}
		  }
	 }

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
	 public void testScenario4() throws Exception {

		  if (!this.to_continue) {
			   System.out.println(" FAILED");
			   fail (" test failed");
			   return;
		  }

		  this.tblIDFld = 4;
		  this.tblNameFld = "test4";
		  boolean rollback_chances = true;
		  final String DEFAULT_RGN = "root";

		  JTAUtils jtaObj = new JTAUtils (CacheTest.cache, CacheTest.currRegion);

		  jtaObj.deleteRows(this.tblName);

		  Context ctx = cache.getJNDIContext();
		  UserTransaction ta = null;
		  Connection conn = null;
		  try {
			   ta = (UserTransaction)ctx.lookup("java:/UserTransaction");

		  } catch (NamingException e) {
			   fail (" user txn lookup failed " + e.getMessage ());
		  }

		  try{
			   String current_region = jtaObj.currRegion.getName ();
			   assertEquals ("default region is not root", DEFAULT_RGN, current_region);

			   jtaObj.getRegionFromCache ("region1");
			   String current_fullpath = jtaObj.currRegion.getFullPath();
			   assertEquals ("failed retrieving current region fullpath after doing getRegionFromCache(region1)", "/" + DEFAULT_RGN + "/region1", current_fullpath);

			   jtaObj.put ("key1", "test");
			   ta.begin();

			   jtaObj.put ("key1", "value1");
			   String str = jtaObj.get ("key1");
			   String tok = jtaObj.parseGetValue (str);
			   assertEquals ("get value do not match with the put", "\"value1\"", tok);

			   current_fullpath = jtaObj.currRegion.getFullPath();
			   assertEquals ("failed retrieving current region fullpath", "/" + DEFAULT_RGN + "/region1", current_fullpath);

			   DataSource da = (DataSource)ctx.lookup("java:/XAPooledDataSource");

			   conn = da.getConnection();
			   Statement stmt = conn.createStatement();

			   String sqlSTR = "insert into " + this.tblName + " values (" + this.tblIDFld + "," + "'" + this.tblNameFld + "'" + ")" ;
			   stmt.executeUpdate(sqlSTR);

			   stmt.close();
			   ta.rollback ();
			   conn.close();

			   rollback_chances = false;

			   current_fullpath = jtaObj.currRegion.getFullPath();
			   assertEquals ("failed retirieving current region fullpath after txn rollback", "/" + DEFAULT_RGN + "/region1", current_fullpath);

			   int ifAnyRows = jtaObj.getRows(this.tblName);
			   assertEquals ("rows retrieved is: "+ifAnyRows, 0, ifAnyRows);
			   /*if (ifAnyRows != 0) {
				 fail (" DB FAILURE");
				 }*/
			   str = jtaObj.get ("key1");
			   tok = jtaObj.parseGetValue (str);
			   assertEquals ("value existing in cache is: "+tok,"\"test\"", tok);
		  }
		  catch (CacheExistsException e) {
			   ta.rollback();
			   fail (" failed " + e.getMessage ());
		  }
		  catch (NamingException e){
			   if (rollback_chances)
					ta.rollback();
			   fail (" failed " + e.getMessage ());
		  }
		  catch (SQLException e) {
			   if (rollback_chances)
					ta.rollback();
			   fail (" failed " + e.getMessage ());
		  }
		  catch(Exception e){
			   if (rollback_chances)
					ta.rollback();
			   fail (" failed " + e.getMessage ());
		  }
		  finally
		  {
			   if (conn != null)
					try {
						 conn.close ();
					}
			   catch (SQLException e) {}
		  }
	 }

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
	 public void testScenario5() throws Exception {
		  if (! this.to_continue) {
			   fail ("test 5 FAILED");
			   return;
		  }

		  this.tblIDFld = 5;
		  this.tblNameFld = "test5";
		  boolean rollback_chances = false;
		  final String DEFAULT_RGN = "root";

		  JTAUtils jtaObj = new JTAUtils (CacheTest.cache, CacheTest.currRegion);

		  jtaObj.deleteRows(this.tblName);

		  Context ctx = cache.getJNDIContext();
		  UserTransaction ta = null;
		  Connection conn = null;
		  try {
			   ta = (UserTransaction)ctx.lookup("java:/UserTransaction");
		  } catch (NamingException e) {
			   fail (" failed in user txn lookup " + e.getMessage ());
		  }

		  try{
			   String current_region = jtaObj.currRegion.getName ();
			   assertEquals ("default region is not root", DEFAULT_RGN, current_region);

			   // trying to create a region 'region1' under /root, if it doesn't exist. And point to the new region...
			   jtaObj.getRegionFromCache ("region1");

			   // now current region should point to region1, as done from within getRegionFromCache method...
			   String current_fullpath = jtaObj.currRegion.getFullPath ();
			   assertEquals ("failed retirieving current fullpath", "/" + DEFAULT_RGN + "/region1", current_fullpath);


			   jtaObj.put ("key1", "test");
			   ta.begin();

			   jtaObj.put ("key1", "value1");


			   String str = jtaObj.get ("key1");
			   String tok = jtaObj.parseGetValue (str);
			   assertEquals ("get value mismatch with put", "\"value1\"", tok);

			   current_fullpath = jtaObj.currRegion.getFullPath();
			   assertEquals ("failed retrieving current fullpath, current fullpath: "+current_fullpath, "/" + DEFAULT_RGN + "/region1", current_fullpath);

			   DataSource da = (DataSource)ctx.lookup("java:/XAPooledDataSource");

			   conn = da.getConnection();
			   Statement stmt = conn.createStatement();

			   String sqlSTR = "insert into " + this.tblName + " values (" + this.tblIDFld + "," + "'" + this.tblNameFld + "'" + ")";
			   stmt.executeUpdate(sqlSTR);

			   rollback_chances = true;

			   // capable of throwing SQLException during insert operation
			   sqlSTR = "insert into " + this.tblName + " values (" + this.tblIDFld + "," + "'" + this.tblNameFld + "'" + ")";
			   stmt.executeUpdate(sqlSTR);

			   stmt.close();
			   ta.commit ();
			   conn.close();
		  }
		  catch (CacheExistsException e) {
			   fail ("test failed " + e.getMessage ());
			   ta.rollback();
		  }
		  catch (NamingException e){
			   fail ("test failed " + e.getMessage ());
			   ta.rollback();
		  }
		  catch (SQLException e) {
			   // exception thrown during inserts are handeled as below by rollback
			   if (rollback_chances) {
					try {
						 ta.rollback();
					} catch (Exception ex) { fail ("failed: " + ex.getMessage ()); }

					// intended error is checked w.r.t database first
					int ifAnyRows = 0;
					try {
						 ifAnyRows = jtaObj.getRows(this.tblName);
					} catch (Exception ex) {
						 fail(" failed: " + ex.getMessage ());
					}
					assertEquals ("rows found after rollback is: "+ifAnyRows, 0, ifAnyRows);
					/*if (ifAnyRows != 0) {
					  fail (" DB FAILURE");
					  }*/
					// intended error is checked w.r.t. cache second
					String current_fullpath = jtaObj.currRegion.getFullPath();
					assertEquals ("failed retrieving current fullpath after rollback, fullpath is: "+current_fullpath, "/" + DEFAULT_RGN + "/region1", current_fullpath);

					// after jdbc rollback, cache value in region1 for key1 must vanish...
					String str1 = null;
					try {
						 str1 = jtaObj.get ("key1");
					}
					catch (CacheException ex) { fail ("failed getting value for 'key1': "+ex.getMessage()); }

					String tok1 = jtaObj.parseGetValue (str1);
					assertEquals ("value found in cache: "+tok1+", after rollback", "\"test\"", tok1);
			   }
			   else {
					fail (" failed: " + e.getMessage ());
					ta.rollback();
			   }
		  }
		  catch(Exception e)
		  {
			   fail (" failed: " + e.getMessage ());
			   ta.rollback();
		  }
		  finally
		  {
			   if (conn != null)
					try {
						 conn.close ();
					}
			   catch (SQLException ex) { fail ("Exception: "+ ex.getMessage ()); }
		  }
	 }

	 /**
	  * Test of testScenario7 method, of class com.gemstone.gemfire.internal.jta.functional.CacheTest1.
	  * Tests a user transaction with cache lookup and with XAPooledDataSource lookup. It does get and
	  * put operations on the cache within the transaction block and checks whether these operations are
	  * committed once the transaction is committed.
	  */
	 public void testScenario7 () throws Exception {
		  if (!this.to_continue) {
			   fail (" test 7 failed ");
			   return;
		  }

		  this.tblIDFld = 7;
		  this.tblNameFld = "test7";
		  boolean rollback_chances = true;
		  final String DEFAULT_RGN = "root";

		  JTAUtils jtaObj = new JTAUtils (CacheTest.cache, CacheTest.currRegion);

		  jtaObj.deleteRows(this.tblName);

		  Context ctx = cache.getJNDIContext();
		  UserTransaction ta = null;
		  Connection conn = null;
		  try {
			   ta = (UserTransaction)ctx.lookup("java:/UserTransaction");
		  } catch (NamingException e) {
			   fail (" user txn lookup failed: " + e.getMessage ());
		  }
		  try{
			   ta.begin();

			   String current_region = jtaObj.currRegion.getName();
			   assertEquals ("default region is not root", DEFAULT_RGN, current_region);

			   jtaObj.getRegionFromCache ("region1");

			   // now current region should point to region1, as done from within getRegionFromCache method...
			   String current_fullpath = jtaObj.currRegion.getFullPath();
			   assertEquals ("failed retrieving the current region fullpath", "/" + DEFAULT_RGN + "/region1", current_fullpath);

			   jtaObj.put ("key1", "value1");

			   // try to get value1 from key1 in the same region
			   String str = jtaObj.get ("key1");
			   String tok = jtaObj.parseGetValue (str);
			   assertEquals ("get value mismatch with put", "\"value1\"", tok);

			   current_fullpath = jtaObj.currRegion.getFullPath ();
			   assertEquals ("failed retrieving current region fullpath", "/" + DEFAULT_RGN + "/region1", current_fullpath);

			   DataSource da = (DataSource)ctx.lookup("java:/XAPooledDataSource");

			   conn = da.getConnection();
			   ta.commit ();
			   conn.close();

			   rollback_chances = false;

			   current_fullpath = jtaObj.currRegion.getFullPath ();
			   assertEquals ("failed retrieving current region fullpath after txn commit, fullpath is: "+current_region, "/" + DEFAULT_RGN + "/region1", current_fullpath);

			   str = jtaObj.get ("key1");
			   tok = jtaObj.parseGetValue (str);
			   assertEquals ("cache value found is: " + tok, "\"value1\"", tok);

		  }
		  catch (CacheExistsException e) {
			   fail (" failed due to: " + e.getMessage());
			   if (rollback_chances)
					ta.rollback();
		  }
		  catch (NamingException e){
			   fail (" failed due to: " + e.getMessage());
			   ta.rollback();
		  }
		  catch (SQLException e) {
			   fail (" failed due to: " + e.getMessage());
			   ta.rollback();
		  }
		  catch(Exception e)
		  {
			   fail (" failed due to: " + e.getMessage());
			   if (rollback_chances)
					ta.rollback();
		  }
		  finally
		  {
			   if (conn != null)
					try {
						 conn.close ();
					}
			   catch (Exception e) {}
		  }
	 }

	 /**
	  * Test of testScenario9 method, of class com.gemstone.gemfire.internal.jta.functional.CacheTest1.
	  * Tests a user transaction with cache lookup and with XAPooledDataSource lookup performing a db
	  * update within the transaction block and committing the transaction. Then agin we perform another
	  * db update with the same DataSource and finally close the Connection. The first update only should
	  * be committed in db and the second will not participate in transaction throwing an SQLException.
	  */
	 public void testScenario9() throws Exception {

		  if (!this.to_continue) {
			   fail (" test 9 FAILED ");
			   return;
		  }

		  this.tblIDFld = 9;
		  this.tblNameFld = "test9";
		  boolean rollback_chances = true;
		  int first_field = this.tblIDFld;

		  JTAUtils jtaObj = new JTAUtils (CacheTest.cache, CacheTest.currRegion);

		  // delete the rows inserted from CacheUtils createTable, otherwise conflict in PK's
		  jtaObj.deleteRows(this.tblName);

		  Context ctx = cache.getJNDIContext();
		  UserTransaction ta = null;
		  Connection conn = null;
		  try {
			   ta = (UserTransaction)ctx.lookup("java:/UserTransaction");
		  } catch (NamingException e) {
			   fail (" user txn lookup failed " + e.getMessage ());
		  }

		  try{
			   ta.begin();

			   DataSource da = (DataSource)ctx.lookup("java:/XAPooledDataSource");
			   conn = da.getConnection();
			   Statement stmt = conn.createStatement();

			   String sqlSTR = "insert into " + this.tblName + " values (" + this.tblIDFld + "," + "'" + this.tblNameFld + "'" + ")";
			   stmt.executeUpdate(sqlSTR);
			   ta.commit ();

			   rollback_chances = false;

			   // intended test for failure-- capable of throwing SQLException
			   this.tblIDFld += 1;
			   sqlSTR = "insert into " + this.tblName + " values (" + this.tblIDFld + "," + "'" + this.tblNameFld + "'" + ")";
			   stmt.executeUpdate(sqlSTR);

			   stmt.close();
			   conn.close();

			   rollback_chances = true;
		  }
		  catch (NamingException e){
			   fail (" failed due to: " + e.getMessage());
			   ta.rollback();
		  }
		  catch (SQLException e) {
			   if (!rollback_chances) {
					int ifAnyRows = jtaObj.getRows (this.tblName);
					assertEquals ("rows found is: "+ifAnyRows, 1, ifAnyRows);
					boolean matched = jtaObj.checkTableAgainstData (this.tblName, first_field + ""); // first field must be there.
					assertEquals ("first entry to db is not found", true, matched);
			   }
			   else {
					ta.rollback ();
					fail (" failed due to: " + e.getMessage());
			   }
		  }
		  catch(Exception e)
		  {
			   fail (" failed due to: " + e.getMessage());
			   if (rollback_chances) {
					ta.rollback ();
			   }
		  }
		  finally
		  {
			   if (conn != null)
					try {
						 conn.close ();
					}
			   catch (SQLException e) {}
		  }
	 }

	 /**
	  * Test of testScenario10 method, of class com.gemstone.gemfire.internal.jta.functional.CacheTest1.
	  * Test a user transaction with cache lookup and with XAPooledDataSource for multiple JDBC Connections.
	  * This should not be allowed for Facets datasource. For other relational DB the behaviour will be DB specific. 
	  * For Oracle DB, n connections in a tranxn can be used provided , n-1 connections are closed before 
	  * opening nth connection.
	  */
	 public void testScenario10() throws Exception {
		  if (!this.to_continue) {
			   fail ("test 10  FAILED");
			   return;
		  }

		  this.tblIDFld = 10;
		  this.tblNameFld = "test10";
		  int rows_inserted = 0;
		  int ifAnyRows = 0;
		  int field1 = this.tblIDFld, field2 = 0;

		  JTAUtils jtaObj = new JTAUtils (CacheTest.cache, CacheTest.currRegion);

		  jtaObj.deleteRows(this.tblName);

		  Context ctx = cache.getJNDIContext();
		  UserTransaction ta = null;
		  Connection conn1 = null;
		  Connection conn2 = null;
		  try {
			   ta = (UserTransaction)ctx.lookup("java:/UserTransaction");

		  } catch (NamingException e) {
			   fail (" user lookup failed: " + e.getMessage ());
		  }

		  try{
			   ta.begin();
			   DataSource da = (DataSource)ctx.lookup("java:/XAPooledDataSource");
			   conn1 = da.getConnection();                     // the first Connection

			   Statement stmt = conn1.createStatement();

			   String sqlSTR = "insert into " + this.tblName + " values (" + this.tblIDFld + "," + "'" + this.tblNameFld + "'" + ")";
			   stmt.executeUpdate(sqlSTR);
			   rows_inserted += 1;

			   conn1.close();

			   conn2 = da.getConnection();                     // the second Connection
			   stmt = conn2.createStatement();

			   this.tblIDFld += 1;
			   field2 = this.tblIDFld;
			   sqlSTR = "insert into " + this.tblName + " values (" + this.tblIDFld + "," + "'" + this.tblNameFld + "'" + ")";

			   stmt.executeUpdate(sqlSTR);
			   rows_inserted += 1;

			   stmt.close();
			   conn2.close();
			   ta.commit ();

			   // if we reach here check for proper entries in db
			   ifAnyRows = jtaObj.getRows (this.tblName);
			   if (ifAnyRows == rows_inserted) {
					boolean matched1 = jtaObj.checkTableAgainstData (this.tblName, (field1 + ""));
					boolean matched2 = jtaObj.checkTableAgainstData (this.tblName, (field2 + ""));

					if (matched1)
						 System.out.print ("(PK " + field1 + "found ");
					else
						 System.out.print ("(PK " + field1 + "not found ");
					if (matched2)
						 System.out.print ("PK " + field2 + "found)");
					else
						 System.out.print ("PK " + field2 + "not found)");

					if (matched1 & matched2) {
						 System.out.println ( "ok");
					}
					else {
						 fail (" inserted data not found in DB !... failed");
					}
			   }
			   else {
					fail (" test interrupted, rows found=" + ifAnyRows + ", rows inserted=" + rows_inserted);
			   }
		  }
		  catch (NamingException e){
			   ta.rollback();
		  }
		  catch (SQLException e) {
			   ta.rollback();
		  }
		  catch(Exception e)
		  {
			   ta.rollback();
		  }
		  finally
		  {
			   if (conn1 != null)
					try {
						 conn1.close ();
					}
			   catch (SQLException e) {}

			   if (conn2 != null)
					try {
						 conn2.close ();
					}
			   catch (SQLException e) {}
		  }

	 }

	 /**
	  * Test of testScenario11 method, of class com.gemstone.gemfire.internal.jta.functional.CacheTest1.
	  * Tests Simple DataSource lookup within a transaction. Things should not participate in the transaction.
	  */
	 public void testScenario11() throws Exception {
		  if (!this.to_continue) {
			   fail ("test 11 FAILED");
			   return;
		  }

		  this.tblIDFld = 11;
		  this.tblNameFld = "test11";
		  boolean rollback_chances = false;

		  JTAUtils jtaObj = new JTAUtils (CacheTest.cache, CacheTest.currRegion);

		  jtaObj.deleteRows(this.tblName);

		  Context ctx = cache.getJNDIContext();
		  UserTransaction ta = null;
		  Connection conn = null;
		  try {
			   ta = (UserTransaction)ctx.lookup("java:/UserTransaction");

		  } catch (NamingException e) {
			   fail ("failed in user txn look up: " + e.getMessage ());
		  }

		  try{
			   ta.begin();
			   DataSource da = (DataSource)ctx.lookup("java:/SimpleDataSource");

			   conn = da.getConnection();
			   Statement stmt = conn.createStatement();

			   String sqlSTR = "insert into " + this.tblName + " values (" + this.tblIDFld + "," + "'" + this.tblNameFld + "'" + ")" ;
			   stmt.executeUpdate(sqlSTR);

			   rollback_chances = true;

			   // try insert the same data once more and see if all info gets rolled back...
			   // capable of throwing SQLException
			   sqlSTR = "insert into " + this.tblName + " values (" + this.tblIDFld + "," + "'" + this.tblNameFld + "'" + ")" ;
			   stmt.executeUpdate(sqlSTR);

			   stmt.close();
			   ta.commit ();
			   conn.close();
		  }
		  catch (NamingException e){
			   ta.rollback();
		  }
		  catch (SQLException e) {
			   if (rollback_chances) {
					try {
						 ta.rollback();
					} catch (Exception ex) {fail ("failed due to : " + ex.getMessage ()); }

					// try to check in the db whether any rows (the first one) are there now...
					int ifAnyRows = jtaObj.getRows(this.tblName);
					assertEquals ("first row not found in case of Simple Datasource", 1, ifAnyRows); // one row-- the first one shud be there.
					boolean matched = jtaObj.checkTableAgainstData (this.tblName, this.tblIDFld + ""); // checking the existence of first row
					assertEquals ("first row PK didn't matched", true, matched);
			   }
			   else {
					ta.rollback();
			   }

		  }
		  catch(Exception e){
			   fail (" failed due to: " + e.getMessage ());
			   ta.rollback();
		  }
		  finally
		  {
			   if (conn != null)
					try {
						 conn.close ();
					}
			   catch (SQLException e) {}
		  }

	 }


	 /**
	  * Test of testScenario14 method, of class com.gemstone.gemfire.internal.jta.functional.CacheTest1.
	  * Tests a local Cache Loader with XADataSource lookup to get the connection. The Connection should
	  * not participate in the transaction and commit/rollback should take affect accordingly along with
	  * cache.
	  */
	 // Please remove the prefix '_' from the method name to execute this particular test.
	 // This particular test fails as of now as the cache get operation retrieves value
	 // even after user transaction rollback.
	 public void testScenario14() throws Exception {
		  if (!this.to_continue) {
			   fail (" test 14 FAILED");
			   return;
		  }

		  final String TABLEID = "2";
//		  final String TABLEFLD = "name2";
		  JTAUtils jtaObj = new JTAUtils (CacheTest.cache, CacheTest.currRegion);

		  try{
			   Context ctx = cache.getJNDIContext();
			   UserTransaction utx = (UserTransaction)ctx.lookup("java:/UserTransaction");
			   utx.begin();
			   AttributesFactory fac = new AttributesFactory(currRegion.getAttributes());
			   fac.setCacheLoader(new XACacheLoaderTxn(this.tblName));
			   Region re = currRegion.createSubregion("employee",fac.create());
			   String retVal = (String) re.get(TABLEID); //TABLEID correspondes to "name1".
			   if(!retVal.equals("newname"))
				 fail("Uncommitted value 'newname' not read by cacheloader name = "+retVal);
			   utx.rollback();
			   
			   DataSource ds = (DataSource)ctx.lookup("java:/XAPooledDataSource");
			   Connection conn = ds.getConnection();
			   Statement stm = conn.createStatement();
			   String str = "select name from "+ tblName +"  where id= (2)";
			   ResultSet rs = stm.executeQuery(str);
			   rs.next();
			   String str1  = rs.getString(1);
			   if(!str1.equals("name2"))
			   	fail("Rollback not occured on XAConnection got in a cache loader");
		  }
		  catch(Exception e){
			   fail (" failed due to :" + e.getMessage ());
		  }
	 }

	 /**
	  * Test of testScenario15 method, of class com.gemstone.gemfire.internal.jta.functional.CacheTest1.
	  * Tests performing DDL operations by looking up a XAPooledDataSource and without transaction.
	  */
	 public void testScenario15() throws Exception {
		  if (!this.to_continue) {
			   fail (" test 15 FAILED");
			   return;
		  }

		  this.tblIDFld = 15;
		  this.tblNameFld = "test15";
		  String tbl = "";
		  boolean row_num = true;
		  int ddl_return = 1;

		  JTAUtils jtaObj = new JTAUtils (CacheTest.cache, CacheTest.currRegion);
		  // delete the rows inserted from CacheUtils createTable, otherwise conflict in PK's. Basically not needed for this test
		  jtaObj.deleteRows(this.tblName);

		  Context ctx = cache.getJNDIContext();
//		  UserTransaction ta = null;
		  Connection conn = null;
		  Statement stmt = null;
		  /*try {
			ta = (UserTransaction)ctx.lookup("java:/UserTransaction");

			} catch (NamingException e) {
			fail ("user txn lookup failed: " + e.getMessage ());
			}*/

		  try{
			   DataSource da = (DataSource)ctx.lookup("java:/XAPooledDataSource");
			   conn = da.getConnection();
			   stmt = conn.createStatement();

			   // Do some DDL stuff
			   String time = new Long(System.currentTimeMillis()).toString();
			   tbl = "my_table" + time;
			   //String sql = "create table " + tbl + " (my_id number primary key, my_name varchar2(50))";
			   String sql = "create table " + tbl + " (my_id integer NOT NULL, my_name varchar(50), CONSTRAINT my_keyx PRIMARY KEY(my_id))";
			   ddl_return = stmt.executeUpdate(sql);

			   // check whether the table was created properly
			   sql = "select * from " + tbl;
			   ResultSet rs = null;
			   rs = stmt.executeQuery (sql);
			   row_num = rs.next ();
			   rs.close ();
			   if (ddl_return == 0 && !row_num) {
					sql = "drop table "+tbl;
					try {
						 stmt = conn.createStatement();
						 stmt.executeUpdate(sql);
					}
					catch (SQLException e) {
						 fail (" failed to drop, " + e.getMessage ());
					}
			   }
			   else
					fail ("unable to create table");

			   /** Code meant for Oracle DB **/
			   /*tbl = tbl.toUpperCase();
				 sql = "select * from tab where tname like '%tbl%'";
				 ResultSet rs = null;
				 rs = stmt.executeQuery(sql);
				 rs.close();
				 sql = "drop table "+tbl;
				 stmt = conn.createStatement();
				 stmt.execute(sql);
				 System.out.println (this.space + "ok");
				*/
			   stmt.close();
			   conn.close ();

		  }
		  catch (NamingException e){
			   fail ("failed, " + e.getMessage ());
		  }
		  catch (SQLException e) {
			   fail ("failed, " + e.getMessage ());
		  }
		  catch(Exception e){
			   fail ("failed, " + e.getMessage ());
		  }
		  finally
		  {
			   if (conn != null)
					try {
						 conn.close ();
					}
			   catch (SQLException ex) { fail ("Exception: "+ex.getMessage()); }
		  }
	 }

	 /**
	  * Test of testScenario16 method, of class com.gemstone.gemfire.internal.jta.functional.CacheTest1.
	  * Tests DDL operations on XAPooledDataSOurce lookup, without transaction but making auto commit
	  * false just before performing DDL queries and making it true before closing the Connection. The
	  * operations should be committed.
	  */
	 public void testScenario16() throws Exception {
		  if (!this.to_continue) {
			   fail ("test16 FAILED");
			   return;
		  }

		  this.tblIDFld = 16;
		  this.tblNameFld = "test16";
		  String tbl = "";
		  int ddl_return = 1;
		  boolean row_num = true;

		  JTAUtils jtaObj = new JTAUtils (CacheTest.cache, CacheTest.currRegion);

		  jtaObj.deleteRows(this.tblName);

		  Context ctx = cache.getJNDIContext();
//		  UserTransaction ta = null;
		  Connection conn = null;
		  Statement stmt = null;
		  /*try {
			ta = (UserTransaction)ctx.lookup("java:/UserTransaction");

			} catch (NamingException e) {
			fail ("failed in User Txn lookup: " + e.getMessage ());
			}*/

		  try{
			   DataSource da = (DataSource)ctx.lookup("java:/XAPooledDataSource");
			   conn = da.getConnection();
			   conn.setAutoCommit(false);
			   stmt = conn.createStatement();

			   String time = new Long(System.currentTimeMillis()).toString();
			   tbl = "my_table" + time;
			   //String sql = "create table " + tbl + " (my_id number primary key, my_name varchar2(50))";
			   String sql = "create table " + tbl + " (my_id integer NOT NULL, my_name varchar(50), CONSTRAINT my_key PRIMARY KEY(my_id))";
			   ddl_return = stmt.executeUpdate(sql);

			   sql = "select * from " + tbl;
			   ResultSet rs = null;
			   rs = stmt.executeQuery (sql);
			   row_num = rs.next ();
			   rs.close ();
			   if (ddl_return == 0 && !row_num) {
					sql = "drop table "+tbl;
					try {
						 stmt = conn.createStatement();
						 stmt.executeUpdate(sql);
					}
					catch (SQLException e) {
						 fail (" failed to drop, " + e.getMessage ());
					}
			   }
			   else
					fail ("table do not exists");

			   /*** Code meant for Oracle DB ***/
			   /*tbl = tbl.toUpperCase();
				 sql = "select * from tab where tname like '%tbl%'";
				 ResultSet rs = null;
				 rs = stmt.executeQuery(sql);
				 rs.close();
				 sql = "drop table "+tbl;
				 stmt = conn.createStatement();
				 stmt.executeUpdate(sql);
				 System.out.println (this.space + "ok");
				*/
			   conn.setAutoCommit(true);
			   stmt.close();
			   conn.close ();
		  }
		  catch (NamingException e){
			   fail ("failed, " + e.getMessage ());
		  }
		  catch (SQLException e) {
			   fail ("failed, " + e.getMessage ());
		  }
		  catch(Exception e){
			   fail ("failed, " + e.getMessage ());
		  }
		  finally
		  {
			   if (conn != null)
					try {
						 conn.close ();
					}
			   catch (SQLException e) {}
		  }
	 }

	 /**
	  * Test of testScenario18 method, of class com.gemstone.gemfire.internal.jta.functional.CacheTest1.
	  * Get a connection (conn1) outside a transaction with pooled datasource lookup and another 
	  * connection (conn2) within the same transaction with XAPooled datasource lookup. 
	  * After making updates we do a rollback on the transaction and close both connections in the order
	  * of opening them. The connection opened within transaction will only participate in the transaction.
	  */
	 public void testScenario18 () throws Exception {
		  if (!this.to_continue) {
			   fail (" test 18 FAILED");
			   return;
		  }

		  this.tblIDFld = 18;
		  this.tblNameFld = "test18";
		  boolean rollback_chances = true;
		  int ifAnyRows = 0;

		  JTAUtils jtaObj = new JTAUtils (CacheTest.cache, CacheTest.currRegion);

		  jtaObj.deleteRows(this.tblName);

		  Context ctx = cache.getJNDIContext();

		  UserTransaction ta = null;
		  Connection conn1 = null;     // connection outside txn
		  Connection conn2 = null;     // connection within txn
		  Statement stmt = null;
		  DataSource da = null;
		  try {
			   da = (DataSource)ctx.lookup("java:/PooledDataSource");
			   conn1 = da.getConnection();
			   ta = (UserTransaction)ctx.lookup("java:/UserTransaction");
		  } catch (NamingException e) {
			   fail ("failed: " + e.getMessage ());
		  } catch (SQLException e) {
			   fail ("failed: " + e.getMessage ());
		  }

		  try {
			   ta.begin();
			   stmt = conn1.createStatement();
			   String sqlSTR = "insert into " + this.tblName + " values (" + this.tblIDFld + "," + "'" + this.tblNameFld + "'" + ")" ;
			   stmt.executeUpdate(sqlSTR);
			   stmt.close();

			   ifAnyRows = jtaObj.getRows(this.tblName);
			   if (ifAnyRows == 0) {
					fail ("failed no rows are there...");
			   }
			   // tryin a XAPooled lookup for second connection
			   da = (DataSource)ctx.lookup("java:/XAPooledDataSource");
			   conn2 = da.getConnection();
			   stmt = conn2.createStatement();

			   this.tblIDFld += 1;
			   sqlSTR = "insert into " + this.tblName + " values (" + this.tblIDFld + "," + "'" + this.tblNameFld + "'" + ")" ;
			   stmt.executeUpdate(sqlSTR);
			   ta.rollback ();  // intensional rollback

			   stmt.close();
			   conn2.close();
			   conn1.close();

			   rollback_chances = false;
			   // if we reach here check whether the rollback was success for conn1 and conn2
			   ifAnyRows = jtaObj.getRows(this.tblName);
			   assertEquals ("at least one row not retained after rollback", 1, ifAnyRows);

			   boolean matched = jtaObj.checkTableAgainstData (this.tblName, this.tblIDFld + ""); // checking conn2's field
			   if (matched) {        // data is still in db
					fail (", PK " + this.tblIDFld + " found in db)" + "   " + "rollback for conn #2 failed");
			   }
		  }
		  catch (Exception e) {
			   fail ("failed, " + e.getMessage ());
			   if (rollback_chances)
					ta.rollback();
		  }
		  finally {
			   if (conn1 != null)
					try {
						 conn1.close ();
					}
			   catch (SQLException ex) { fail (" Exception: " + ex.getMessage ());}
			   if (conn2 != null)
					try {
						 conn2.close ();
					}
			   catch (SQLException ex) { fail (" Exception: " + ex.getMessage ());}
		  }
	 }

	 // required for test # 14
	 class XACacheLoaderTxn implements CacheLoader {

		  String tableName;
		  /** Creates a new instance of XACacheLoaderTxn */
		  public XACacheLoaderTxn(String str) {
			   this.tableName = str;
		  }

		  public final Object load(LoaderHelper helper) throws CacheLoaderException {
			   System.out.println("In Loader.load for"+helper.getKey());
			   return loadFromDatabase(helper.getKey());
		  }

		  private Object loadFromDatabase(Object ob)
		  {
			   Object obj =  null;
			   try{
					Context ctx = CacheFactory.getAnyInstance().getJNDIContext();
					DataSource ds = (DataSource)ctx.lookup("java:/XAPooledDataSource");
					Connection conn = ds.getConnection();
					Statement stm = conn.createStatement();
					String str = "update "+ tableName +" set name ='newname' where id = ("+(new Integer(ob.toString())).intValue()+")";
					stm.executeUpdate(str);
					ResultSet rs = stm.executeQuery("select name from "+ tableName +" where id = ("+(new Integer(ob.toString())).intValue()+")");
					rs.next();
					obj = rs.getString(1);
					stm.close();
					conn.close();
					return obj;
			   }catch(Exception e){
					e.printStackTrace();
			   }
			   return obj;
		  }

		  public void close() {
			   // TODO Auto-generated method stub
		  }
	 } // end of XACacheLoaderTxn class #



}// end of CacheTest class#
