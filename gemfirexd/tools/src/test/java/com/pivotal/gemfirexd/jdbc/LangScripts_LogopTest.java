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

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.derbyTesting.junit.JDBC;

import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.engine.Misc;

import junit.framework.TestSuite;
import junit.textui.TestRunner;

public class LangScripts_LogopTest extends JdbcTestBase {

  public static void main(String[] args) {
    TestRunner.run(new TestSuite(LangScripts_LogopTest.class));
  }
  
  public LangScripts_LogopTest(String name) {
    super(name); 
  }

  // This test is the as-is LangScript conversion, without any partitioning clauses
  public void testLangScript_LogopTestNoPartitioning() throws Exception
  {
    // This is a JUnit conversion of the Derby Lang Logop.sql script
    // without any GemFireXD extensions
	  
    // Catch exceptions from illegal syntax
    // Tests still not fixed marked FIXME
	  
    // Array of SQL text to execute and sqlstates to expect
    // The first object is a String, the second is either 
    // 1) null - this means query returns no rows and throws no exceptions
    // 2) a string - this means query returns no rows and throws expected SQLSTATE
    // 3) a String array - this means query returns rows which must match (unordered) given resultset
    //       - for an empty result set, an uninitialized size [0][0] array is used
    Object[][] Script_LogopUT = {
	// this test is for logical operators (AND, OR, etc.)
	{ "create table t (x int, y int)", null },
	{ "insert into t values (1, 1)", null },
	{ "insert into t values (1, 2)", null },
	{ "insert into t values (2, 1)", null },
	{ "insert into t values (2, 2)", null },
	{ "insert into t values (null, 2)", null },
	{ "insert into t values (1, null)", null },
	{ "insert into t values (null, null)", null },
	{ "select x, y from t where x = 1 and y = 2", new String[][] { {"1","2"} } },
	{ "select x, y from t where y = 2 and x = 1", new String[][] { {"1","2"} } },
	{ "select x, y from t where x = 1 and y = 3", new String[0][0] },
	{ "select x, y from t where y = 3 and x = 1", new String[0][0] },
	{ "create table s (x int)", null },
	{ "insert into s values (1)", null },
	{ "select x from s where x = 5 and 2147483647 + 10 = 2", "22003" },
	{ "select x from s where 2147483647 + 10 = 2 and x = 5", "22003" },
	{ "select x, y from t where x = 1 and x + 0 = 1 and y = 2 and y + 0 = 2", new String[][] { {"1","2"} } },
	{ "select x, y from t where x = 1 or y = 2", new String[][] {
		{"1","1"},{"1","2"},{"2","2"},{null,"2"},{"1",null} } },
	{ "select x, y from t where y = 2 or x = 1", new String[][] {
		{"1","1"},{"1","2"},{"2","2"},{null,"2"},{"1",null} } },
	{ "select x, y from t where x = 4 or y = 5", new String[0][0] },
	{ "select x, y from t where y = 5 or x = 4", new String[0][0] },
	{ "select x from s where x = 1 or 2147483647 + 10 = 2", new String[][] { {"1"} } },
	{ "select x from s where 2147483647 + 10 = 2 or x = 1", "22003" },
	{ "select x, y from t where x = 1 or x + 0 = 1 or y = 2 or y + 0 = 2", new String[][] {
		{ "1","1"},{"1","2"},{"2","2"},{null,"2"},{"1",null} } },
	{ "select x from s where (1 = 1) or (2 = 2) and (3 = 4)", new String[][] { {"1"} } },
	{ "select x from s where (1 = 2) and (3 = 3) or (4 = 4)", new String[][] { {"1"} } },
	{ "select x from s where ( (1 = 1) or (2 = 2) ) and (3 = 4)", new String[0][0] },
	{ "select x from s where (1 = 2) and ( (3 = 3) or (4 = 4) )", new String[0][0] },
	// Test layers and layers of AND/OR mix
	{ "select * from s where (	( ( ((1=1) and (1=1)) and ((1=1) and (1=1)) ) and  ( ((1=1) and (1=1)) and ((1=1) and (1=1)) ) ) and ( ( ((1=1) and (1=1)) and ((1=1) and (1=1)) ) and ( ((1=1) and (1=1)) and ((1=1) and (1=1)) ) ) )", new String[][] { {"1"} } },
	{ "select * from s where (	( ( ((1=1) or (1=1)) or ((1=1) or (1=1)) ) or ( ((1=1) or (1=1)) or ((1=1) or (1=1)) ) ) or ( ( ((1=1) or (1=1)) or ((1=1) or (1=1)) ) or ( ((1=1) or (1=1)) or ((1=1) or (1=1)) ) ) )", new String[][] { {"1"} } },
	{ "select * from s where (	( ( ((1=1) and (1=1)) or ((1=1) and (1=1)) ) or ( ((1=1) and (1=1)) or ((1=1) and (1=1)) ) ) or ( ( ((1=1) and (1=1)) or ((1=1) and (1=1)) ) or ( ((1=1) and (1=1)) or ((1=1) and (1=1)) ) ) )", new String[][] { {"1"} } },
	{ "select * from s where ( ( ( ((1=1) or (1=1)) and ((1=1) or (1=1)) ) and ( ((1=1) or (1=1)) and ((1=1) or (1=1)) ) ) and ( ( ((1=1) or (1=1)) and ((1=1) or (1=1)) ) and ( ((1=1) or (1=1)) and ((1=1) or (1=1)) ) ) )", new String[][] { {"1"} } },
	{ "select * from s where ( ( ( ( ( ((1=1) and (1=1)) and (1=1)) and (1=1)) and (1=1))	and (1=1)) and (1=1))", new String[][] { {"1"} } },
	{ "select * from s where ( ( ( ( ( ((1=1) or (1=1)) or (1=1)) or (1=1)) or (1=1)) or (1=1)) or (1=1))", new String[][] { {"1"} } },
	{ "select * from s where ( ( ( ( ( ((1=1) or (1=2)) or (1=2)) or (1=2)) or (1=2)) or (1=2)) or (1=2))", new String[][] { {"1"} } },
	{ "select * from s where ( ( ( ( ( ((1=2) or (1=2)) or (1=1)) or (1=2)) or (1=2)) or (1=2)) or (1=2))", new String[][] { {"1"} } },
	{ "select * from s where ( ( ( ( ( ((1=2) or (1=2)) or (1=2)) or (1=1)) or (1=2)) or (1=2)) or (1=2))", new String[][] { {"1"} } },
	{ "select * from s where ( ( ( ( ( ((1=2) or (1=2)) or (1=2)) or (1=2)) or (1=1)) or (1=2)) or (1=2))", new String[][] { {"1"} } },
	{ "select * from s where ( ( ( ( ( ((1=2) or (1=2)) or (1=2)) or (1=2)) or (1=2)) or (1=1)) or (1=2))", new String[][] { {"1"} } },
	{ "select * from s where ( ( ( ( ( ((1=2) or (1=2)) or (1=2)) or (1=2)) or (1=2)) or (1=2)) or (1=1))", new String[][] { {"1"} } },
	{ "select * from s where ( ( ( ( ( (1=1) or ((1=2) or (1=2)) ) or (1=2)) or (1=2)) or (1=2)) or (1=2))", new String[][] { {"1"} } },
	{ "select * from s where ( ( ( ( ( (1=2) or ((1=1) or (1=2)) ) or (1=2)) or (1=2)) or (1=2)) or (1=2))", new String[][] { {"1"} } },
	{ "select * from s where ( ( ( ( ( (1=2) or ((1=2) or (1=1)) ) or (1=2)) or (1=2)) or (1=2)) or (1=2))", new String[][] { {"1"} } },
	{ "select * from s where ( ( ( ( ( (1=2) or ((1=2) or (1=2)) ) or (1=1)) or (1=2)) or (1=2)) or (1=2))", new String[][] { {"1"} } },
	{ "select * from s where ( ( ( ( ( (1=2) or ((1=2) or (1=2)) ) or (1=2)) or (1=1)) or (1=2)) or (1=2))", new String[][] { {"1"} } },
	{ "select * from s where ( ( ( ( ( (1=2) or ((1=2) or (1=2)) ) or (1=2)) or (1=2)) or (1=1)) or (1=2))", new String[][] { {"1"} } },
	{ "select * from s where ( ( ( ( ( (1=2) or ((1=2) or (1=2)) ) or (1=2)) or (1=2)) or (1=2)) or (1=1))", new String[][] { {"1"} } },
	{ "select x from s where 2147483647 + 10 = 2 and (1=2)", "22003" },
	{ "select x from s where (1=2) and 2147483647 + 10 = 2", new String[0][0] },
	{ "select x from s where not ( (1 = 1) or (2 = 2) ) and (3 = 4)", new String[0][0] },
	{ "select x from s where not ( ( (1 = 1) or (2 = 2) ) and (3 = 4) )", new String[][] { {"1"} } },
	{ "select x from s where (1 = 2) and not ( (3 = 3) or (4 = 4) )", new String[0][0] },
	{ "select x from s where not ( (1 = 2) and ( (3 = 3) or (4 = 4) ) )", new String[][] { {"1"} } },
	{ "select ( not ( (1 = 1) or (2 = 2) ) and (3 = 4) ) from s", "42X01" },
	{ "select ( not ( ( (1 = 1) or (2 = 2) ) and (3 = 4) ) ) from s", "42X01" },
	{ "select ( (1 = 2) and not ( (3 = 3) or (4 = 4) ) ) from s", "42X01" },
	{ "select ( not ( (1 = 2) and ( (3 = 3) or (4 = 4) ) ) ) from s", "42X01" },
	{ "select * from s where not (	( ( ((1=1) and (1=1)) and ((1=1) and (1=1)) ) and ( ((1=1) and (1=1)) and ((1=1) and (1=1)) ) ) and ( ( ((1=1) and (1=1)) and ((1=1) and (1=1)) ) and ( ((1=1) and (1=1)) and ((1=1) and (1=2)) ) ) )", new String[][] { {"1"} } },
	{ "select * from s where not (	( ( ((1=1) or (1=1)) or ((1=1) or (1=1)) ) or ( ((1=1) or (1=1)) or ((1=1) or (1=1)) ) ) or ( ( ((1=1) or (1=1)) or ((1=1) or (1=1)) ) or ( ((1=1) or (1=1)) or ((1=1) or (1=2)) ) ) )", new String[0][0] },
	{ "select * from s where not (	( ( ((1=1) and (1=1)) or ((1=1) and (1=1)) ) or ( ((1=1) and (1=1)) or ((1=1) and (1=1)) ) ) or ( ( ((1=1) and (1=1)) or ((1=1) and (1=1)) ) or ( ((1=1) and (1=1)) or ((1=1) and (1=2)) ) ) )", new String[0][0] },
	{ "select * from s where not ( ( ( ((1=1) or (1=1)) and ((1=1) or (1=1)) ) and ( ((1=1) or (1=1)) and ((1=1) or (1=1)) ) ) and ( ( ((1=1) or (1=1)) and ((1=1) or (1=1)) ) and ( ((1=1) or (1=1)) and ((1=1) or (1=2)) ) ) )", new String[0][0] },
	{ "select * from s where not ( ( ( ( ( ((1=1) and (1=2)) and (1=1)) and (1=1)) and (1=1)) and (1=1)) and (1=1))", new String[][] { {"1"} } },
	{ "select * from s where not ( ( ( ( ( ((1=2) or (1=1)) or (1=1)) or (1=1)) or (1=1)) or (1=1)) or (1=1))", new String[0][0] },
	{ "select * from s where not ( ( ( ( ( ((1=2) or (1=2)) or (1=2)) or (1=2)) or (1=2)) or (1=2)) or (1=2))", new String[][] { {"1"} } },
	{ "select * from s where not ( ( ( ( ( ((1=2) or (1=2)) or (1=1)) or (1=2)) or (1=2)) or (1=2)) or (1=2))", new String[0][0] },
	{ "select * from s where not ( ( ( ( ( ((1=2) or (1=2)) or (1=2)) or (1=1)) or (1=2)) or (1=2)) or (1=2))", new String[0][0] },
	{ "select * from s where not ( ( ( ( ( ((1=2) or (1=2)) or (1=2)) or (1=2)) or (1=1)) or (1=2)) or (1=2))", new String[0][0] },
	{ "select * from s where not ( ( ( ( ( ((1=2) or (1=2)) or (1=2)) or (1=2)) or (1=2)) or (1=1)) or (1=2))", new String[0][0] },
	{ "select * from s where not ( ( ( ( ( ((1=2) or (1=2)) or (1=2)) or (1=2)) or (1=2)) or (1=2)) or (1=1))", new String[0][0] },
	{ "select * from s where not ( ( ( ( ( (1=1) or ((1=2) or (1=2)) ) or (1=2)) or (1=2)) or (1=2)) or (1=2))", new String[0][0] },
	{ "select * from s where not ( ( ( ( ( (1=2) or ((1=2) or (1=2)) ) or (1=2)) or (1=2)) or (1=2)) or (1=2))", new String[][] { {"1"} } },
	{ "select * from s where not ( ( ( ( ( (1=2) or ((1=2) or (1=1)) ) or (1=2)) or (1=2)) or (1=2)) or (1=2))", new String[0][0] },
	{ "select * from s where not ( ( ( ( ( (1=2) or ((1=2) or (1=2)) ) or (1=1)) or (1=2)) or (1=2)) or (1=2))", new String[0][0] },
	{ "select * from s where not ( ( ( ( ( (1=2) or ((1=2) or (1=2)) ) or (1=2)) or (1=1)) or (1=2)) or (1=2))", new String[0][0] },
	{ "select * from s where not ( ( ( ( ( (1=2) or ((1=2) or (1=2)) ) or (1=2)) or (1=2)) or (1=1)) or (1=2))", new String[0][0] },
	{ "select * from s where not ( ( ( ( ( (1=2) or ((1=2) or (1=2)) ) or (1=2)) or (1=2)) or (1=2)) or (1=1))", new String[0][0] },
	{ "select * from s where not ( not ( not ((1=2) or (1=1))) or (not ((1=2) or (1=2)) ) )", new String[0][0] },
	{ "select not ( not ( not ((1=2) or (1=1))) or (not ((1=2) or (1=2)) ) ) from s", "42X01" },
	{ "select * from s where 1", "42X19" },
	{ "select * from s where 1 and (1=1)", "42Y94" },
	{ "select * from s where (1=1) and 1", "42Y94" },
	{ "select * from s where 1 or (1=1)", "42Y94" },
	{ "select * from s where (1=1) or 1", "42Y94" },
	{ "select * from s where not 1", "42X40" },
	{ "drop table t", null },
	{ "drop table s", null }
    };

    // Do not use partitioning as default, use replicate
    // (Some results are expected to be different with partitioning)
    skipDefaultPartitioned = true;

    Connection conn = TestUtil.getConnection();
    Statement stmt = conn.createStatement();
    // Go through the array, execute each string[0], check sqlstate [1]
    // This will fail on the first one that succeeds where it shouldn't
    // or throws unknown exception
    JDBC.SQLUnitTestHelper(stmt,Script_LogopUT);
  }
  
  // This test is the script enhanced with partitioning
  public void testLangScript_LogopWithPartitioning() throws Exception
  {
    // This form of the Logop.sql test has partitioning activated
    // TODO : Create specialized partitioning for these tables for Logop
    //  Currently, default partitioning is active
    Object[][] Script_LogopUTPartitioning = {
	// this test is for logical operators (AND, OR, etc.)
	{ "create table t (x int, y int) partition by column(x)", null },
	{ "insert into t values (1, 1)", null },
	{ "insert into t values (1, 2)", null },
	{ "insert into t values (2, 1)", null },
	{ "insert into t values (2, 2)", null },
	{ "insert into t values (null, 2)", null },
	{ "insert into t values (1, null)", null },
	{ "insert into t values (null, null)", null },
	{ "select x, y from t where x = 1 and y = 2", new String[][] { {"1","2"} } },
	{ "select x, y from t where y = 2 and x = 1", new String[][] { {"1","2"} } },
	{ "select x, y from t where x = 1 and y = 3", new String[0][0] },
	{ "select x, y from t where y = 3 and x = 1", new String[0][0] },
	{ "create table s (x int) partition by column(x)", null },
	{ "insert into s values (1)", null },
	{ "select x from s where x = 5 and 2147483647 + 10 = 2", "22003" },
	{ "select x from s where 2147483647 + 10 = 2 and x = 5", "22003" },
	{ "select x, y from t where x = 1 and x + 0 = 1 and y = 2 and y + 0 = 2", new String[][] { {"1","2"} } },
	{ "select x, y from t where x = 1 or y = 2", new String[][] {
		{"1","1"},{"1","2"},{"2","2"},{null,"2"},{"1",null} } },
	{ "select x, y from t where y = 2 or x = 1", new String[][] {
		{"1","1"},{"1","2"},{"2","2"},{null,"2"},{"1",null} } },
	{ "select x, y from t where x = 4 or y = 5", new String[0][0] },
	{ "select x, y from t where y = 5 or x = 4", new String[0][0] },
	{ "select x from s where x = 1 or 2147483647 + 10 = 2", new String[][] { {"1"} } },
	{ "select x from s where 2147483647 + 10 = 2 or x = 1", "22003" },
	{ "select x, y from t where x = 1 or x + 0 = 1 or y = 2 or y + 0 = 2", new String[][] {
		{ "1","1"},{"1","2"},{"2","2"},{null,"2"},{"1",null} } },
	{ "select x from s where (1 = 1) or (2 = 2) and (3 = 4)", new String[][] { {"1"} } },
	{ "select x from s where (1 = 2) and (3 = 3) or (4 = 4)", new String[][] { {"1"} } },
	{ "select x from s where ( (1 = 1) or (2 = 2) ) and (3 = 4)", new String[0][0] },
	{ "select x from s where (1 = 2) and ( (3 = 3) or (4 = 4) )", new String[0][0] },
	// Test layers and layers of AND/OR mix
	{ "select * from s where (	( ( ((1=1) and (1=1)) and ((1=1) and (1=1)) ) and  ( ((1=1) and (1=1)) and ((1=1) and (1=1)) ) ) and ( ( ((1=1) and (1=1)) and ((1=1) and (1=1)) ) and ( ((1=1) and (1=1)) and ((1=1) and (1=1)) ) ) )", new String[][] { {"1"} } },
	{ "select * from s where (	( ( ((1=1) or (1=1)) or ((1=1) or (1=1)) ) or ( ((1=1) or (1=1)) or ((1=1) or (1=1)) ) ) or ( ( ((1=1) or (1=1)) or ((1=1) or (1=1)) ) or ( ((1=1) or (1=1)) or ((1=1) or (1=1)) ) ) )", new String[][] { {"1"} } },
	{ "select * from s where (	( ( ((1=1) and (1=1)) or ((1=1) and (1=1)) ) or ( ((1=1) and (1=1)) or ((1=1) and (1=1)) ) ) or ( ( ((1=1) and (1=1)) or ((1=1) and (1=1)) ) or ( ((1=1) and (1=1)) or ((1=1) and (1=1)) ) ) )", new String[][] { {"1"} } },
	{ "select * from s where ( ( ( ((1=1) or (1=1)) and ((1=1) or (1=1)) ) and ( ((1=1) or (1=1)) and ((1=1) or (1=1)) ) ) and ( ( ((1=1) or (1=1)) and ((1=1) or (1=1)) ) and ( ((1=1) or (1=1)) and ((1=1) or (1=1)) ) ) )", new String[][] { {"1"} } },
	{ "select * from s where ( ( ( ( ( ((1=1) and (1=1)) and (1=1)) and (1=1)) and (1=1))	and (1=1)) and (1=1))", new String[][] { {"1"} } },
	{ "select * from s where ( ( ( ( ( ((1=1) or (1=1)) or (1=1)) or (1=1)) or (1=1)) or (1=1)) or (1=1))", new String[][] { {"1"} } },
	{ "select * from s where ( ( ( ( ( ((1=1) or (1=2)) or (1=2)) or (1=2)) or (1=2)) or (1=2)) or (1=2))", new String[][] { {"1"} } },
	{ "select * from s where ( ( ( ( ( ((1=2) or (1=2)) or (1=1)) or (1=2)) or (1=2)) or (1=2)) or (1=2))", new String[][] { {"1"} } },
	{ "select * from s where ( ( ( ( ( ((1=2) or (1=2)) or (1=2)) or (1=1)) or (1=2)) or (1=2)) or (1=2))", new String[][] { {"1"} } },
	{ "select * from s where ( ( ( ( ( ((1=2) or (1=2)) or (1=2)) or (1=2)) or (1=1)) or (1=2)) or (1=2))", new String[][] { {"1"} } },
	{ "select * from s where ( ( ( ( ( ((1=2) or (1=2)) or (1=2)) or (1=2)) or (1=2)) or (1=1)) or (1=2))", new String[][] { {"1"} } },
	{ "select * from s where ( ( ( ( ( ((1=2) or (1=2)) or (1=2)) or (1=2)) or (1=2)) or (1=2)) or (1=1))", new String[][] { {"1"} } },
	{ "select * from s where ( ( ( ( ( (1=1) or ((1=2) or (1=2)) ) or (1=2)) or (1=2)) or (1=2)) or (1=2))", new String[][] { {"1"} } },
	{ "select * from s where ( ( ( ( ( (1=2) or ((1=1) or (1=2)) ) or (1=2)) or (1=2)) or (1=2)) or (1=2))", new String[][] { {"1"} } },
	{ "select * from s where ( ( ( ( ( (1=2) or ((1=2) or (1=1)) ) or (1=2)) or (1=2)) or (1=2)) or (1=2))", new String[][] { {"1"} } },
	{ "select * from s where ( ( ( ( ( (1=2) or ((1=2) or (1=2)) ) or (1=1)) or (1=2)) or (1=2)) or (1=2))", new String[][] { {"1"} } },
	{ "select * from s where ( ( ( ( ( (1=2) or ((1=2) or (1=2)) ) or (1=2)) or (1=1)) or (1=2)) or (1=2))", new String[][] { {"1"} } },
	{ "select * from s where ( ( ( ( ( (1=2) or ((1=2) or (1=2)) ) or (1=2)) or (1=2)) or (1=1)) or (1=2))", new String[][] { {"1"} } },
	{ "select * from s where ( ( ( ( ( (1=2) or ((1=2) or (1=2)) ) or (1=2)) or (1=2)) or (1=2)) or (1=1))", new String[][] { {"1"} } },
	{ "select x from s where 2147483647 + 10 = 2 and (1=2)", "22003" },
	{ "select x from s where (1=2) and 2147483647 + 10 = 2", new String[0][0] },
	{ "select x from s where not ( (1 = 1) or (2 = 2) ) and (3 = 4)", new String[0][0] },
	{ "select x from s where not ( ( (1 = 1) or (2 = 2) ) and (3 = 4) )", new String[][] { {"1"} } },
	{ "select x from s where (1 = 2) and not ( (3 = 3) or (4 = 4) )", new String[0][0] },
	{ "select x from s where not ( (1 = 2) and ( (3 = 3) or (4 = 4) ) )", new String[][] { {"1"} } },
	{ "select ( not ( (1 = 1) or (2 = 2) ) and (3 = 4) ) from s", "42X01" },
	{ "select ( not ( ( (1 = 1) or (2 = 2) ) and (3 = 4) ) ) from s", "42X01" },
	{ "select ( (1 = 2) and not ( (3 = 3) or (4 = 4) ) ) from s", "42X01" },
	{ "select ( not ( (1 = 2) and ( (3 = 3) or (4 = 4) ) ) ) from s", "42X01" },
	{ "select * from s where not (	( ( ((1=1) and (1=1)) and ((1=1) and (1=1)) ) and ( ((1=1) and (1=1)) and ((1=1) and (1=1)) ) ) and ( ( ((1=1) and (1=1)) and ((1=1) and (1=1)) ) and ( ((1=1) and (1=1)) and ((1=1) and (1=2)) ) ) )", new String[][] { {"1"} } },
	{ "select * from s where not (	( ( ((1=1) or (1=1)) or ((1=1) or (1=1)) ) or ( ((1=1) or (1=1)) or ((1=1) or (1=1)) ) ) or ( ( ((1=1) or (1=1)) or ((1=1) or (1=1)) ) or ( ((1=1) or (1=1)) or ((1=1) or (1=2)) ) ) )", new String[0][0] },
	{ "select * from s where not (	( ( ((1=1) and (1=1)) or ((1=1) and (1=1)) ) or ( ((1=1) and (1=1)) or ((1=1) and (1=1)) ) ) or ( ( ((1=1) and (1=1)) or ((1=1) and (1=1)) ) or ( ((1=1) and (1=1)) or ((1=1) and (1=2)) ) ) )", new String[0][0] },
	{ "select * from s where not ( ( ( ((1=1) or (1=1)) and ((1=1) or (1=1)) ) and ( ((1=1) or (1=1)) and ((1=1) or (1=1)) ) ) and ( ( ((1=1) or (1=1)) and ((1=1) or (1=1)) ) and ( ((1=1) or (1=1)) and ((1=1) or (1=2)) ) ) )", new String[0][0] },
	{ "select * from s where not ( ( ( ( ( ((1=1) and (1=2)) and (1=1)) and (1=1)) and (1=1)) and (1=1)) and (1=1))", new String[][] { {"1"} } },
	{ "select * from s where not ( ( ( ( ( ((1=2) or (1=1)) or (1=1)) or (1=1)) or (1=1)) or (1=1)) or (1=1))", new String[0][0] },
	{ "select * from s where not ( ( ( ( ( ((1=2) or (1=2)) or (1=2)) or (1=2)) or (1=2)) or (1=2)) or (1=2))", new String[][] { {"1"} } },
	{ "select * from s where not ( ( ( ( ( ((1=2) or (1=2)) or (1=1)) or (1=2)) or (1=2)) or (1=2)) or (1=2))", new String[0][0] },
	{ "select * from s where not ( ( ( ( ( ((1=2) or (1=2)) or (1=2)) or (1=1)) or (1=2)) or (1=2)) or (1=2))", new String[0][0] },
	{ "select * from s where not ( ( ( ( ( ((1=2) or (1=2)) or (1=2)) or (1=2)) or (1=1)) or (1=2)) or (1=2))", new String[0][0] },
	{ "select * from s where not ( ( ( ( ( ((1=2) or (1=2)) or (1=2)) or (1=2)) or (1=2)) or (1=1)) or (1=2))", new String[0][0] },
	{ "select * from s where not ( ( ( ( ( ((1=2) or (1=2)) or (1=2)) or (1=2)) or (1=2)) or (1=2)) or (1=1))", new String[0][0] },
	{ "select * from s where not ( ( ( ( ( (1=1) or ((1=2) or (1=2)) ) or (1=2)) or (1=2)) or (1=2)) or (1=2))", new String[0][0] },
	{ "select * from s where not ( ( ( ( ( (1=2) or ((1=2) or (1=2)) ) or (1=2)) or (1=2)) or (1=2)) or (1=2))", new String[][] { {"1"} } },
	{ "select * from s where not ( ( ( ( ( (1=2) or ((1=2) or (1=1)) ) or (1=2)) or (1=2)) or (1=2)) or (1=2))", new String[0][0] },
	{ "select * from s where not ( ( ( ( ( (1=2) or ((1=2) or (1=2)) ) or (1=1)) or (1=2)) or (1=2)) or (1=2))", new String[0][0] },
	{ "select * from s where not ( ( ( ( ( (1=2) or ((1=2) or (1=2)) ) or (1=2)) or (1=1)) or (1=2)) or (1=2))", new String[0][0] },
	{ "select * from s where not ( ( ( ( ( (1=2) or ((1=2) or (1=2)) ) or (1=2)) or (1=2)) or (1=1)) or (1=2))", new String[0][0] },
	{ "select * from s where not ( ( ( ( ( (1=2) or ((1=2) or (1=2)) ) or (1=2)) or (1=2)) or (1=2)) or (1=1))", new String[0][0] },
	{ "select * from s where not ( not ( not ((1=2) or (1=1))) or (not ((1=2) or (1=2)) ) )", new String[0][0] },
	{ "select not ( not ( not ((1=2) or (1=1))) or (not ((1=2) or (1=2)) ) ) from s", "42X01" },
	{ "select * from s where 1", "42X19" },
	{ "select * from s where 1 and (1=1)", "42Y94" },
	{ "select * from s where (1=1) and 1", "42Y94" },
	{ "select * from s where 1 or (1=1)", "42Y94" },
	{ "select * from s where (1=1) or 1", "42Y94" },
	{ "select * from s where not 1", "42X40" },
	{ "drop table t", null },
	{ "drop table s", null }
    };

    Connection conn = TestUtil.getConnection();
    Statement stmt = conn.createStatement();
    // Go through the array, execute each string[0], check sqlstate [1]
    // This will fail on the first one that succeeds where it shouldn't
    // or throws unknown exception
    JDBC.SQLUnitTestHelper(stmt,Script_LogopUTPartitioning);

  }
}
