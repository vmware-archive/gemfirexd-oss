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

public class LangScripts_MiscErrorsTest extends JdbcTestBase {

  public static void main(String[] args) {
    TestRunner.run(new TestSuite(LangScripts_MiscErrorsTest.class));
  }
  
  public LangScripts_MiscErrorsTest(String name) {
    super(name); 
  }

  // This test is the as-is LangScript conversion, without any partitioning clauses
  public void testLangScript_MiscErrorsTestNoPartitioning() throws Exception
  {
    // This is a JUnit conversion of the Derby Lang MiscErrors.sql script
    // without any GemFireXD extensions
	  
    // Catch exceptions from illegal syntax
    // Tests still not fixed marked FIXME
	  
    // Array of SQL text to execute and sqlstates to expect
    // The first object is a String, the second is either 
    // 1) null - this means query returns no rows and throws no exceptions
    // 2) a string - this means query returns no rows and throws expected SQLSTATE
    // 3) a String array - this means query returns rows which must match (unordered) given resultset
    //       - for an empty result set, an uninitialized size [0][0] array is used
    Object[][] Script_MiscErrorsUT = {
	// this test is for miscellaneous errors
	// lexical error
	{ "select @#^%*&! from swearwords", "42X02" },
	// try to create duplicate table
	{ "create table a (one int)", null },
	{ "create table a (one int, two int)", "X0Y32" },
	{ "create table a (one int)", "X0Y32" },
	{ "drop table a ", null },
	{ "create table a (one int, two int, three int)", null },
	{"insert into a values (1,2,3)", null },
	{ "select * from a", new String[][] { {"1","2","3"} } },
	{ "drop table a", null },
	// TODO : set isolation to repeatable read here
	// see that statements that fail at parse or bind time are not put in the statment cache;
	{ "values 1", new String[][] { {"1"} } },
	// GemFireXD prepares the statement once before execution, so it is already in the statement cache during execution
	// Derby expects zero rows from this query
	{ "select SQL_TEXT from syscs_diag.statement_cache where CAST(SQL_TEXT AS LONG VARCHAR) LIKE '%932432%'", new String[][] {
		 } },
	{ "VALUES FRED932432", "42X04" },
	{ "SELECT * FROM BILL932432", "42X05" },
	{ "SELECT 932432", "42X01" },
	// Only one in statement_cache should be the original select, all others had errors
	{ "select SQL_TEXT from syscs_diag.statement_cache where CAST(SQL_TEXT AS LONG VARCHAR) LIKE '%932432%'", new String[][] {
		} }
   };

    // Do not use partitioning as default, use replicate
    // (Some results are expected to be different with partitioning)
    skipDefaultPartitioned = true;

    Connection conn = TestUtil.getConnection();
    Statement stmt = conn.createStatement();
    // Go through the array, execute each string[0], check sqlstate [1]
    // This will fail on the first one that succeeds where it shouldn't
    // or throws unknown exception
    JDBC.SQLUnitTestHelper(stmt,Script_MiscErrorsUT);
  }
  
  // This test is the script enhanced with partitioning
  public void testLangScript_MiscErrorsWithPartitioning() throws Exception
  {
    // This form of the MiscErrors.sql test has partitioning activated
    // TODO : Create specialized partitioning for these tables for MiscErrors
    //  Currently, default partitioning is active
    Object[][] Script_MiscErrorsUTPartitioning = {
	// this test is for miscellaneous errors
	// lexical error
	// FIXME
	// This throws XJ001 because TokenMgrErr is not correctly caught by the parsing layer - known bug
	//{ "select @#^%*&! from swearwords", "42X02" },
	// try to create duplicate table
	{ "create table a (one int)", null },
	{ "create table a (one int, two int)", "X0Y32" },
	{ "create table a (one int)", "X0Y32" },
	{ "drop table a ", null },
	{ "create table a (one int, two int, three int)", null },
	{"insert into a values (1,2,3)", null },
	{ "select * from a", new String[][] { {"1","2","3"} } },
	{ "drop table a", null },
	// TODO : set isolation to repeatable read here
	// see that statements that fail at parse or bind time are not put in the statment cache;
	{ "values 1", new String[][] { {"1"} } },
	// GemFireXD prepares the statement once before execution, so it is already in the statement cache during execution
	// Derby expects zero rows from this query
	{ "select SQL_TEXT from syscs_diag.statement_cache where CAST(SQL_TEXT AS LONG VARCHAR) LIKE '%932432%'", new String[][] {
		 } },
	{ "VALUES FRED932432", "42X04" },
	{ "SELECT * FROM BILL932432", "42X05" },
	{ "SELECT 932432", "42X01" },
	// Only one in statement_cache should be the original select, all others had errors
	{ "select SQL_TEXT from syscs_diag.statement_cache where CAST(SQL_TEXT AS LONG VARCHAR) LIKE '%932432%'", new String[][] {
		 } }
    };

    Connection conn = TestUtil.getConnection();
    Statement stmt = conn.createStatement();
    // Go through the array, execute each string[0], check sqlstate [1]
    // This will fail on the first one that succeeds where it shouldn't
    // or throws unknown exception
    JDBC.SQLUnitTestHelper(stmt,Script_MiscErrorsUTPartitioning);

  }
}
