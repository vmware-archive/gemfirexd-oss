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
package com.pivotal.gemfirexd.derbylang;

import java.sql.Connection;
import java.sql.Statement;

import org.apache.derbyTesting.junit.JDBC;

import com.pivotal.gemfirexd.DistributedSQLTestBase;
import com.pivotal.gemfirexd.TestUtil;

public class LangScripts_MiscErrorsDUnit extends DistributedSQLTestBase {

	public LangScripts_MiscErrorsDUnit(String name) {
		super(name);
		// TODO Auto-generated constructor stub
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
		// Asif :with changes in Statement matching , we also get 0 rows
		{ "select SQL_TEXT from syscs_diag.statement_cache where CAST(SQL_TEXT AS LONG VARCHAR) LIKE '%932432%'", new String[][] {
			 } },
		{ "VALUES FRED932432", "42X04" },
		{ "SELECT * FROM BILL932432", "42X05" },
		{ "SELECT 932432", "42X01" },
		// Only one in statement_cache should be the original select, all others had errors
		{ "select SQL_TEXT from syscs_diag.statement_cache where CAST(SQL_TEXT AS LONG VARCHAR) LIKE '%932432%'", new String[][] {
			 } }
	   };

	    // Start 1 client and 3 servers, use default partitioning
	    startVMs(1, 3);

	    Connection conn = TestUtil.getConnection();
	    Statement stmt = conn.createStatement();
	    // Go through the array, execute each string[0], check sqlstate [1]
	    // This will fail on the first one that succeeds where it shouldn't
	    // or throws unknown exception
	    JDBC.SQLUnitTestHelper(stmt,Script_MiscErrorsUT);
	  }

}
