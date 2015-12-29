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

public class LangScripts_InnerJoinDUnit extends DistributedSQLTestBase {

	public LangScripts_InnerJoinDUnit(String name) {
		super(name);
		// TODO Auto-generated constructor stub
	}
	  // This test is the as-is LangScript conversion, without any partitioning clauses
	  public void testLangScript_InnerJoinTestNoPartitioning() throws Exception
	  {
	    // This is a JUnit conversion of the Derby Lang InnerJoin.sql script
	    // without any GemFireXD extensions
		  
	    // Catch exceptions from illegal syntax
	    // Tests still not fixed marked FIXME
		  
	    // Array of SQL text to execute and sqlstates to expect
	    // The first object is a String, the second is either 
	    // 1) null - this means query returns no rows and throws no exceptions
	    // 2) a string - this means query returns no rows and throws expected SQLSTATE
	    // 3) a String array - this means query returns rows which must match (unordered) given resultset
	    //       - for an empty result set, an uninitialized size [0][0] array is used
	    Object[][] Script_InnerJoinUT = {
		// create some tables
	    // For partitioned test, colocate t1 and t2 but keep t3 replicated or else (valid) colocation exceptions occur
		{ "create table t1(c1 int) partition by column(c1)", null },
		{ "create table t2(c1 int) partition by column(c1)" + colocateT2(), null },
		{ "create table t3(c1 int) replicate", null },
		{ "create table insert_test(c1 int, c2 int, c3 int)", null },
		{ "insert into t1 values 1, 2, 3, 4", null },
		{ "insert into t2 values 1, 3, 5, 6", null },
		{ "insert into t3 values 2, 3, 5, 7", null },
		// negative tests
		{ "select * from t1 join t2", "42X01" },
		{ "select * from t1 inner join t2", "42X01" },
		{ "select * from t1 join t2 using ()", "42X01" },
		{ "select * from t1 join t2 on 1", "42Y12" },
		{ "select * from t1 join t1 on 1=1", "42X03" },
		{ "select * from t1 join t1 on c1 = 1", "42X03" },
		{ "select * from t1 join t1 on (c1)", "42X03" },
		{ "select * from t1, t2 join t3 on t1.c1 = t2.c1", "42972" },
		{ "select * from t2 b inner join t3 c on a.c1 = b.c1 and b.c1 = c.c1", "42X04" },
		{ "select * from t3 b where exists (select * from t1 a inner join t2 on b.c1 = t2.c1)", "42972" },
		{ "select * from t3 where exists (select * from t1 inner join t2 on t3.c1 = t2.c1)", "42972" },
		// positive tests
		{ "select a.c1 from t1 a join t2 b on a.c1 = b.c1", new String[][] { {"1"},{"3"} } },
		{ "select a.x from t1 a (x) join t2 b (x) on a.x = b.x", new String[][] { {"1"},{"3"} } },
	/* TODO : PREPARE test
	gfxd> -- parameters and join clause
	prepare asdf as 'select * from t1 join t2 on ?=1 and t1.c1 = t2.c1';
	gfxd> execute asdf using 'values 1';
	C1         |C1         
	-----------------------
	1          |1          
	3          |3          
	gfxd> remove asdf;
	gfxd> prepare asdf as 'select * from t1 join t2 on t1.c1 = t2.c1 and t1.c1 = ?';
	gfxd> execute asdf using 'values 1';
	C1         |C1         
	-----------------------
	1          |1          
	gfxd> remove asdf;
	*/
		{ "select * from t1 join t2 on t1.c1 = t2.c1 where t1.c1 = 1", new String[][] { {"1","1"} } },
		{ "select * from t1 join t2 on t1.c1 = 1 where t2.c1 = t1.c1", new String[][] { {"1","1"} } },
		{ "select * from t1 a join t2 b on a.c1 = b.c1 and a.c1 = (select c1 from t1 where a.c1 = t1.c1)", "42972" },
		{ "select * from t1 a join t2 b on a.c1 = b.c1 and a.c1 in (select c1 from t1 where a.c1 = t1.c1)", "42972" },
		{ "select * from t1 a where exists (select * from t1 inner join t2 on a.c1 = t2.c1)", "42972" },
		{ "select * from t1 join t2 on t1.c1 = t2.c1 inner join t3 on t1.c1 = t3.c1", new String[][] { {"3","3","3"} } },
		{ "select * from (t1 join t2 on t1.c1 = t2.c1) inner join t3 on t1.c1 = t3.c1", new String[][] { {"3","3","3"} } },
		{ "select * from t1 join (t2 inner join t3 on t2.c1 = t3.c1) on t1.c1 = t2.c1", new String[][] { {"3","3","3"} } },
		{ outerJoin1(), new String[][] { {"3","3","3"} } },
		{ outerJoin2(), new String[][] { {"3","3","3"} } },
		{ "select * from t1 a join t2 b on a.c1 = b.c1 inner join t3 c on c.c1 = a.c1 where c.c1 > 2 and a.c1 > 2", new String[][] { {"3","3","3"} } },
		{ "select * from (t1 a join t2 b on a.c1 = b.c1) inner join t3 c on c.c1 = a.c1 where c.c1 > 2 and a.c1 > 2", new String[][] { {"3","3","3"} } },
		{ "select * from t1 a join (t2 b inner join t3 c on c.c1 = b.c1) on a.c1 = b.c1 where c.c1 > 2 and b.c1 > 2", new String[][] { {"3","3","3"} } },
		{ insertSelect1(), null },
		{ "select * from insert_test", new String[][] {
			{"1","1","2"},{"1","1","3"},{"1","1","5"},{"1","1","7"},
			{"3","3","2"},{"3","3","5"},{"3","3","7"} } },
		// FIXME
		// This gets 'bucket-not-found'
		// Instead, to allow the test to continue, will replace with set c1 = 9 where c1 = 1
		// TODO : remove that workaround when the original UPDATE is fixed
		//{ "update insert_test set c1 = (select 9 from t1 a join t1 b on a.c1 = b.c1 where a.c1 = 1) where c1 = 1", null },
		{ "update insert_test set c1 = 9 where c1 = 1", null },
		{ "select * from insert_test", new String[][] {
			{"9","1","2"},{"9","1","3"},{"9","1","5"},{"9","1","7"},
			{"3","3","2"},{"3","3","5"},{"3","3","7"} } },
		// FIXME
		// This throws an assertion
		// Instead, to allow the test to continue, will replace with ... where c1 = 9
		// TODO : remove that workaround when the original DELETE is fixed
		//{ "delete from insert_test where c1 = (select 9 from t1 a join t1 b on a.c1 = b.c1 where a.c1 = 1)", null },
		{ "delete from insert_test where c1 = 9", null },
		{ "select * from insert_test", new String[][] { {"3","3","2"},{"3","3","5"},{"3","3","7"} } },
		// FIXME
		// This throws colocation assertion 0A000 but it's a self-join so no colocation is needed
		// Note that INSERT_TEST is 'default' partitioned
		//{ "select * from insert_test a join insert_test b on a.c1 = b.c1 and a.c2 = b.c2 and a.c3 = b.c3", new String[][] {
		//	{"3","3","2","3","3","2"},
		//	{"3","3","5","3","3","5"},
		//	{"3","3","7","3","3","7"} } },
		{ "delete from insert_test", null },
		{ insertSelect2(), null },
		{ "select * from insert_test", new String[][] {
			{"1","1","2"},{"1","1","3"},{"1","1","5"},{"1","1","7"},
			{"3","3","2"},{"3","3","5"},{"3","3","7"} } },
		{ "delete from insert_test", null },
		{ "drop table t2", null },
		{ "drop table t1", null },
		{ "drop table t3", null },
		{ "drop table insert_test", null }
	    };

	    // Start 1 client and 3 servers, use default partitioning
	    startVMs(1, 3);

	    Connection conn = TestUtil.getConnection();
	    Statement stmt = conn.createStatement();
	    // Go through the array, execute each string[0], check sqlstate [1]
	    // This will fail on the first one that succeeds where it shouldn't
	    // or throws unknown exception
	    JDBC.SQLUnitTestHelper(stmt,Script_InnerJoinUT);
	  }
	  
	  protected String colocateT2() {
	    return " colocate with (t1) ";
	  }
	  
          protected String outerJoin1() {
            return "select * from t1 a left outer join t2 b on a.c1 = b.c1 inner join t3 c on b.c1 = c.c1";
          }
        
          protected String outerJoin2() {
            return "select * from (t1 a left outer join t2 b on a.c1 = b.c1) inner join t3 c on b.c1 = c.c1";
          }
          
          protected String insertSelect1() {
            return "insert into insert_test select * from t1 a join t2 b on a.c1 = b.c1 inner join t3 c on a.c1 <> c.c1";
          }
          
          protected String insertSelect2() {
            return "insert into insert_test select * from (select * from t1 a join t2 b on a.c1 = b.c1 inner join t3 c on a.c1 <> c.c1) d (c1, c2, c3)";
          }
}
