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

public class LangScripts_ValuesClauseDUnit extends DistributedSQLTestBase {

	public LangScripts_ValuesClauseDUnit(String name) {
		super(name);
		// TODO Auto-generated constructor stub
	}
	  // This test is the as-is LangScript conversion, without any partitioning clauses
	  public void testLangScript_ValuesClauseTestNoPartitioning() throws Exception
	  {
	    // This is a JUnit conversion of the Derby Lang ValuesClause.sql script
	    // without any GemFireXD extensions
	    // The original test compared runtime plans, but that's not how we determine correct results for GemFireXD
		  
	    // Catch exceptions from illegal syntax
	    // Tests still not fixed marked FIXME
		  
	    // Array of SQL text to execute and sqlstates to expect
	    // The first object is a String, the second is either 
	    // 1) null - this means query returns no rows and throws no exceptions
	    // 2) a string - this means query returns no rows and throws expected SQLSTATE
	    // 3) a String array - this means query returns rows which must match (unordered) given resultset
	    //       - for an empty result set, an uninitialized size [0][0] array is used
	    Object[][] Script_ValuesClauseUT = {
		// this test is for the values clause functionality
		{ "create table t1 (i int, j int) PARTITION BY COLUMN(i)", null },
	     { "create table t2 (k int, l int) PARTITION BY COLUMN(k)", null },
		{ "insert into t2 values (1, 2)", null },
	     { "insert into t2 values (3, 4)", null },
		{ "values(null)", "42X07" },
	     { "values(1,null)", "42X07" },
	     { "values(null,1)", "42X07" },
	     { "values(null),(1)", "42X07" },
	     { "values(1),(null)", "42X07" },
	     { "select x from (values(null,1)) as x(x,y)", "42X07" },
	     { "select x from (values(1,null)) as x(x,y)", "42X07" },
	     { "select x from (values null) as x(x)", "42X07" },
		{ "values()", "42X01" },
		{ "values 1", new String[][] { {"1"} } },
	     { "values (1)", new String[][] { {"1"} } },
	     { "insert into t1 values (1, null)", null },
	     { "select * from t1", new String[][] { {"1",null} } },
	     { "delete from t1", null },
		{ "values (1, 2, 3)", new String[][] { {"1","2","3"} } },
		{ "select * from (values (1, 2, 3)) a", new String[][] { {"1","2","3"} } },
	     { "select a, b from (values (1, 2, 3)) a (a, b, c)", new String[][] { {"1","2"} } },
	     { "select * from (values (1, 2, 3)) a, (values (4, 5, 6)) b", new String[][] { {"1","2","3","4","5","6"} } },
	     { "select * from t2, (values (1, 2, 3)) a", new String[][] { {"1","2","1","2","3"},{"3","4","1","2","3"} } },
	     { "select * from (values (1, 2, 3)) a (a, b, c), t2 where l = b", new String[][] { {"1","2","3","1","2"} } },
	     // FIXME
	     // These returns a NULL result in peer-client mode, in thin-client it works for replicated regions, not for partitioned
		//{ "values (select k from t2 where k = 1)", new String[][] { {"1"} } },
	     //{ "values (2, (select k from t2 where k = 1))", new String[][] { {"2","1"} } },
	     //{ "values ((select k from t2 where k = 1), 2)", new String[][] { {"1","2"} } },
	     //{ "values ((select k from t2 where k = 1), (select l from t2 where l = 4))", new String[][] { {"1","4"} } },
	     //{ "insert into t1 values ((select k from t2 where k = 1), (select l from t2 where l = 4))", null },
	     //{ "select * from t1", new String[][] { {"1","4"} } },
	     { "delete from t1", null },
		//{ "update t2 set k = (values 5) where l = 2", null },
	     //{ "select * from t2", new String[][] { {"5","2"},{"3","4"} } },
		//{ "update t2 set k = (values (select 2 from t2 where l = 5))", null },
	     //{ "select * from t2", new String[][] { {null,"2"},{null,"4"} } },
		{ "values 1, (2, 3), 4", "42X59" },
	     { "values (2, 3), (4, 5, 6)", "42X59" },
		{ "values 1, , 2", "42X80" },
		{ "values (1 = 1.2)", "42X01" },
		{ "values (1.2 = 1)", "42X01" },
		{ "values (1 = cast(1 as bigint))", "42X01" },
		{ "values (1 = cast(1 as smallint))", "42X01" },
		{ "values (cast(1 as bigint) = 1)", "42X01" },
		{ "values (cast(1 as smallint) = 1)", "42X01" },
		{ "create table insert_test1 (c1 int)", null },
	     { "insert into insert_test1 values 1, 2, 3", null },
	     { "select * from insert_test1", new String [][] { {"1"},{"2"},{"3"} } },
	     { "delete from insert_test1", null },
	     { "insert into insert_test1 values 1, null, 3", null },
	     { "select * from insert_test1", new String [][] { {"1"},{null},{"3"} } },
	     { "delete from insert_test1", null },
		{ "create table x(x int) REPLICATE", null },
	     { "insert into x values 1, 2, 3, 4", null },
	     // FIXME
	     // This returns NULL as the MAX()
	     //{ "select * from (values (1, (select max(x) from x), 1)) c", new String[][] { {"1","4","1"} } },
	     //{ "select * from x, (values (1, (select max(x) from x), 1)) c(a, b, c) where x = c", new String[][] { {"1","1","4","1"} } },
	     { "drop table x", null },
		{ "drop table t1", null },
	     { "drop table t2", null },
	     { "drop table insert_test1", null },
		{ "create table target (a int, b int) REPLICATE", null },
	     { "create index idx on target(b)", null },
	     { "insert into target values (1, 2), (2, 3), (0, 2)", null },
	     { "create table sub (a int, b int) REPLICATE", null },
	     { "insert into sub values (1, 2), (2, 3), (2, 4)", null },
	     { "select * from (select b from sub) as q(b)", new String[][] { {"2"},{"3"},{"4"} } },
	     { "select * from table (select b from sub) as q(b)", new String[][] { {"2"},{"3"},{"4"} } },
	     { "select * from table (select * from table (select b from sub) as q(b)) as p(a)", new String[][] { {"2"},{"3"},{"4"} } },
	     { "select * from table (select b from sub) as q(b), target", new String[][] {
			{"2","1","2"},{"2","2","3"},{"2","0","2"},
			{"3","1","2"},{"3","2","3"},{"3","0","2"},
			{"4","1","2"},{"4","2","3"},{"4","0","2"} } },
	     { "select * from table (select b from sub) as q(b), target where q.b = target.b", new String[][] {
			{"2","1","2"},{"2","0","2"},{"3","2","3"} } },
	     { "select * from  (values (1)) as q(a)", new String[][] { {"1"} } },
	     { "select * from  table (values (1)) as q(a), table (values ('a'), ('b'), ('c')) as p(a)", new String[][] {
			{"1","a"},{"1","b"},{"1","c"} } },
		{ "select * from  table target", "42X01" },
	     { "select * from  table (target)", "42X01" },
	     { "select * from  table (target as q)", "42X01" },
	     { "drop table sub", null },
	     { "drop table target", null },
		{ "create table t1 (c1 int)", null },
	     { "insert into t1 values 1", null },
		{ "select nullif(c1, 1) is null from t1", "42X01" },
		{ "values 1 is null", "42X01" },
		{ "values 1 = 1", "42X01" },
		{ "select 1 = 1 from t1", "42X01" },
		{ "values (nullif('abc','a') = 'abc')", "42X01" },
		{ "select (nullif('abc','a') = 'abc') from t1", "42X01" },
		{ "select c11 = any (select c11 from t1) from t1", "42X01" },
		{ "values 2 > 1", "42X01" },
		{ "select 2 > 1 from t1", "42X01" },
		{ "values 2 >= 1", "42X01" },
		{ "select 2 >= 1 from t1", "42X01" },
		{ "values 1 < 2", "42X01" },
		{ "select 1 < 2 from t1", "42X01" },
		{ "values 1 <= 2", "42X01" },
		{ "select 1 <= 2 from t1", "42X01" },
		{ "values (1>1)", "42X01" },
		{ "select (c1 < 2) from t1", "42X01" },
		{ "values (1 between 2 and 5)", "42X01" },
		{ "select (c1 between 1 and 3) from t1", "42X01" },
		{ "values '%foobar' like '%%foobar' escape '%'", "42X01" },
		{ "select '_foobar' like '__foobar' escape '_' from t1", "42X01" },
		{ "values 1 instanceof int", "42X01" },
		{ "values 1 instanceof java.lang.Integer between false and true", "42X01" },
		{ "select exists (values 1) from t1", "42X01" },
	     { "values exists (values 2)", "42X80" },
		{ "update t1 set c11 = exists(values 1)", "42X01" },
		{ "values not true ? false : true", "42X80" },
		{ "select not true ? false : true from t1", "42X01" },
		{ "values 1 ? 2 : 3", "42X01" },
		{ "select c1 is null ? true : false from t1", "42X01" },
		{ "select new java.lang.Integer(c1 is null ? 0 : c1) from t1", "42X01" },
		{ "select c1, (c1=1? cast(null as int) : c1) is null from t1", "42X01" },
		{ "values new java.lang.String() = ''", "42X01" },
		{ "values new java.lang.String('asdf') = 'asdf'", "42X01" },
		{ "select new java.lang.String() = '' from t1", "42X01" },
		{ "select new java.lang.String('asdf') = 'asdf' from t1", "42X01" },
		{ "VALUES INTEGER(1.5)", new String[][] { {"1"} } },
	     { "VALUES INT(1.5)", new String[][] { {"1"} } },
	     { "create table t3 (i int)", null},
		{ "select * from t3 where (values null)", "42X07" },
		{ "select * from t3 order by (values null)", "42X07" },
		{ "select (values null) from t3", "42X07" },
		{ "select * from t3 group by (values null)", "42X07" },
		{ "select * from t3 group by i having (values null)", "42X07" },
	     { "drop table t3", null }
	   };

	    // Start 1 client and 3 servers, use default partitioning
	    startVMs(1, 3);


	    Connection conn = TestUtil.getConnection();
	    Statement stmt = conn.createStatement();
	    // Go through the array, execute each string[0], check sqlstate [1]
	    // This will fail on the first one that succeeds where it shouldn't
	    // or throws unknown exception
	    JDBC.SQLUnitTestHelper(stmt,Script_ValuesClauseUT);
	  }

}
