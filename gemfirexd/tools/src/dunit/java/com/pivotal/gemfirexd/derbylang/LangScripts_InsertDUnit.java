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

public class LangScripts_InsertDUnit extends DistributedSQLTestBase {

	public LangScripts_InsertDUnit(String name) {
		super(name);
		// TODO Auto-generated constructor stub
	}

	public void testLangScript_InsertTest() throws Exception
	  {
	    // This is a JUnit conversion of the Derby Lang Insert.sql script
	    // without any GemFireXD extensions
		  
	    // Catch exceptions from illegal syntax
	    // Tests still not fixed marked FIXME
		  
	    // Array of SQL text to execute and sqlstates to expect
	    // The first object is a String, the second is either 
	    // 1) null - this means query returns no rows and throws no exceptions
	    // 2) a string - this means query returns no rows and throws expected SQLSTATE
	    // 3) a String array - this means query returns rows which must match (unordered) given resultset
	    //       - for an empty result set, an uninitialized size [0][0] array is used
	    Object[][] Script_InsertUT = {
	    // TODO : These tables are replicated due to colocation and unsupported insert statements below
	    // Future work should try these are partitioned by default as well
		{ "create table t1 (i int, j int) replicate", null },
		{ "create table t2 (k int, l int) replicate", null },
		{ "insert into t2 values (1, 2)", null },
		{ "insert into t2 values (3, 4)", null },
		{ "insert into t1 select * from t2", null },
		{ "insert into t1 (i, j) select * from t2", null },
		{ "insert into t1 (j, i) select * from t2", null },
		{ "select * from t1", new String[][] {
			{"1","2"},{"3","4"},
			{"1","2"},{"3","4"},
			{"2","1"},{"4","3"} } },
		{ "drop table t1", null },
		{ "create table t1 (i int, j int) replicate", null },
		{ "insert into t1 select k, l from t2", null },
		{ "insert into t1 select l, k from t2", null },
		{ "insert into t1 (i, j) select k, l from t2", null },
		{ "insert into t1 (j, i) select k, l from t2", null },
		{ "select * from t1", new String[][] {
			{"1","2"},{"3","4"},
			{"2","1"},{"4","3"},
			{"1","2"},{"3","4"},
			{"2","1"},{"4","3"} } },
		{ "drop table t1", null },
		{ "create table t1 (i int, j int) replicate", null },
		{ "insert into t1 select 5, 6 from t2", null },
		{ "insert into t1 (i, j) select 5, 6 from t2", null },
		{ "insert into t1 (j, i) select 6, 5 from t2", null },
		{ "select * from t1", new String[][] {
			{"5","6"},{"5","6"},
			{"5","6"},{"5","6"},
			{"5","6"},{"5","6"} } },
		{ "drop table t1", null },
		{ "create table t1 (i int, j int) replicate", null },
		{ "insert into t1 (i) select 666 from t2", null },
		{ "insert into t1 (j) select 666 from t2", null },
		{ "select * from t1 where i = 666 or j = 666", new String[][] {
			{"666",null},
			{"666",null},
			{null,"666"},
			{null,"666"} } },
		{ "drop table t1", null },
		{ "create table t1 (i int, j int) replicate", null },
		// Negative test cases - column references in values clause
		{ "insert into t1 values(1, c1)", "42X04" },
		{ "insert into t1 values", "42X80" },
		{ "insert into t1 values(1,1,1)", "42802" },
		{ "insert into t1 select 1, 2, 3 from t2", "42802" },
		{ "insert into t1 (i, i) values(2,2)", "42X13" },
		{ "insert into t1 (i, j) values(1)", "42802" },
		{ "insert into t1 (i) values (1, 2)", "42802" },
		{ "insert into t1 select 666 from t2", "42802" },
		{ "insert into t1 values (1,1)", null },
		{ "insert into t1 values (2,2)", null },
		{ "delete from t2", null },
		// T2 and T1 now both have the rows (1,1) and (2,2) in them
		// The original test used ROLLBACK to get back to this point in time
		// I will just re-insert the rows to T1 as T2 is never the target of INSERT
		{ "insert into t2 select * from t1", null },
		{ "select * from t1", new String[][] {
			{"1","1"},{"2","2"} } },
		{ "insert into t1 select t1.* from t1, t2", null },
		{ "select * from t1 order by 1, 2", new String[][] {
			{"1","1"},{"1","1"},
			{"1","1"},{"2","2"},
			{"2","2"},{"2","2"} } },
		{ "delete from t1", null },
		{ "insert into t1 values (1,1)", null },
		{ "insert into t1 values (2,2)", null },
		{ "insert into t1 (i) select (select i from t1 where i = 1) from t2", null },
		{ "select * from t1", new String[][] {
			{"1","1"},{"2","2"},{"1",null},{"1",null} } },
		{ "delete from t1", null },
		{ "insert into t1 values (1,1)", null },
		{ "insert into t1 values (2,2)", null },
		{ "insert into t1 (i) select 1 from t2 where 1 = (select i from t1 where i = 1)", null },
		{ "select * from t1", new String[][] {
			{"1","1"},{"2","2"},{"1",null},{"1",null} } },
		{ "delete from t1", null },
		{ "insert into t1 values (1,1)", null },
		{ "insert into t1 values (2,2)", null },
		{ "insert into t1 select * from (select * from t1) a", null },
		{ "select * from t1", new String[][] {
			{"1","1"},{"2","2"},{"1","1"},{"2","2"} } },
		{ "delete from t1", null },
		{ "insert into t1 values (1,1)", null },
		{ "insert into t1 values (2,2)", null },
		{ "insert into t1 select * from t2 union select * from t1", null },
		// FIXME
		// This gives 2 rows when run from a peer client instead of 4 in a multi-node setup
		//{ "select * from t1", new String[][] {
		//	{"1","1"},{"2","2"},{"1","1"},{"2","2"} } },
		{ "delete from t1", null },
		{ "insert into t1 values (1,1)", null },
		{ "insert into t1 values (2,2)", null },
		{ "insert into t1 select * from t2 union select * from (select * from t1) a", null },
		//{ "select * from t1", new String[][] {
		//	{"1","1"},{"2","2"},{"1","1"},{"2","2"} } },
		{ "delete from t1", null },
		{ "insert into t1 values (1,1)", null },
		{ "insert into t1 values (2,2)", null },
		{ "insert into t1 select * from t1 where i = 1", null },
		{ "select * from t1", new String[][] {
			{"1","1"},{"2","2"},{"1","1"} } },
		{ "delete from t1", null },
		{ "insert into t1 values (1,1)", null },
		{ "insert into t1 values (2,2)", null },
		{ "insert into t1 select * from t1 where i = 17", null },
		{ "select * from t1", new String[][] {
			{"1","1"},{"2","2"} } },
		{ "delete from t1", null },
		{ "insert into t1 values (1,1)", null },
		{ "insert into t1 values (2,2)", null },
		{ "create table atom_test_target (c1 smallint)", null },
		{ "create table atom_test_source (c1 smallint)", null },
		{ "insert into atom_test_source values 1, 30000,0, 2", null },
		{ "insert into atom_test_target select c1 + c1 from atom_test_source", "22003" },
		// GemFireXD in non-transactional mode may have inserted some of the rows into target
		// TODO : run this test in transactional mode and verify no rows were inserted?
		//{ "select * from atom_test_target", ?? },
		{ "insert into atom_test_target select c1 / c1 from atom_test_source", "22012" },
		//{ "select * from atom_test_target", ?? },
		{ "create table tchar( i int, c char(1) for bit data default x'02')", null },
		{ "create table tchar1 (i int, c char(5) for bit data default x'2020202020',v varchar(5) for bit data default x'2020',l long varchar for bit data default x'303030')", null },
		{ "drop table tchar", null },
		{ "drop table tchar1", null },
		{ "create table i1 (i int, t int, s smallint, l bigint, r real, dp double, dc dec)", null },
		{ "create table i2 (i int, t int, s smallint, l bigint, r real, dp double, dc dec)", null },
		{ "create table tab1 (i integer, t integer, s integer, l bigint,r real, dp double,dc decimal)", null },
		{ "insert into i1 values (1, 2, 3, 4, 5.5, 6.6, 7.7)", null },
		{ "insert into i1 values (null, null, null, null, null, null, null)", null },
		{ "insert into tab1 values(1, cast(2 as int), cast(3 as smallint), cast(4 as bigint), cast(5.5 as real), cast(6.6 as double), 7.7)", null },
		{ "insert into tab1 values (null, null, null, null, null, null, null)", null },
		{ "insert into i2 select i, i, i, i, i, i, i from i1", null },
		{ "insert into i2 select t, t, t, t, t, t, t from i1", null },
		{ "insert into i2 select s, s, s, s, s, s, s from i1", null },
		{ "insert into i2 select l, l, l, l, l, l, l from i1", null },
		{ "insert into i2 select r, r, r, r, r, r, r from i1", null },
		{ "insert into i2 select dp, dp, dp, dp, dp, dp, dp from i1", null },
		{ "insert into i2 select dc, dc, dc, dc, dc, dc, dc from i1", null },
		{ "select * from i2", new String[][] {
			{"1","1","1","1","1.0","1.0","1"},
			{"2","2","2","2","2.0","2.0","2"},
			{"3","3","3","3","3.0","3.0","3"},
			{"4","4","4","4","4.0","4.0","4"},
			{"5","5","5","5","5.5","5.5","5"},
			{"6","6","6","6","6.6","6.6","6"},
			{"7","7","7","7","7.0","7.0","7"},
			{null,null,null,null,null,null,null},
			{null,null,null,null,null,null,null},
			{null,null,null,null,null,null,null},
			{null,null,null,null,null,null,null},
			{null,null,null,null,null,null,null},
			{null,null,null,null,null,null,null},
			{null,null,null,null,null,null,null} } },
		{ "delete from i2", null },
		{ "insert into i2 select i, t, s, l, r, dp, dc from tab1", null },
		{ "select * from i2", new String [][] {
			{"1","2","3","4","5.5","6.6","7"},
			{null,null,null,null,null,null,null} } },
		{ "create table i3 (b char(1) for bit data, bv varchar(1) for bit data, lbv long varchar for bit data,c char(10),cv varchar(10),lvc long varchar,dt date,t time,ts timestamp)", null },
		{ "create table i4 (b char (1) for bit data, bv varchar(1) for bit data, lbv long varchar for bit data,c char(10),cv varchar(10),lvc long varchar,dt date,	t time,ts timestamp)", null },
		{ "insert into i3 values (X'11', X'22', X'25', '3', '4', '5', '1990-10-10', '11:11:11', '1990-11-11 11:11:11')", null },
		{ "insert into i3 values (null, null, null, null, null, null, null, null, null)", null },
		{ "insert into i4 select * from i3", null },
		{ "select * from i4", new String[][] {
			{"11","22","25","3","4","5","1990-10-10","11:11:11","1990-11-11 11:11:11.0"},
			{null,null,null,null,null,null,null,null,null} } }, 
		{ "delete from i4", null },
		{ "create table tab2 (c char,cv varchar(10),lvc long varchar,dt date,	t time,ts timestamp)", null },
		{ "insert into tab2 values ('3', '4', '5', '1990-10-10', '11:11:11', '1990-11-11 11:11:11')", null },
		{ "insert into tab2 values (null, null, null, null, null, null)", null },
		{ "insert into i4 (c, cv, lvc, dt, t, ts) select c, cv, lvc, dt, t, ts from tab2", null },
		{ "select * from i4", new String[][] {
			{null,null,null,"3","4","5","1990-10-10","11:11:11","1990-11-11 11:11:11.0"},
			{null,null,null,null,null,null,null,null,null} } },
		{ "drop table t1", null },
		{ "drop table t2", null },
		{ "drop table atom_test_target", null },
		{ "drop table atom_test_source", null },
		{ "drop table i1", null },
		{ "drop table i2", null },
		{ "drop table i3", null },
		{ "drop table i4", null },
		{ "drop table tab1", null },
		{ "drop table tab2", null },
		//FIXME
		// This test throws an EmptyStackException during the INSERT
		//{ "create table  U (SNAME varchar(32000), TNAME Varchar(32000), C1 bigint)", null },
		//{ "insert into U(SNAME, TNAME, C1) select distinct SCHEMANAME, TABLENAME, 2 from SYS.SYSTABLES T join  SYS.SYSSCHEMAS S on T.SCHEMAID = S.SCHEMAID where TABLENAME = 'U'", null },
		//{ "select * from U", new String[][] { {"APP","U","2"} } },  
		//{ "drop table  U", null },
		{ "create table d3310 (x bigint)", null },
		// This also throws EmptyStackException
		//{ "insert into d3310 select distinct * from (values 2.0, 2.1, 2.2) v", null },
		//{ "select * from d3310", new String[][] { {"2"},{"2"},{"2"} } },
		{ "drop table d3310", null }
	    };

	    // Start 1 client and 3 servers, use default partitioning
	    startVMs(1, 3);


	    Connection conn = TestUtil.getConnection();
	    Statement stmt = conn.createStatement();
	    // Go through the array, execute each string[0], check sqlstate [1]
	    // This will fail on the first one that succeeds where it shouldn't
	    // or throws unknown exception
	    JDBC.SQLUnitTestHelper(stmt,Script_InsertUT);
	  }

}
