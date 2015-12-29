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

public class LangScripts_UnionDUnit extends DistributedSQLTestBase {

	public LangScripts_UnionDUnit(String name) {
		super(name);
		// TODO Auto-generated constructor stub
	}
	 // This test is the as-is LangScript conversion, without any partitioning clauses
	  public void testLangScript_UnionTestNoPartitioning() throws Exception
	  {
	    // This is a JUnit conversion of the Derby Lang Union.sql script
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
	    Object[][] Script_UnionUT = {
		// this test shows union functionality
	    // FIXME
	    // This whole test is pretty much blocked for partitioned tables, as set ops on partitioned tables is not supported
	    // Therefore, all tables are made replicated
		{ "create table t1 (i int, s smallint, d double precision, r real, c10 char(10),c30 char(30), vc10 varchar(10), vc30 varchar(30)) REPLICATE", null },
	     { "create table t2 (i int, s smallint, d double precision, r real, c10 char(10),	c30 char(30), vc10 varchar(10), vc30 varchar(30)) REPLICATE", null },
	     { "create table dups (i int, s smallint, d double precision, r real, c10 char(10),c30 char(30), vc10 varchar(10), vc30 varchar(30)) REPLICATE", null },
		{ "insert into t1 values (null, null, null, null, null, null, null, null)", null },
	     { "insert into t1 values (1, 1, 1e1, 1e1, '11111', '11111     11', '11111','11111      11')", null },
	     { "insert into t1 values (2, 2, 2e1, 2e1, '22222', '22222     22', '22222',	'22222      22')", null },
	     { "insert into t2 values (null, null, null, null, null, null, null, null)", null },
	     { "insert into t2 values (3, 3, 3e1, 3e1, '33333', '33333     33', '33333','33333      33')", null },
	     { "insert into t2 values (4, 4, 4e1, 4e1, '44444', '44444     44', '44444',	'44444      44')", null },
	     // FIXME
	     // This UNION ALL does not insert the correct rows for a peer client in a multi-server cluster
	     // To workaround, instead insert from T1 and then T2 - this needs to be fixed
	    // { "insert into dups select * from t1 union all select * from t2", null },
	     {"insert into dups select * from t1", null },
	     {"insert into dups select * from t2", null },
		{ "values (1, 2, 3, 4) union values (5, 6, 7, 8)", new String[][] {
			{"1","2","3","4"},
			{"5","6","7","8"} } },
	     { "values (1, 2, 3, 4) union values (1, 2, 3, 4)", new String[][] { 	{"1","2","3","4"} } } ,
	     { "values (1, 2, 3, 4) union distinct values (5, 6, 7, 8)", new String[][] {
			{"1","2","3","4"},
			{"5","6","7","8"} } },
	     { "values (1, 2, 3, 4) union distinct values (1, 2, 3, 4)", new String[][] { 	{"1","2","3","4"} } } ,
	     { "values (1, 2, 3, 4) union values (5, 6, 7, 8) union values (9, 10, 11, 12)", new String[][] {
			{"1","2","3","4"},
			{"5","6","7","8"}, {"9","10","11","12"} } },
	     { "values (1, 2, 3, 4) union values (1, 2, 3, 4) union values (1, 2, 3, 4)", new String[][] { 	{"1","2","3","4"} } } ,
	     { "select * from t1 union select * from t2", new String[][] {
			{"1","1","10.0","10.0","11111","11111     11","11111","11111      11"},
			{"2","2","20.0","20.0","22222","22222     22","22222","22222      22"},
			{"3","3","30.0","30.0","33333","33333     33","33333","33333      33"},
			{"4","4","40.0","40.0","44444","44444     44","44444","44444      44"},
			{null,null,null,null,null,null,null,null} } },
	     { "select * from t1 union select * from t1", new String[][] {
			{"1","1","10.0","10.0","11111","11111     11","11111","11111      11"},
			{"2","2","20.0","20.0","22222","22222     22","22222","22222      22"},
			{null,null,null,null,null,null,null,null} } },
	     { "select * from t1 union select * from t2 union select * from dups", new String[][] {
			{"1","1","10.0","10.0","11111","11111     11","11111","11111      11"},
			{"2","2","20.0","20.0","22222","22222     22","22222","22222      22"},
			{"3","3","30.0","30.0","33333","33333     33","33333","33333      33"},
			{"4","4","40.0","40.0","44444","44444     44","44444","44444      44"},
			{null,null,null,null,null,null,null,null} } },
	     { "select * from t1 union select i, s, d, r, c10, c30, vc10, vc30 from t2", new String[][] {
			{"1","1","10.0","10.0","11111","11111     11","11111","11111      11"},
			{"2","2","20.0","20.0","22222","22222     22","22222","22222      22"},
			{"3","3","30.0","30.0","33333","33333     33","33333","33333      33"},
			{"4","4","40.0","40.0","44444","44444     44","44444","44444      44"},
			{null,null,null,null,null,null,null,null} } },
	     { "select * from t1 union select i, s, d, r, c10, c30, vc10, vc30 from t2	union select * from dups", new String[][] {
			{"1","1","10.0","10.0","11111","11111     11","11111","11111      11"},
			{"2","2","20.0","20.0","22222","22222     22","22222","22222      22"},
			{"3","3","30.0","30.0","33333","33333     33","33333","33333      33"},
			{"4","4","40.0","40.0","44444","44444     44","44444","44444      44"},
			{null,null,null,null,null,null,null,null} } },
		{ "select * from (values (1, 2, 3, 4) union values (5, 6, 7, 8)) a", new String[][] {
			{"1","2","3","4"},
			{"5","6","7","8"} } },
	     { "select * from (values (1, 2, 3, 4) union values (5, 6, 7, 8) union values (1, 2, 3, 4)) a", new String[][] {
			{"1","2","3","4"},
			{"5","6","7","8"} } },
		{ "select i from t1 union select i from t2 union all select i from dups", new String[][] { {"1"},{"2"},{"3"},{"4"},{null},
			{null},{"1"},{"2"},{null},{"3"},{"4"} } },
	    { "(select i from t1 union select i from t2) union all select i from dups", new String[][] { {"1"},{"2"},{"3"},{"4"},{null},
			{null},{"1"},{"2"},{null},{"3"},{"4"} } },
	     { "select i from t1 union (select i from t2 union all select i from dups)", new String[][] { {"1"},{"2"},{"3"},{"4"},{null} } },
	     { "select i from t1 union all select i from t2 union select i from dups", new String[][] { {"1"},{"2"},{"3"},{"4"},{null} } },
	     { "(select i from t1 union all select i from t2) union select i from dups", new String[][] { {"1"},{"2"},{"3"},{"4"},{null} } },
	     { "select i from t1 union all (select i from t2 union select i from dups)", new String[][] { {null},{"1"},{"2"},{"1"},{"2"},{"3"},{"4"},{null} } },
		{ "select a.i, b.i from t1 a, t2 b union select b.i, a.i from t1 a, t2 b", new String[][] {
			{"1","3"},{"1","4"},{"1",null},{"2","3"},{"2","4"},{"2",null},
			{"3","1"},{"3","2"},{"3",null},{"4","1"},{"4","2"},{"4",null},
			{null,"1"},{null,"2"},{null,"3"},{null,"4"},{null,null} } },
	     { "values (9, 10) union select a.i, b.i from t1 a, t2 b union select b.i, a.i from t1 a, t2 b", new String[][] {
			{"9","10"},{"1","3"},{"1","4"},{"1",null},{"2","3"},{"2","4"},{"2",null},
			{"3","1"},{"3","2"},{"3",null},{"4","1"},{"4","2"},{"4",null},
			{null,"1"},{null,"2"},{null,"3"},{null,"4"},{null,null} } },
	     { "select a.i, b.i from t1 a, t2 b union select b.i, a.i from t1 a, t2 b union values (9, 10)", new String[][] {
			{"9","10"},{"1","3"},{"1","4"},{"1",null},{"2","3"},{"2","4"},{"2",null},
			{"3","1"},{"3","2"},{"3",null},{"4","1"},{"4","2"},{"4",null},
			{null,"1"},{null,"2"},{null,"3"},{null,"4"},{null,null} } },
		{ "select i from t1 where i = (values 1 union values 1)", new String[][] { {"1"} } },
	     { "select i from t1 where i = (values 1 union values 1 union values 1)", new String[][] { {"1"} } },
	     { "select i from t1 where i = (select 1 from t2 union values 1)", new String[][] { {"1"} } },
	     { "select i from t1 where i in (select i from t2 union values 1 union values 2)", new String[][] { {"1"},{"2"} } },
	     { "select i from t1 where i in (select a from (select i from t2 union values 1 union values 2) a (a))", new String[][] { {"1"},{"2"} } },
	     { "select i from t1 where i not in (select i from t2 union values 1 union values 2)", new String[0][0] },
	     { "select i from t1 where i not in (select i from t2 where i is not null union values 1 union values 22)", new String[][] { {"2"} } },
	     { "select i from t1 where i not in (select a from (select i from t2 where i is not null union values 111 union values 2) a (a))", new String[][] { {"1"} } },
	     { "select i from t1 a where i in (select i from t2 where 1 = 0 union select a.i from t2 where a.i < i)", new String[][] { {"1"},{"2"} } },
	     { "select i from t1 a where i in (select a.i from t2 where a.i < i union select i from t2 where 1 < 0)", new String[][] { {"1"},{"2"} } },
	     { "select i from t1 where exists (select * from t2 union select * from t2)", new String[][] { {"1"},{"2"},{null} } },
	     { "select i from t1 where exists (select 1 from t2 union select 2 from t2)", new String[][] { {"1"},{"2"},{null} } },
	     { "select i from t1 where exists (select 1 from t2 where 1 = 0 union select 2 from t2 where t1.i < i)", new String[][] { {"1"},{"2"} } },
	     { "select i from t1 where exists (select i from t2 where t1.i < i union select i from t2 where 1 = 0 union select i from t2 where t1.i < i union    select i from t2 where 1 = 0)", new String[][] { {"1"},{"2"} } },
		{ "select i from t1 where exists (select 1 from t2 where 1 = 0 union select * from t2 where t1.i < i)", "42X58" },
	     { "select i from t1 where exists (select i from t2 where t1.i < i union select * from t2 where 1 = 0 union select * from t2 where t1.i < i union    select i from t2 where 1 = 0)", "42X58" },
	     { "select i from t1 union select i from dups order by i desc", new String[][] { {null},{"4"},{"3"},{"2"},{"1"} } },
		{ "create table insert_test (i int, s smallint, d double precision, r real,c10 char(10), c30 char(30), vc10 varchar(10), vc30 varchar(30))", null },
		//FIXME
		// INSERT from a UNION subselect does not insert the proper number of rows, for multi-server clusters
		//{ "insert into insert_test select * from t1 union select * from dups", null },
	     //{ "select * from insert_test", new String[][] {
		//	{"1","1","10.0","10.0","11111","11111     11","11111","11111      11"},
		//	{"2","2","20.0","20.0","22222","22222     22","22222","22222      22"},
		//	{"3","3","30.0","30.0","33333","33333     33","33333","33333      33"},
		//	{"4","4","40.0","40.0","44444","44444     44","44444","44444      44"},
		//	{null,null,null,null,null,null,null,null} } },
	     //{ "delete from insert_test", null },
	     //{ "insert into insert_test (s, i) values (2, 1) union values (4, 3)", null },
	     //{ "select * from insert_test", new String[][] {
		//	{"1","2",null,null,null,null,null,null},
		//	{"3","4",null,null,null,null,null,null} } },
	     //{ "delete from insert_test", null },
		//{ "insert into insert_test (vc30) select vc10 from t1 union select c30 from t2", null },
	     //{ "select * from insert_test", new String[][] {
		//	{null,null,null,null,null,null,null,"11111"},
		//	{null,null,null,null,null,null,null,"22222"},
		//	{null,null,null,null,null,null,null,"33333     33"},
		//	{null,null,null,null,null,null,null,"44444     44"},
		//	{null,null,null,null,null,null,null,null} } },
	     //{ "delete from insert_test", null },
		{ "insert into insert_test values (1, 2) union values (3, 4)", "42802" },
		{ "values (1) union values (1.1)", new String[][] { {"1.0"},{"1.1"} } },
	     { "values (1) union values (1.1e1)", new String[][] { {"1.0"},{"11.0"} } },
	     { "values (1.1) union values (1)", new String[][] { {"1.0"},{"1.1"} } },
	     { "values (1.1e1) union values (1)", new String[][] { {"1.0"},{"11.0"} } },
		{ "values (x'aa') union values (1)", "42X61" },
		{ "drop table t1", null },
	     { "drop table t2", null },
	     { "drop table dups", null },
	     { "drop table insert_test", null },
		{ "create table t1 (i int, s smallint, d double precision, r real, c10 char(10), c30 char(30), vc10 varchar(10), vc30 varchar(30))", null },
	     { "create table t2 (i int, s smallint, d double precision, r real, c10 char(10), c30 char(30), vc10 varchar(10), vc30 varchar(30))", null },
		{ "insert into t1 values (null, null, null, null, null, null, null, null)", null },
	     { "insert into t1 values (1, 1, 1e1, 1e1, '11111', '11111     11', '11111','11111      11')", null },
	     { "insert into t1 values (2, 2, 2e1, 2e1, '22222', '22222     22', '22222','22222      22')", null },
	     { "insert into t2 values (null, null, null, null, null, null, null, null)", null },
	     { "insert into t2 values (3, 3, 3e1, 3e1, '33333', '33333     33', '33333','33333      33')", null },
	     { "insert into t2 values (4, 4, 4e1, 4e1, '44444', '44444     44', '44444','44444      44')", null },
		{ "select * from t1 union all select * from t1, t2", "42X58" },
	     { "select * from t1 union all values (1, 2, 3, 4)", "42X58" },
	     { "values (1, 2, 3, 4) union all select * from t1", "42X58" },
		{ "values (1, 2, 3, 4) union all values (5, 6, 7, 8)", new String[][] {
			{"1","2","3","4"},
			{"5","6","7","8"} } },
	     { "values (1, 2, 3, 4) union all values (5, 6, 7, 8) union all values (9, 10, 11, 12)", new String[][] {
			{"1","2","3","4"},
			{"5","6","7","8"}, {"9","10","11","12"} } },
		{ "select * from (values (1, 2, 3, 4) union all values (5, 6, 7, 8)) a", new String[][] {
			{"1","2","3","4"},
			{"5","6","7","8"} } },
	     { "select * from (values (1, 2, 3, 4) union all values (5, 6, 7, 8)) a (a, b, c, d)", new String[][] {
			{"1","2","3","4"},
			{"5","6","7","8"} } },
	     { "select b, d from (values (1, 2, 3, 4) union all values (5, 6, 7, 8)) a (a, b, c, d)", new String[][] {
			{"2","4"},	{"6","8"} } },
		 // FIXME
		 // Nested unions not supported in GemFireXD for partitioned regions
	     //{ "select * from (select i, s, c10, vc10 from t1 union all select i, s, c10, vc10 from t2) a (j, k, l, m), (select i, s, c10, vc10 from t1 union all select i, s, c10, vc10 from t2) b (j, k, l, m) where a.j = b.j", new String[][] {
		//	{"1","1","11111","11111","1","1","11111","11111"},
		//	{"2","2","22222","22222","2","2","22222","22222"},
		//	{"3","3","33333","33333","3","3","33333","33333"},
		//	{"4","4","44444","44444","4","4","44444","44444"} } },
		{ "select i from t1 where i = (select * from t2 union all select 1 from t1)", "42X38" },
	     { "select i from t1 where i = (select 1 from t2 union all select * from t1)", "42X38" },
	     { "select i from t1 where i = (values (1, 2, 3) union all values (1, 2, 3))", "42X39" },
	     { "select i from t1 where i = (select i, s from t2 union all select i, s from t1)", "42X39" },
	     { "select i from t1 where i = (values 1 union all values 1)", "21000" },
	     { "select i from t1 where i in (select date('1999-02-04') from t2 union all select date('1999-03-08') from t2)", "42818" },
	     //FIXME
	     // Set operators not allowed in subqueries for partitioned regions
		//{ "select i from t1 where i = (select i from t2 where 1 = 0 union all values 1)", new String[][] { {"1"} } },
		//{ "select i from t1 where i in (select i from t2 union all values 1 union all values 2)", new String[][] { {"1"},{"2"} } },
	     //{ "select i from t1 where i in (select a from (select i from t2 union all values 1 union all values 2) a (a))", new String[][] { {"1"},{"2"} } },
		//{ "select i from t1 where i not in (select i from t2 union all values 1 union all values 2)", new String[0][0] },
	     //{ "select i from t1 where i not in (select i from t2 where i is not null union all values 1 union all values 22)", new String[][] { {"2"} } },
	     //{ "select i from t1 where i not in (select a from (select i from t2 where i is not null union all values 111 union all values 2) a (a))", new String[][] { {"1"} } },
		//{ "select i from t1 a where i in (select i from t2 where 1 = 0 union all select a.i from t2 where a.i < i)", new String[][] { {"1"},{"2"} } },
	     //{ "select i from t1 a where i in (select a.i from t2 where a.i < i union all select i from t2 where 1 < 0)", new String[][] { {"1"},{"2"} } },
		//{ "select i from t1 where exists (select * from t2 union all select * from t2)", new String[][] { {"1"},{"2"},{null} } },
	     //{ "select i from t1 where exists (select 1 from t2 union all select 2 from t2)", new String[][] { {"1"},{"2"},{null} } },
	     //{ "select i from t1 where exists (select 1 from t2 where 1 = 0 union all select 2 from t2 where t1.i < i)", new String[][] { {"1"},{"2"} } },
	     //{ "select i from t1 where exists (select i from t2 where t1.i < i union all select i from t2 where 1 = 0 union all select i from t2 where t1.i < i union all select i from t2 where 1 = 0)", new String[][] { {"1"},{"2"} } },
		//{ "select i from t1 where exists (select 1 from t2 where 1 = 0 union all select * from t2 where t1.i < i)", "42X58" },
	     //{ "select i from t1 where exists (select i from t2 where t1.i < i union all select * from t2 where 1 = 0 union all select * from t2 where t1.i < i union all select i from t2 where 1 = 0)", "42X58" },
	     //{ "select vc10 from (select vc10 from t1 union all select vc10 from t1 union all select vc10 from t1 union all select vc10 from t1 union all select vc10 from t1 union all select vc10 from t1 union all select vc10 from t1) t", new String[][] {
		//	{null},{"11111"},{"22222"},
		//	{null},{"11111"},{"22222"},
		//	{null},{"11111"},{"22222"},
		//	{null},{"11111"},{"22222"},
		//	{null},{"11111"},{"22222"},
		//	{null},{"11111"},{"22222"},
		//	{null},{"11111"},{"22222"} } },
		//{ "select vc10 from (select vc10 from t1 union all (select vc10 from t1 union all select vc10 from t1)) t", new String[][] {
		//	{null},{"11111"},{"22222"},
		//	{null},{"11111"},{"22222"},
		//	{null},{"11111"},{"22222"} } },
		{ "drop table t1", null },
	     { "drop table t2", null },
		{ "create table a (f1 varchar(10))", null },
	     { "create table b (f2 varchar(10))", null },
	     { "insert into b values('test')", null },
	     { "select nullif('x','x') as f0, f1 from a union all select nullif('x','x') as f0, nullif('x','x') as f1 from b", new String[][] { {null,null} } },
	     { "drop table a", null },
	     { "drop table b", null },
	     { "create table a (f1 int)", null },
	     { "create table b (f2 int)", null },
	     { "insert into b values(1)", null },
	     { "select nullif('x','x') as f0, f1 from a union all select nullif('x','x') as f0, nullif(1,1) as f1 from b", new String[][] { {null,null} } },
	     { "drop table a", null },
	     { "drop table b", null },
		{ "create table o (name varchar(20), ord int)", null },
	     { "create table a (ord int, amount int)", null },
	     { "create view v1 (vx, vy) as select name, sum(ord) from o where ord > 0 group by name, ord having ord <= ANY (select ord from a)", null },
	     //FIXME
	     // View over groupby not supported on partitioned tables
	     //{ "select vx, vy from v1 union select vx, sum(vy) from v1 group by vx, vy having (vy / 2) > 15", new String[0][0] },
	     { "drop view v1", null },
	     { "drop table o", null },
	     { "drop table a", null },
	     // FIXME
	     // These should be partitioned and colocated but too many restrictions on partitioned tables in below queries
		{ "create table t1 (i int, j int) REPLICATE", null },
	     { "create table t2 (i int, j int) REPLICATE", null },
	     { "insert into t1 values (1, 2), (2, 4), (3, 6), (4, 8), (5, 10)", null },
	     { "insert into t2 values (1, 2), (2, -4), (3, 6), (4, -8), (5, 10)", null },
	     { "insert into t2 values (3, 6), (4, 8), (3, -6), (4, -8)", null },
		{ "select * from t1 union select * from t2 union select * from t1", new String[][] {
			{"1","2"},{"2","-4"},{"2","4"},{"3","-6"},{"3","6"},{"4","-8"},{"4","8"},{"5","10"} } },
		{ "select * from (select * from t1 union select * from t2 union select * from t1) x", new String[][] {
			{"1","2"},{"2","-4"},{"2","4"},{"3","-6"},{"3","6"},{"4","-8"},{"4","8"},{"5","10"} } },
		{ "create view uv as select * from t1 union select * from t2 union select * from t1", null },
	     { "select * from uv", new String[][] {
			{"1","2"},{"2","-4"},{"2","4"},{"3","-6"},{"3","6"},{"4","-8"},{"4","8"},{"5","10"} } },
	     { "drop view uv", null },
		{ "select * from t1 union select * from t2 union all select * from t1", new String[][] {
			{"1","2"},{"2","4"},{"3","6"},{"4","8"},{"5","10"},
			{"1","2"},{"2","-4"},{"2","4"},{"3","-6"},{"3","6"},{"4","-8"},{"4","8"},{"5","10"} } },
		{ "select * from (select * from t1 union select * from t2 union all select * from t1) x", new String[][] {
			{"1","2"},{"2","4"},{"3","6"},{"4","8"},{"5","10"},
			{"1","2"},{"2","-4"},{"2","4"},{"3","-6"},{"3","6"},{"4","-8"},{"4","8"},{"5","10"} } },
		{ "select * from t1 union select * from t2 except select * from t1", new String[][] {
			{"2","-4"},{"3","-6"},{"4","-8"} } },
		{ "select * from (select * from t1 union select * from t2 except select * from t1) x", new String[][] {
			{"2","-4"},{"3","-6"},{"4","-8"} } },
		{ "select * from t1 union select * from t2 except all select * from t1", new String[][] {
			{"2","-4"},{"3","-6"},{"4","-8"} } },
		{ "select * from (select * from t1 union select * from t2 except all select * from t1) x", new String[][] {
			{"2","-4"},{"3","-6"},{"4","-8"} } },
		{ "(select * from t1 union select * from t2) intersect select * from t2", new String[][] {
			{"1","2"},{"2","-4"},{"3","-6"},{"3","6"},{"4","-8"},{"4","8"},{"5","10"} } },
		{ "create view iv as (select * from t1 union select * from t2) intersect select * from t2", null },
	     { "select * from iv", new String[][] {
			{"1","2"},{"2","-4"},{"3","-6"},{"3","6"},{"4","-8"},{"4","8"},{"5","10"} } },
	     { "drop view iv", null },
		{ "(select * from t1 union select * from t2) intersect all select * from t2", new String[][] {
			{"1","2"},{"2","-4"},{"3","-6"},{"3","6"},{"4","-8"},{"4","8"},{"5","10"} } },
		{ "create view iv as (select * from t1 union select * from t2) intersect all select * from t2", null },
	     { "select * from iv", new String[][] {
			{"1","2"},{"2","-4"},{"3","-6"},{"3","6"},{"4","-8"},{"4","8"},{"5","10"} } },
	     { "drop view iv", null },
		{ "select * from (select * from t1 union select * from t2) x2 left join t2 on x2.i = t2.i", new String[][] {
			{"1","2","1","2"},
			{"2","-4","2","-4"},
			{"2","4","2","-4"},
			{"3","-6","3","6"},
			{"3","-6","3","6"},
			{"3","-6","3","-6"},
			{"3","6","3","6"},
			{"3","6","3","6"},
			{"3","6","3","-6"},
			{"4","-8","4","-8"},
			{"4","-8","4","8"},
			{"4","-8","4","-8"},
			{"4","8","4","-8"},
			{"4","8","4","8"},
			{"4","8","4","-8"},
			{"5","10","5","10"} } },
		{ "drop table t1", null },
	     { "drop table t2", null }
	   };

	    // Start 1 client and 3 servers, use default partitioning
	    startVMs(1, 3);


	    Connection conn = TestUtil.getConnection();
	    Statement stmt = conn.createStatement();
	    // Go through the array, execute each string[0], check sqlstate [1]
	    // This will fail on the first one that succeeds where it shouldn't
	    // or throws unknown exception
	    JDBC.SQLUnitTestHelper(stmt,Script_UnionUT);
	  }

}
