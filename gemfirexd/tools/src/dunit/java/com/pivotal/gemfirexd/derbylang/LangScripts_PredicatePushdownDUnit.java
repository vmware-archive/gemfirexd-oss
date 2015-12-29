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

public class LangScripts_PredicatePushdownDUnit extends
		DistributedSQLTestBase {

  @Override
  protected String reduceLogging() {
    return "config";
  }

	public LangScripts_PredicatePushdownDUnit(String name) {
		super(name);
	}
	
	  public void testLangScript_PredicatePushdownTest() throws Exception
	  {
	    // This is a JUnit conversion of the Derby Lang PredicatePushdown.sql script
	    // without any GemFireXD extensions
		  
	    // Catch exceptions from illegal syntax
	    // Tests still not fixed marked FIXME
		  
	    // Array of SQL text to execute and sqlstates to expect
	    // The first object is a String, the second is either 
	    // 1) null - this means query returns no rows and throws no exceptions
	    // 2) a string - this means query returns no rows and throws expected SQLSTATE
	    // 3) a String array - this means query returns rows which must match (unordered) given resultset
	    //       - for an empty result set, an uninitialized size [0][0] array is used
	    Object[][] Script_PredicatePushdownUT = {
		// The original test checked against runtime plans. GemFireXD will not, just check results.
	    // FIXME
	    // This test has complex joins and colocation is often impossible. Until colocation is not needed, use replicated tables instead
		{ "CREATE TABLE T1 (I INTEGER, J INTEGER) REPLICATE", null },
		{ "insert into t1 values (1, 2), (2, 4), (3, 6), (4, 8), (5, 10)", null },
		{ "CREATE TABLE T2 (I INTEGER, J INTEGER) REPLICATE", null },
		{ "insert into t2 values (1, 2), (2, -4), (3, 6), (4, -8), (5, 10)", null },
		{ "CREATE TABLE T3 (A INTEGER, B INTEGER) REPLICATE", null },
		{ "insert into T3 values (1,1), (2,2), (3,3), (4,4), (6, 24),(7, 28), (8, 32), (9, 36), (10, 40)", null },
		{ "insert into t3 (a) values 11, 12, 13, 14, 15, 16, 17, 18, 19, 20", null },
		{ "update t3 set b = 2 * a where a > 10", null },
		{ "CREATE TABLE T4 (A INTEGER, B INTEGER) REPLICATE", null },
		{ "insert into t4 values (3, 12), (4, 16)", null },
		{ "insert into t4 (a) values 11, 12, 13, 14, 15, 16, 17, 18, 19, 20", null },
		{ "update t4 set b = 2 * a where a > 10", null },
		{ "create view V1 as select i, j from T1 union select i,j from T2", null },
		{ "create view V2 as select a,b from T3 union select a,b from T4", null },
		{ "select * from V1, V2 where V1.j = V2.b", new String[][] {
			{"1","2","2","2"},{"2","4","4","4"} } },
		{ "select * from V2, V1 where V1.j = V2.b", new String[][] {
			{"2","2","1","2"},{"4","4","2","4"} } },
		{ "select * from  (select * from t1 union    select * from t2 union      select * from t1 union        select * from t2) x1,(select * from t3 union select * from t4 union  select * from t4 ) x2 where x1.i = x2.a", new String[][] {
			{"1","2","1","1"},{"2","-4","2","2"},{"2","4","2","2"},
			{"3","6","3","3"},{"3","6","3","12"},{"4","-8","4","4"},
			{"4","-8","4","16"},{"4","8","4","4"},{"4","8","4","16"} } },
		{ "select * from (select * from t1 union all select * from t2) x1, (select * from t3 union select * from t4) x2 where x1.i = x2.a", new String[][] {
			{"1","2","1","1"},{"2","4","2","2"},{"3","6","3","3"},
			{"3","6","3","12"},{"4","8","4","4"},{"4","8","4","16"},
			{"1","2","1","1"},{"2","-4","2","2"},{"3","6","3","3"},
			{"3","6","3","12"},{"4","-8","4","4"},{"4","-8","4","16"} } },
		{ "select * from (select * from t1 union select * from t2) x1, (select * from t3 union all select * from t4) x2 where x1.i = x2.a", new String[][] {
			{"1","2","1","1"},{"2","-4","2","2"},{"2","4","2","2"},
			{"3","6","3","3"},{"3","6","3","12"},{"4","-8","4","4"},
			{"4","-8","4","16"},{"4","8","4","4"},{"4","8","4","16"} } },
		{ "select * from (select * from t1 union all select * from t2) x1, (select * from t3 union all select * from t4) x2 where x1.i = x2.a", new String[][] {
			{"1","2","1","1"},{"2","4","2","2"},{"3","6","3","3"},
			{"3","6","3","12"},{"4","8","4","4"},{"4","8","4","16"},
			{"1","2","1","1"},{"2","-4","2","2"},{"3","6","3","3"},
			{"3","6","3","12"},{"4","-8","4","4"},{"4","-8","4","16"} } },
		{ "create table tc (c1 char, c2 char, c3 char, c int) REPLICATE", null },
		{ "create view vz (z1, z2, z3, z4) as select distinct xx1.c1, xx1.c2, 'bokibob' bb, xx1.c from (select c1, c, c2, c3 from tc) xx1 union select 'i','j','j',i from t2", null },
		{ "create view vz2 (z1, z2, z3, z4) as select distinct xx1.c1, xx1.c2, 'bokibob' bb, xx1.c from (select c1, c, c2, c3 from tc) xx1", null },
		{ "create view vz3 (z1, z2, z3, z4) as select distinct xx1.c1, xx1.c2, 'bokibob' bb, xx1.c from (select c1, c, c2, 28 from tc) xx1 union select 'i','j','j',i from t2", null },
		{ "create view vz4 (z1, z2, z3, z4) as select distinct xx1.c1, xx1.c2, 'bokibob' bb, xx1.c from (select c1, c, c2, 28 from tc) xx1 union select 'i','j','j',i from t2 union select c1, c2, c3, c from tc", null },
		{ "create view vz5a (z1, z2, z3, z4) as select distinct xx1.c1, xx1.c2, 'bokibob' bb, xx1.c from (select c1, c2, c3, c from t2, tc where tc.c = t2.i) xx1 union select 'i','j','j',i from t2", null },
		{ "create view vz5b (z1, z2, z3, z4) as select distinct xx1.c1, xx1.c2, 'bokibob' bb, xx1.c from (select c1, c2, c3, c from t2, (select distinct * from tc) tc where tc.c = t2.i) xx1 union select 'i','j','j',i from t2", null },
		{ "create view vz5c (z1, z2, z3, z4) as select distinct xx1.c1, xx1.c2, 'bokibob' bb, xx1.c from (select c1, c2, c3, c from t2, (select * from tc union select * from tc) tc where tc.c = t2.i) xx1 union select 'i','j','j',i from t2", null },
		{ "create view vz5d (z1, z2, z3, z4) as select distinct xx1.c1, xx1.c2, 'bokibob' bb, xx1.c from (select c1, c2, c3, c from t2, (select * from tc         union select z1 c1, z2 c2, z3 c3, z4 c from vz5b) tc where tc.c = t2.i) xx1 union select 'i','j','j',i from t2", null },
		{ "select x1.c1 from (select count(*) from t1 union select count(*) from t2) x1 (c1), (select count(*) from t3 union select count(*) from t4) x2 (c2) where x1.c1 = x2.c2", new String[0][0] },
		{ "select x1.c1 from (select count(*) from (select distinct j from t1) xx1 union select count(*) from t2) x1 (c1), (select count(*) from t3 union select count(*) from t4) x2 (c2) where x1.c1 = x2.c2", new String[0][0] },
		{ "select x1.c1 from (select count(*) from (select distinct j from t1 union select distinct j from t2) xx1 union select count(*) from t2) x1 (c1),  (select count(*) from t3 union select count(*) from t4) x2 (c2) where x1.c1 = x2.c2", new String[0][0] },
		{ "select x1.c1 from (select xx1.c from (select distinct c, c1 from tc) xx1 union select count(*) from t2) x1 (c1),(select count(*) from t3 union select count(*) from t4) x2 (c2) where x1.c1 = x2.c2", new String[0][0] },
		{ "select x1.c1 from (select xx1.c from (select c, c1 from tc) xx1 union select count(*) from t2) x1 (c1),(select count(*) from t3 union select count(*) from t4) x2 (c2) where x1.c1 = x2.c2", new String[0][0] },
		//FIXME
		// Throws ASSERT FAILED VirtualColumnId (4) does not agree with position within Vector (3)
		//{ "select x1.z1 from (select xx1.c1, xx1.c2, xx1.c, xx1.c3 from (select c1, c2, c3, c from tc) xx1 union select 'i','j',j,'i' from t2) x1 (z1, z2, z3, z4),(select count(*) from t3 union select count (*) from t4) x2 (c2) where x1.z3 = x2.c2", new String[0][0] },
		//FIXME
		// Throws ASSERT FAILED Failed to locate scope target result column when trying to scope operand 'X1.SQLCol4'.
		//{ "select x1.z1 from (select xx1.c1, xx1.c2, xx1.c, xx1.c3 from (select c1, c2, c3, c from tc) xx1 union select 'i','j',j,'i' from t2) x1 (z1, z2, z3, z4),(select a from t3 union select count (*) from t4) x2 (c2) where x1.z3 = x2.c2", new String[][] { {"i"},{"i"},{"i"} } },
		//FIXME
		// Throws ASSERT FAILED VirtualColumnId (4) does not agree with position within Vector (3)
		//{ "select x1.z1 from (select xx1.c1, xx1.c2, xx1.c, xx1.c3 from (select c1, c2, c3, c from tc) xx1 union select 'i','j',j,'i' from t2) x1 (z1, z2, z3, z4), (select count(*) from t3 union select a from t4) x2 (c2) where x1.z3 = x2.c2", new String[0][0] },
		{ "select x1.c1 from (select count(*) from (select distinct j from t1) xx1 union select count(*) from t2) x1 (c1),(select a from t3 union select a from t4) x2 (c2) where x1.c1 = x2.c2", new String[0][0] },
		{ "select x1.c1 from (select count(*) from (select distinct j from t1) xx1 union select i from t2) x1 (c1),(select a from t3 union select a from t4) x2 (c2) where x1.c1 = x2.c2", new String[][] { {"1"},{"2"},{"3"},{"4"} } },
		{ "select x1.c1 from (select count(*) from (select distinct j from t1) xx1 union select count(*) from t2) x1 (c1), (select i from t2 union select i from t1) x2 (c2) where x1.c1 = x2.c2", new String[][] { {"5"} } },
		{ "select x1.c1 from (select count(*) from (select distinct j from t1) xx1 union select count(*) from t2) x1 (c1), (select 1 from t2 union select i from t1) x2 (c2) where x1.c1 = x2.c2", new String[][] { {"5"} } },
		//FIXME
		//Throws ASSERT FAILED Failed to locate scope target result column when trying to scope operand 'null.SQLCol4'
		//{ "select x1.z4 from (select z1, z4, z3 from vz union select '1', 4, '3' from t1) x1 (z1, z4, z3), (select distinct j from t2 union select j from t1) x2 (c2) where x1.z4 = x2.c2", new String[][] { {"4"},{"2"},{"4"} } },
		//{ "select x1.z4, x2.c2 from (select z1, z4, z3 from vz union select '1', i+1, '3' from t1) x1 (z1, z4, z3), (select distinct j from t2 union select j from t1) x2 (c2) where x1.z4 = x2.c2", new String[][] {
		//	{"2","2"},{"4","4"},{"6","6"},{"2","2"},{"4","4"} } },
		{ "select x1.z4 from (select z1, z4, z3 from vz2 union select '1', 4, '3' from t1) x1 (z1, z4, z3), (select distinct j from t2 union select j from t1) x2 (c2) where x1.z4 = x2.c2", new String[][] { {"4"} } },
		//FIXME
		// Throws ASSERT FAILED Failed to locate scope target result column when trying to scope operand 'null.SQLCol5
		//{ "select x1.z4, x2.c2 from (select z1, z4, z3 from vz4 union select '1', i+1, '3' from t1) x1 (z1, z4, z3), (select distinct j from t2 union select j from t1) x2 (c2) where x1.z4 = x2.c2", new String[][] {
		//	{"2","2"},{"4","4"},{"6","6"},{"2","2"},{"4","4"} } },
		{ "select t1.i, vz5a.* from t1 left outer join vz5a on t1.i = vz5a.z4", new String[][] {
			{"1","i","j","j","1"},
			{"2","i","j","j","2"},
			{"3","i","j","j","3"},
			{"4","i","j","j","4"},
			{"5","i","j","j","5"} } },
		{ "select t1.i, vz5b.* from t1 left outer join vz5b on t1.i = vz5b.z4", new String[][] {
			{"1","i","j","j","1"},
			{"2","i","j","j","2"},
			{"3","i","j","j","3"},
			{"4","i","j","j","4"},
			{"5","i","j","j","5"} } },
		{ "select t1.i, vz5d.* from t1 left outer join vz5d on t1.i = vz5d.z4", new String[][] {
			{"1","i","j","bokibob","1"},
			{"1","i","j","j","1"},
			{"2","i","j","bokibob","2"},
			{"2","i","j","j","2"},
			{"3","i","j","bokibob","3"},
			{"3","i","j","j","3"},
			{"4","i","j","bokibob","4"},
			{"4","i","j","j","4"},
			{"5","i","j","bokibob","5"},
			{"5","i","j","j","5"} } },
		//FIXME
		//Throws ASSERT FAILED visibleSize() should match
		//{ "select x1.z4, x2.c2 from (select z1, z4, z3 from vz union select '1', i+1, '3' from t1) x1 (z1, z4, z3) left join (select distinct i,j from (select distinct i,j from t2) x3  union select i, j from t1) x2 (c1, c2) on x1.z4 = x2.c2", new String[][] {
		//	{"2","2"},{"3",null},{"4","4"},{"5",null},{"6","6"},
		//	{"1",null},{"2","2"},{"3",null},{"4","4"},{"5",null} } },
		//FIXME
		//Throws ASSERT FAILED Failed to locate scope target result column when trying to scope operand 'null.SQLCol4'.:
		//{ "select x1.z4, x2.c2 from (select distinct i,j from (select distinct j,i from t2) x3 union select i, j from t1) x2 (c1, c2) left join (select z1, z4, z3 from vz union select '1', i+1, '3' from t1) x1 (z1, z4, z3) on x1.z4 = x2.c2", new String[][] {
		//	{"2","2"},{"2","2"},{null,"-4"},{"4","4"},{"4","4"},
		//	{"6","6"},{null,"-8"},{null,"8"},{null,"10"} } },
		//{ "select x1.z4, x2.c2 from (select distinct i,j from (select distinct j,i from t2) x3 union select i, j from t1) x2 (c1, c2) left join (select z1, z4, z3 from vz union select '1', sin(i), '3' from t1) x1 (z1, z4, z3) on x1.z4 = x2.c2", new String[][] {
		//	{"2.0","2"},{null,"-4"},{"4.0","4"},{null,"6"},{null,"-8"},{null,"8"},{null,"10"} } },
		//{ "select x1.z4, x2.c2 from (select distinct i,j from (select distinct j,i from t2) x3 union select i, j from t1) x2 (c1, c2) left join (select z1, z4, z3 from vz union select '1', i, '3' from t1) x1 (z1, z4, z3) on x1.z4 = x2.c2", new String[][] {
		//	{"2","2"},{"2","2"},{null,"-4"},{"4","4"},{"4","4"},{null,"6"},{null,"-8"},{null,"8"},{null,"10"} } },
		//{ "select x1.z4, x2.c2 from (select distinct i,j from (select distinct j,i from t2) x3 union select i, j from t1) x2 (c1, c2) left join (select z1, z4, z3 from vz3 union select '1', sin(i), '3' from t1) x1 (z1, z4, z3) on x1.z4 = x2.c2", new String[][] {
		//	{"2.0","2"},{null,"-4"},{"4.0","4"},{null,"6"},{null,"-8"},{null,"8"},{null,"10"} } },
		//{ "select x1.z4, x2.c2 from (select distinct i,j from (select distinct j,i from t2) x3 union select i, j from t1) x2 (c1, c2) left join (select z1, z4, z3 from vz union select '1', sin(i), '3' from t1 union select '1', 14, '3' from t1) x1 (z1, z4, z3) on x1.z4 = x2.c2", new String[][] {
		//	{"2.0","2"},{null,"-4"},{"4.0","4"},{null,"6"},{null,"-8"},{null,"8"},{null,"10"} } },
		//{ "select x1.z4, x2.c2 from (select distinct i,j from (select distinct j,i from t2) x3 union select i, j from t1) x2 (c1, c2) left join (select '1', sin(i), '3' from t1 union select '1', 14, '3' from t1 union select z1, z4, z3 from vz) x1 (z1, z4, z3) on x1.z4 = x2.c2", new String[][] {
		//	{"2.0","2"},{null,"-4"},{"4.0","4"},{null,"6"},{null,"-8"},{null,"8"},{null,"10"} } },
		{ "drop view vz", null },
		{ "drop view vz2", null },
	     { "drop view vz3", null },
	     { "drop view vz4", null },
	     { "drop view vz5a", null },
	     { "drop view vz5d", null },
	     { "drop view vz5b", null },
	     { "drop view vz5c", null },
	     { "drop table tc", null },
	     { "insert into t3 (a) values 21, 22, 23, 24, 25, 26, 27, 28, 29, 30", null },
	     { "insert into t3 (a) values 31, 32, 33, 34, 35, 36, 37, 38, 39, 40", null },
	     { "insert into t3 (a) values 41, 42, 43, 44, 45, 46, 47, 48, 49, 50", null },
	     { "insert into t3 (a) values 51, 52, 53, 54, 55, 56, 57, 58, 59, 60", null },
	     { "insert into t3 (a) values 61, 62, 63, 64, 65, 66, 67, 68, 69, 70", null },
	     { "insert into t3 (a) values 71, 72, 73, 74, 75, 76, 77, 78, 79, 80", null },
	     { "insert into t3 (a) values 81, 82, 83, 84, 85, 86, 87, 88, 89, 90", null },
	     { "insert into t3 (a) values 91, 92, 93, 94, 95, 96, 97, 98, 99, 100", null },
	     { "update t3 set b = 2 * a where a > 20", null },
	     { "insert into t4 (a, b) (select a,b from t3 where a > 20)", null },
	     { "insert into t4 (a, b) (select a,b from t3 where a > 20)", null },
	     { "insert into t3 (a, b) (select a,b from t4 where a > 20)", null },
	     { "insert into t4 (a, b) (select a,b from t3 where a > 20)", null },
	     { "insert into t3 (a, b) (select a,b from t4 where a > 20)", null },
	     { "insert into t4 (a, b) (select a,b from t3 where a > 20)", null },
	     { "insert into t3 (a, b) (select a,b from t4 where a > 20)", null },
	     { "insert into t4 (a, b) (select a,b from t3 where a > 20)", null },
	     { "insert into t3 (a, b) (select a,b from t4 where a > 20)", null },
	     { "insert into t4 (a, b) (select a,b from t3 where a > 20)", null },
	     { "insert into t3 (a, b) (select a,b from t4 where a > 20)", null },
	     { "insert into t4 (a, b) (select a,b from t3 where a > 20)", null },
	     { "insert into t3 (a, b) (select a,b from t4 where a > 20)", null },
	     { "insert into t4 (a, b) (select a,b from t3 where a > 20)", null },
	     { "insert into t3 (a, b) (select a,b from t4 where a > 60)", null },
		{ "select count(*) from t3", new String[][] { {"54579"} } },
	     { "select count(*) from t4", new String[][] { {"48812"} } },
		{ "CREATE INDEX T3_IX1 ON T3 (A)", null },
	     { "CREATE INDEX T3_IX2 ON T3 (B)", null },
	     { "CREATE INDEX T4_IX1 ON T4 (A)", null },
	     { "CREATE INDEX T4_IX2 ON T4 (B)", null },
		{ "CREATE TABLE T5 (I INTEGER, J INTEGER) REPLICATE", null },
	     { "insert into t5 values (5, 10)", null },
	     { "CREATE TABLE T6 (P INTEGER, Q INTEGER) REPLICATE", null },
	     { "insert into t5 values (2, 4), (4, 8)", null },
	     { "CREATE TABLE XX1 (II INTEGER NOT NULL, JJ CHAR(10), MM INTEGER, OO DOUBLE, KK BIGINT) REPLICATE", null },
	     { "CREATE TABLE YY1 (II INTEGER NOT NULL, JJ CHAR(10), AA INTEGER, OO DOUBLE, KK BIGINT) REPLICATE", null },
	     { "ALTER TABLE YY1 ADD CONSTRAINT PK_YY1 PRIMARY KEY (II)", null },
	     { "ALTER TABLE XX1 ADD CONSTRAINT PK_XX1 PRIMARY KEY (II)", null },
	     { "create view xxunion as select all ii, jj, kk, mm from xx1 union all select ii, jj, kk, mm from xx1 union all select ii, jj, kk, mm from xx1 union all select ii, jj, kk, mm from xx1 union all select ii, jj, kk, mm from xx1 union all select ii, jj, kk, mm from xx1 union all select ii, jj, kk, mm from xx1 union all select ii, jj, kk, mm from xx1 union all select ii, jj, kk, mm from xx1 union all select ii, jj, kk, mm from xx1 union all select ii, jj, kk, mm from xx1 union all select ii, jj, kk, mm from xx1 union all select ii, jj, kk, mm from xx1 union all select ii, jj, kk, mm from xx1 union all select ii, jj, kk, mm from xx1 union all select ii, jj, kk, mm from xx1 union all select ii, jj, kk, mm from xx1 union all select ii, jj, kk, mm from xx1 union all select ii, jj, kk, mm from xx1 union all select ii, jj, kk, mm from xx1 union all select ii, jj, kk, mm from xx1 union all select ii, jj, kk, mm from xx1 union all select ii, jj, kk, mm from xx1 union all select ii, jj, kk, mm from xx1 union all select ii, jj, kk, mm from xx1", null },
	     { "create view yyunion as select all ii, jj, kk, aa from yy1 union all select ii, jj, kk, aa from yy1 union all select ii, jj, kk, aa from yy1 union all select ii, jj, kk, aa from yy1 union all select ii, jj, kk, aa from yy1 union all select ii, jj, kk, aa from yy1 union all select ii, jj, kk, aa from yy1 union all select ii, jj, kk, aa from yy1 union all select ii, jj, kk, aa from yy1 union all select ii, jj, kk, aa from yy1 union all select ii, jj, kk, aa from yy1 union all select ii, jj, kk, aa from yy1 union all select ii, jj, kk, aa from yy1 union all select ii, jj, kk, aa from yy1 union all select ii, jj, kk, aa from yy1 union all select ii, jj, kk, aa from yy1 union all select ii, jj, kk, aa from yy1 union all select ii, jj, kk, aa from yy1 union all select ii, jj, kk, aa from yy1 union all select ii, jj, kk, aa from yy1 union all select ii, jj, kk, aa from yy1 union all select ii, jj, kk, aa from yy1 union all select ii, jj, kk, aa from yy1 union all select ii, jj, kk, aa from yy1 union all select ii, jj, kk, aa from yy1", null },
		{ "select * from V1, V2 where V1.j = V2.b", new String[][] { {"1","2","2","2"},{"2","4","4","4"} } },
	     { "select * from V2, V1 where V1.j = V2.b", new String[][] { {"2","2","1","2"},{"4","4","2","4"} } },
		//FIXME
		//GemFireXD returns 202 instead of 404
		//{ "select count(*) from V1, V2 where V1.i in (2,4)", new String[][] { {"404"} } },     
	     { "select count(*) from V1, V2 where V1.j > 0", new String[][] { {"505"} } },
		{ "select * from V1, V2 where V1.j = V2.b and V1.i in (2,4)", new String[][] { {"2","4","4","4"} } },
		{ "select * from (select * from t1 union select * from t2) x1, (select * from t3 union select * from t4) x2 where x1.i = x2.a", new String[][] {
			{"1","2","1","1"},{"2","-4","2","2"},{"2","4","2","2"},{"3","6","3","3"},
			{"3","6","3","12"},{"4","-8","4","4"},{"4","-8","4","16"},{"4","8","4","4"},{"4","8","4","16"} } },
		{ "select * from (select * from t1 union select * from t2) x1, t3 where x1.i = t3.a", new String[][] {
			{"1","2","1","1"},{"2","-4","2","2"},{"2","4","2","2"},{"3","6","3","3"},{"4","-8","4","4"},{"4","8","4","4"} } },
		{ "select * from (select * from t1 union all select * from t2) x1, (select * from t3 union select * from t4) x2 where x1.i = x2.a", new String[][] {
			{"1","2","1","1"},{"2","4","2","2"},{"3","6","3","3"},{"3","6","3","12"},{"4","8","4","4"},
			{"4","8","4","16"},{"1","2","1","1"},{"2","-4","2","2"},{"3","6","3","3"},{"3","6","3","12"},
			{"4","-8","4","4"},{"4","-8","4","16"} } },
	     { "select * from (select * from t1 union all select * from t2) x1, (select * from t3 union all select * from t4) x2 where x1.i = x2.a", new String[][] {
			{"1","2","1","1"},{"2","4","2","2"},{"3","6","3","3"},{"3","6","3","12"},{"4","8","4","4"},
			{"4","8","4","16"},{"1","2","1","1"},{"2","-4","2","2"},{"3","6","3","3"},{"3","6","3","12"},
			{"4","-8","4","4"},{"4","-8","4","16"} } },
		{ "select * from v1, v2 where V1.i = V1.j", new String[0][0] },
		{"select * from (select * from t1 union select * from t2) x1 (c, d), (select * from t3 union select * from t4) x2 (e, f) where x1.c = x2.e", new String[][] {
			{"1","2","1","1"},{"2","-4","2","2"},{"2","4","2","2"},{"3","6","3","3"},{"3","6","3","12"},{"4","-8","4","4"},
			{"4","-8","4","16"},{"4","8","4","4"},{"4","8","4","16"} } },
	     { "select * from (select * from t1 union select * from t2) x1 (a, b), (select * from t3 union select * from t4) x2 (i, j) where x1.a = x2.i", new String[][] {
			{"1","2","1","1"},{"2","-4","2","2"},{"2","4","2","2"},{"3","6","3","3"},{"3","6","3","12"},{"4","-8","4","4"},
			{"4","-8","4","16"},{"4","8","4","4"},{"4","8","4","16"} } },
		{ "select count(*) from (select * from t1 union select * from t3) x1 (c, d), (select * from t2 union select * from t4) x2 (e, f) where x1.c = x2.e", new String[][] { {"103"} } },
		{ "select * from (select * from t1 union select * from t2 union select * from t1 union select * from t2) x1, (select * from t3 union select * from t4 union select * from t4) x2 where x1.i = x2.a", new String[][] {
			{"1","2","1","1"},{"2","-4","2","2"},{"2","4","2","2"},{"3","6","3","3"},{"3","6","3","12"},{"4","-8","4","4"},
			{"4","-8","4","16"},{"4","8","4","4"},{"4","8","4","16"} } },
		{ "select * from (select * from t1 union select * from t2 union select * from t1 union select * from t2) x1 where x1.i > 0", new String[][] {
			{"1","2"},{"2","-4"},{"2","4"},{"3","6"},{"4","-8"},{"4","8"},{"5","10"} } },
		{ "select count(*) from (select * from t1 union select * from t2 union select * from t3 union select * from t4) x1 (i, b) where x1.i > 0", new String[][] { {"108"} } },
		{ "select * from (select * from t1 union select * from t2) x1 inner join (select * from t3 union select * from t4) x2 on x1.j = x2.b", new String[][] { {"1","2","2","2"},{"2","4","4","4"} } },
		{ "select * from t2,(select * from t1 union values (3,3), (4,4), (5,5), (6,6)) X1 (a,b) where X1.a = t2.i", new String[][] {
			{"1","2","1","2"},{"2","-4","2","4"},{"3","6","3","3"},
			{"3", "6","3","6"},{"4","-8","4","4"},{"4","-8","4","8"},
			{"5","10","5","5"},{"5","10","5","10"} } },
		{ "select * from (select i,j from t2 union values (1,1),(2,2),(3,3),(4,4) union select i,j from t1) x0 (i,j), (select a, b from t3 union values (4, 5), (5, 6), (6, 7) union select a, b from t4) x1 (a,b) where x0.i = x1.a", new String[][] {
			{"1","1","1","1"},{"1","2","1","1"},{"2","-4","2","2"},{"2","2","2","2"},
			{"2","4","2","2"},{"3","3","3","3"},{"3","3","3","12"},{"3","6","3","3"},
			{"3","6","3","12"},{"4","-8","4","4"},{"4","-8","4","5"},{"4","-8","4","16"},
			{"4","4","4","4"},{"4","4","4","5"},{"4","4","4","16"},{"4","8","4","4"},
			{"4","8","4","5"},{"4","8","4","16"},{"5","10","5","6"} } },
		//FIXME
		//Throws ASSERT FAILED visibleSize() should match
		//{ "select * from t5,(values (2,2), (4,4) union values (1,1),(2,2),(3,3),(4,4) union select i,j from t1) x0 (i,j) where x0.i = t5.i", new String[][] {
		//	{"2","4","2","2"},{"2","4","2","4"},{"4","8","4","4"},{"4","8","4","8"},{"5","10","5","10"} } },
		{" select distinct xx0.kk, xx0.ii, xx0.jj from xxunion xx0, yyunion yy0 where xx0.mm = yy0.ii", new String[0][0] },
		//FIXME 
		// GemFireXD returns 396 here instead of 909
		//{ "select count(*) from (select * from t1 union select * from t2) x1,(select * from t3 union select * from t4) x2,(select * from t4 union select * from t3) x3 where x1.i = x3.a", new String[][] { {"909"} } },
		//FIXME
		//GemFireXD returns 6 here instead of 9
		//{ "select count(*) from (select * from t1 union select * from t2) x1,(select * from t3 union select * from t4) x2,(select * from t4 union select * from t3) x3 where x1.i = x3.a and x3.b = x2.b", new String[][] { {"9"} } },
		{ "select * from (select i, b j from t1, t4 where i = j union select * from t2) x1, t3 where x1.j = t3.a", new String[][] {
			{"1","2","2","2"},{"3","6","6","24"},{"5","10","10","40"} } },
		{ "select * from (select i, b j from t1, t4 where i = j union select * from t2) x1, v2 where x1.j = v2.a", new String[][] {
			{"1","2","2","2"},{"3","6","6","24"},{"5","10","10","40"} } },
		{ "select * from (select i, b j from t1, t4 where i = j union select * from t2) x1, (select i, b j from t2, t3 where i = j union select * from t1) x2 where x1.j = x2.i", new String[][] { {"1","2","2","4"} } },
		{ "select count(*) from (select i,a,j,b from V1,V2 where V1.j = V2.b) X3", new String[][] { {"2"} } },
		{ "select t2.i,p from (select distinct i,p from (select distinct i,a from t1, t3 where t1.j = t3.b) X1, t6 where X1.a = t6.p) X2, t2 where t2.i = X2.i", new String[0][0] },
		{ "select x1.j, x2.b from (select distinct i,j from t1) x1, (select distinct a,b from t3) x2 where x1.i = x2.a order by x1.j, x2.b", new String[][] {
			{"2","1"},{"4","2"},{"6","3"},{"8","4"} } },
	     { "select x1.j, x2.b from (select distinct i,j from t1) x1, (select distinct a,b from t3) x2, (select distinct i,j from t2) x3, (select distinct a,b from t4) x4 where x1.i = x2.a and x3.i = x4.a order by x1.j, x2.b", new String[][] {
			{"2","1"},{"2","1"},{"4","2"},{"4","2"},{"6","3"},{"6","3"},{"8","4"},{"8","4"} } },
		// FIXME
		// Nested set operators are not supported in multi-node setup
		//{ "select X0.a, X2.i from (select a,b from t4 union select a,b from t3) X0, (select i,j from (select i,j from t1 union select i,j from t2) X1,     T6 where T6.p = X1.i) X2 where X0.b = X2.j", new String[0][0] },
		//{ "select X0.a, X2.i from (select a,b from t4 union select a,b from t3) X0, (select i,j from (select i,j from t1 union select i,j from t2) X1,      T6) X2 where X0.b = X2.j", new String[0][0] },
		//{ "select X0.a, X2.i from (select a,b from t4 union select a,b from t3) X0, (select i,j from (select i,j from t1 union select i,j from t2) X1,      T6  where T6.p = X1.i) X2", new String[0][0] },
	     { "select * from (select * from t1 union select * from t2) x1, (values (2, 4), (3, 6), (4, 8)) x2 (a, b) where x1.i = x2.a", new String[][] {
			{"2","-4","2","4"},{"2","4","2","4"},{"3","6","3","6"},{"4","-8","4","8"},{"4","8","4","8"} } },
	     { "select * from (select * from t1 union (values (1, -1), (2, -2), (5, -5))) x1 (i, j), (values (2, 4), (3, 6), (4, 8)) x2 (a, b) where x1.i = x2.a", new String[][] {
			{"2","-2","2","4"},{"2","4","2","4"},{"3","6","3","6"},{"4","8","4","8"} } },
	     { "select * from (select * from t1 union all (values (1, -1), (2, -2), (5, -5))) x1 (i, j), (values (2, 4), (3, 6), (4, 8)) x2 (a, b) where x1.i = x2.a", new String[][] {
			{"2","4","2","4"},{"3","6","3","6"},{"4","8","4","8"},{"2","-2","2","4"} } },
	     { "select * from (select * from t1 union (values (1, -1), (2, -2), (5, -5))) x1 (i, j), (values (2, 4), (3, 6), (4, 8)) x2 (a, b) where x1.i = x2.a and x2.b = x1.j", new String[][] {
			{"2","4","2","4"},{"3","6","3","6"},{"4","8","4","8"} } },
	     { "select * from (values (2, -4), (3, -6), (4, -8) union values (1, -1), (2, -2), (5, -5)) x1 (i, j), (values (2, 4), (3, 6), (4, 8)) x2 (a, b)where x1.i = x2.a", new String[][] {
			{"2","-4","2","4"},{"2","-2","2","4"},{"3","-6","3","6"},{"4","-8","4","8"} } },
	     { "select * from (values (2, -4), (3, -6), (4, -8) union values (1, -1), (2, -2), (5, -5)) x1 (i, j), (values (2, 4), (3, 6), (4, 8)) x2 (a, b) where x1.i = x2.a and x2.b = x1.j", new String[0][0] },
		{ "drop view v1", null },
	     { "drop view v2", null },
	     { "drop table t1", null },
	     { "drop table t2", null },
	     { "drop table t3", null },
	     { "drop table t4", null },
	     { "drop table t5", null },
	     { "drop table t6", null },
	     { "drop view xxunion", null },
	     { "drop view yyunion", null },
	     { "drop table xx1", null },
	     { "drop table yy1", null },
		{ "CREATE TABLE T1 (I INTEGER, D DOUBLE, C CHAR(10)) REPLICATE", null },
	     { "CREATE TABLE T2 (I2 INTEGER, D2 DOUBLE, C2 CHAR(10)) REPLICATE", null },
	     { "CREATE TABLE T3 (I3 INTEGER, D3 DOUBLE, C3 CHAR(10)) REPLICATE", null },
	     { "insert into t1 values (1, -1, '1'), (2, -2, '2')", null },
	     { "insert into t2 values (2, -2, '2'), (4, -4, '4'), (8, -8, '8')", null },
	     { "insert into t3 values (3, -3, '3'), (6, -6, '6'), (9, -9, '9')", null },
	     { "CREATE TABLE T4 (C4 CHAR(10)) REPLICATE", null },
	     { "insert into t4 values '1', '2', '3', '4', '5', '6', '7', '8', '9'", null },
	     { "insert into t4 select rtrim(c4) || rtrim(c4) from t4", null },
	     { "CREATE TABLE T5 (I5 INTEGER, D5 DOUBLE, C5 CHAR(10)) REPLICATE", null },
	     { "CREATE TABLE T6 (I6 INTEGER, D6 DOUBLE, C6 CHAR(10)) REPLICATE", null },
	     { "insert into t5  values (100, 100.0, '100'), (200, 200.0, '200'), (300, 300.0, '300')", null },
	     { "insert into t6  values (400, 400.0, '400'), (200, 200.0, '200'), (300, 300.0, '300')", null },
	     { "create view v_keycol_at_pos_3 as select distinct i col1, d col2, c col3 from t1", null },
	     { "create view v1_keycol_at_pos_2 as select distinct i2 col1, c2 col3, d2 col2 from t2", null },
	     { "create view v2_keycol_at_pos_2 as select distinct i3 col1, c3 col3, d3 col2 from t3", null },
	     { "create view v1_intersect as select distinct i5 col1, c5 col3, d5 col2 from t5", null },
	     { "create view v2_intersect as select distinct i6 col1, c6 col3, d6 col2 from t6", null },
	     { "create view v1_values as select distinct vals1 col1, vals2 col2, vals3 col3 from (values (321, 321.0, '321'), (432, 432.0, '432'), (654, 654.0, '654')) VT(vals1, vals2, vals3)", null },
	     { "create view v_union as select distinct i col1, d col2, c col3 from t1 union select distinct i3 col1, d3 col2, c3 col3 from t3", null },
		{ "create view topview as (select distinct 'other:' col0, vpos3.col3, vpos3.col1 from v_keycol_at_pos_3 vpos3 union select distinct 't2stuff:' col0, vpos2_1.col3, vpos2_1.col1 from v1_keycol_at_pos_2 vpos2_1 union select distinct 't3stuff:' col0, vpos2_2.col3, vpos2_2.col1 from v2_keycol_at_pos_2 vpos2_2)", null },
		{ "create view topview2 as (select distinct 'other:' col0, vpos3.col3, vpos3.col1 from v_keycol_at_pos_3 vpos3 union select distinct 't2stuff:' col0, vpos2_1.col3, vpos2_1.col1 from v1_keycol_at_pos_2 vpos2_1 union select distinct 't3stuff:' col0, vpos2_2.col3, vpos2_2.col1 from v2_keycol_at_pos_2 vpos2_2 union select distinct 'morestuff:' col0, vu.col3, vu.col1 from v_union vu)", null },
		{ "create view topview3 (col0, col3, col1) as (select distinct 'other:' col0, vpos3.col3, vpos3.col1 from v_keycol_at_pos_3 vpos3 intersect    select distinct 't2stuff:' col0, vpos2_1.col3, vpos2_1.col1 from v1_keycol_at_pos_2 vpos2_1 union select distinct 't3stuff:' col0, vpos2_2.col3, vpos2_2.col1 from v2_keycol_at_pos_2 vpos2_2 union select distinct 'morestuff:' col0, vu.col3, vu.col1 from v_union vu)", null },
		{ "create view topview4 (col0, col3, col1) as (select distinct 'intersect:' col0, vi1.col3, vi1.col1 from v1_intersect vi1 intersect select distinct 'intersect:' col0, vi2.col3, vi2.col1  from v2_intersect vi2 union select distinct 't3stuff:' col0, vpos2_2.col3, vpos2_2.col1 from v2_keycol_at_pos_2 vpos2_2 union select distinct 'morestuff:' col0, vu.col3, vu.col1 from v_union vu)", null },
		{ "create view topview5 (col0, col3, col1) as (select distinct 'values:' col0, vv1.col3, vv1.col1 from v1_values vv1 union select distinct 'intersect:' col0, vi2.col3, vi2.col1 from v2_intersect vi2 union select distinct 't3stuff:' col0, vpos2_2.col3, vpos2_2.col1 from v2_keycol_at_pos_2 vpos2_2 union select distinct 'morestuff:' col0, vu.col3, vu.col1 from v_union vu)", null },
		{ "select * from t4, topview where t4.c4 = topview.col3", new String[][] {
			{"1","other:","1","1"}, {"2","other:","2","2"},
			{"2","t2stuff:","2","2"},{"4","t2stuff:","4","4"},
			{"8","t2stuff:","8","8"},{"3","t3stuff:","3","3"},
			{"6","t3stuff:","6","6"},{"9","t3stuff:","9","9"} } },
	     { "select * from t4, topview where topview.col3 = t4.c4", new String[][] {
			{"1","other:","1","1"}, {"2","other:","2","2"},
			{"2","t2stuff:","2","2"},{"4","t2stuff:","4","4"},
			{"8","t2stuff:","8","8"},{"3","t3stuff:","3","3"},
			{"6","t3stuff:","6","6"},{"9","t3stuff:","9","9"} } },
	     { "select * from topview x1 left join topview3 on topview3.col3 = x1.col3", new String[][] {
			{"other:","1","1","morestuff:","1","1"},
			{"other:","2","2","morestuff:","2","2"},
			{"t2stuff:","2","2","morestuff:","2","2"},
			{"t2stuff:","4","4",null,null,null},
			{"t2stuff:","8","8",null,null,null},
			{"t3stuff:","3","3","morestuff:","3","3"},
			{"t3stuff:","3","3","t3stuff:","3","3"},
			{"t3stuff:","6","6","morestuff:","6","6"},
			{"t3stuff:","6","6","t3stuff:","6","6"},
			{"t3stuff:","9","9","morestuff:","9","9"},
			{"t3stuff:","9","9","t3stuff:","9","9"} } },
		 // FIXME
		 // This throws ArrayIndexOutOfBoundsException on a peer-client connection!
	     //{ "select * from topview x1 left join topview4 on topview4.col3 = x1.col3", new String[][] {
		 //	{"other:","1","1","morestuff:","1","1"},
		 //	{"other:","2","2","morestuff:","2","2"},
		 //	{"t2stuff:","2","2","morestuff:","2","2"},
		 //	{"t2stuff:","4","4",null,null,null},
		 //	{"t2stuff:","8","8",null,null,null},
		 //	{"t3stuff:","3","3","morestuff:","3","3"},
		 //	{"t3stuff:","3","3","t3stuff:","3","3"},
		 //	{"t3stuff:","6","6","morestuff:","6","6"},
		 //	{"t3stuff:","6","6","t3stuff:","6","6"},
		 //	{"t3stuff:","9","9","morestuff:","9","9"},
		 //	{"t3stuff:","9","9","t3stuff:","9","9"} } },
	     { "select * from topview x1 left join topview5 on topview5.col3 = x1.col3", new String[][] {
			{"other:","1","1","morestuff:","1","1"},
			{"other:","2","2","morestuff:","2","2"},
			{"t2stuff:","2","2","morestuff:","2","2"},
			{"t2stuff:","4","4",null,null,null},
			{"t2stuff:","8","8",null,null,null},
			{"t3stuff:","3","3","morestuff:","3","3"},
			{"t3stuff:","3","3","t3stuff:","3","3"},
			{"t3stuff:","6","6","morestuff:","6","6"},
			{"t3stuff:","6","6","t3stuff:","6","6"},
			{"t3stuff:","9","9","morestuff:","9","9"},
			{"t3stuff:","9","9","t3stuff:","9","9"} } },
		{ "insert into t1 select * from t2", null },
	     { "insert into t2 select * from t3", null },
	     { "insert into t3 select * from t1", null },
	     { "insert into t1 select * from t2", null },
	     { "insert into t2 select * from t3", null },
	     { "insert into t3 select * from t1", null },
	     { "insert into t1 select * from t2", null },
	     { "insert into t2 select * from t3", null },
	     { "insert into t3 select * from t1", null },
	     { "insert into t1 select * from t2", null },
	     { "insert into t2 select * from t3", null },
	     { "insert into t3 select * from t1", null },
		//FIXME
		//GemFireXD returns 7 here instead of 42
		//{ "select count(*) from topview x1 left join topview4 on topview4.col3 = x1.col3", new String[][] { {"42"} } },
		{ "drop view topview", null },
	     { "drop view topview2", null },
	     { "drop view topview3", null },
	     { "drop view topview4", null },
	     { "drop view topview5", null },
	     { "drop view v_keycol_at_pos_3", null },
	     { "drop view v1_keycol_at_pos_2", null },
	     { "drop view v2_keycol_at_pos_2", null },
	     { "drop view v1_intersect", null },
	     { "drop view v2_intersect", null },
	     { "drop view v1_values", null },
	     { "drop view v_union", null },
	     { "drop table t1", null },
		{ "drop table t2", null },
		{ "drop table t3", null },
		{ "drop table t4", null },
		{ "drop table t5", null },
		{ "drop table t6", null }
	   };

	    // Start 1 client and 3 servers, use default partitioning
	    startVMs(1, 3);


	    Connection conn = TestUtil.getConnection();
	    Statement stmt = conn.createStatement();
	    // Go through the array, execute each string[0], check sqlstate [1]
	    // This will fail on the first one that succeeds where it shouldn't
	    // or throws unknown exception
	    JDBC.SQLUnitTestHelper(stmt,Script_PredicatePushdownUT);
	  }

}
