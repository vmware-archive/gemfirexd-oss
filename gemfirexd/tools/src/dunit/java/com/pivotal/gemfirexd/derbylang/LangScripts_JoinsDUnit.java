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

public class LangScripts_JoinsDUnit extends DistributedSQLTestBase {

	public LangScripts_JoinsDUnit(String name) {
		super(name);
		// TODO Auto-generated constructor stub
	}
	  // This test is the as-is LangScript conversion, without any partitioning clauses
	  public void testLangScript_JoinsTestNoPartitioning() throws Exception
	  {
	    // This is a JUnit conversion of the Derby Lang Joins.sql script
	    // without any GemFireXD extensions
		  
	    // Catch exceptions from illegal syntax
	    // Tests still not fixed marked FIXME
		  
	    // Array of SQL text to execute and sqlstates to expect
	    // The first object is a String, the second is either 
	    // 1) null - this means query returns no rows and throws no exceptions
	    // 2) a string - this means query returns no rows and throws expected SQLSTATE
	    // 3) a String array - this means query returns rows which must match (unordered) given resultset
	    //       - for an empty result set, an uninitialized size [0][0] array is used
	    Object[][] Script_JoinsUT = {
	    // FIXME 
	    // This test really should be run with all tables partitioned and colocated - but joins are not always on expected columns
	    // Try all tables but one as REPLICATEd, there are too many 0A000 colocation errors below to make the test workable with partitioned tables
		{ "create table t1 (t1_c1 int, t1_c2 char(10)) replicate", null },
		{ "create table t2 (t2_c1 int, t2_c2 char(10)) replicate", null },
		{ "create table t3 (t3_c1 int, t3_c2 char(10)) replicate", null },
		{ "create table t4 (t4_c1 int, t4_c2 char(10))", null },
		{ "insert into t1 values (1, 't1-row1')", null },
		{ "insert into t1 values (2, 't1-row2')", null },
		{ "insert into t2 values (1, 't2-row1')", null },
		{ "insert into t2 values (2, 't2-row2')", null },
		{ "insert into t3 values (1, 't3-row1')", null },
		{ "insert into t3 values (2, 't3-row2')", null },
		{ "insert into t4 values (1, 't4-row1')", null },
		{ "insert into t4 values (2, 't4-row2')", null },
		{ "select * from t1, t1", "42X09" },
		// cartesian products
		{ "select * from t1, t2", new String[][] {
			{"1","t1-row1","1","t2-row1"},
			{"1","t1-row1","2","t2-row2"},
			{"2","t1-row2","1","t2-row1"},
			{"2","t1-row2","2","t2-row2"} } },
		{ "select t2.*, t1.* from t1, t2", new String[][] {
			{"1","t2-row1","1","t1-row1"},
			{"2","t2-row2","1","t1-row1"},
			{"1","t2-row1","2","t1-row2"},
			{"2","t2-row2","2","t1-row2"} } },
		{ "select t2_c2, t1_c2, t1_c1, t2_c1 from t1, t2", new String[][] {
			{"t2-row1","t1-row1","1","1"},
			{"t2-row2","t1-row1","1","2"},
			{"t2-row1","t1-row2","2","1"},
			{"t2-row2","t1-row2","2","2"} } },
		{ "select t2_c2, t1_c1 from t1, t2", new String[][] {
			{"t2-row1","1"},{"t2-row2","1"},{"t2-row1","2"},{"t2-row2","2"} } },
		// project/restricts
		{ "select a.t1_c1, b.t1_c1, cc.t1_c1, d.t1_c1, e.t1_c1, f.t1_c1, g.t1_c1, h.t1_c1, i.t1_c1, j.t1_c1 from t1 a, t1 b, t1 cc, t1 d, t1 e, t1 f, t1 g, t1 h, t1 i, t1 j where a.t1_c2 = b.t1_c2 and b.t1_c2 = cc.t1_c2 and cc.t1_c2 = d.t1_c2 and d.t1_c2 = e.t1_c2 and e.t1_c2 = f.t1_c2 and f.t1_c2 = g.t1_c2 and g.t1_c2 = h.t1_c2 and h.t1_c2 = i.t1_c2 and i.t1_c2 = j.t1_c2", new String[][] {
			{"1","1","1","1","1","1","1","1","1","1"},
			{"2","2","2","2","2","2","2","2","2","2"} } },
		{ "select a.t1_c1, b.t1_c1, cc.t1_c1, d.t1_c1, e.t1_c1, f.t1_c1, g.t1_c1, h.t1_c1, i.t1_c1, j.t1_c1 from t1 a, t1 b, t1 cc, t1 d, t1 e, t1 f, t1 g, t1 h, t1 i, t1 j where a.t1_c1 = 1 and b.t1_c1 = 1 and cc.t1_c1 = 1 and d.t1_c1 = 1 and e.t1_c1 = 1 and f.t1_c1 = 1 and g.t1_c1 = 1 and h.t1_c1 = 1 and i.t1_c1 = 1 and a.t1_c2 = b.t1_c2 and b.t1_c2 = cc.t1_c2 and cc.t1_c2 = d.t1_c2 and d.t1_c2 = e.t1_c2 and e.t1_c2 = f.t1_c2 and f.t1_c2 = g.t1_c2 and g.t1_c2 = h.t1_c2 and h.t1_c2 = i.t1_c2 and i.t1_c2 = j.t1_c2", new String[][] {
			{"1","1","1","1","1","1","1","1","1","1"} } },
		// project out entire tables
		{ "select 1, 2 from t1, t2", new String[][] { {"1","2"},{"1","2"},{"1","2"},{"1","2"} } },
		{ "select 1, t1.t1_c1 from t1, t2", new String[][]  { {"1","1"},{"1","1"},{"1","2"},{"1","2"} } },
		{ "select t2.t2_c2,1 from t1, t2", new String[][] { {"t2-row1","1"},{"t2-row2","1"},{"t2-row1","1"},{"t2-row2","1"} } },
		{ "select c.t1_c1 from (select a.t1_c1 from t1 a, t1 b) c, t1 d where c.t1_c1 = d.t1_c1", new String[][] { {"1"},{"1"},{"2"},{"2"} } },
		// create a table for testing inserts
		{ "create table instab (instab_c1 int, instab_c2 char(10), instab_c3 int, instab_c4 char(10))", null },
		// insert select with joins
		{ "insert into instab select * from t1, t2", null },
		{ "select * from instab", new String[][] { 
			{"1","t1-row1","1","t2-row1"}, {"1","t1-row1","2","t2-row2"}, {"2","t1-row2","1","t2-row1"}, {"2","t1-row2","2","t2-row2"} } },
		{ "delete from instab", null },
		{ "insert into instab (instab_c1, instab_c2, instab_c3, instab_c4) select * from t1, t2", null },
		{ "select * from instab", new String[][] { 
			{"1","t1-row1","1","t2-row1"}, {"1","t1-row1","2","t2-row2"}, {"2","t1-row2","1","t2-row1"}, {"2","t1-row2","2","t2-row2"} } },
		{ "delete from instab", null },
		{ "insert into instab (instab_c1, instab_c2, instab_c3, instab_c4) select t2_c1, t2_c2, t1_c1, t1_c2 from t1, t2", null },
		{ "select * from instab", new String[][] { 
			{"1","t2-row1","2","t1-row2"}, {"2","t2-row2","1","t1-row1"}, {"1","t2-row1","1","t1-row1"}, {"2","t2-row2","2","t1-row2"} } },
		{ "delete from instab", null },
		{ "insert into instab (instab_c3, instab_c1, instab_c2, instab_c4) select t2_c1, t1_c1, t1_c2, t2_c2 from t1, t2", null },
		{ "select * from instab", new String[][] { 
			{"1","t1-row1","1","t2-row1"}, {"1","t1-row1","2","t2-row2"}, {"2","t1-row2","1","t2-row1"}, {"2","t1-row2","2","t2-row2"} } },
		{ "delete from instab", null },
		// projection
		{ "insert into instab (instab_c1, instab_c3) select t1_c1, t2_c1 from t1, t2", null },
		{ "select * from instab", new String[][] {
			{"1",null, "1",null},
			{"1",null, "2",null},
			{"2",null, "1",null},
			{"2",null, "2",null} } },
		{ "delete from instab", null },
		{ "insert into instab select 1, '2', 3, '4' from t1, t2", null },
		{ "select * from instab", new String[][] {
			{"1","2","3","4"},
			{"1","2","3","4"},
			{"1","2","3","4"},
			{"1","2","3","4"} } },
		{ "delete from instab", null },
		{ "insert into instab select 1, t1.t1_c2, 3, t1.t1_c2 from t1, t2", null },
		{ "select * from instab", new String[][] {
			{"1","t1-row1","3","t1-row1"},
			{"1","t1-row1","3","t1-row1"},
			{"1","t1-row2","3","t1-row2"},
			{"1","t1-row2","3","t1-row2"} } },
		{ "delete from instab", null },
		{ "insert into instab select t2.t2_c1, '2', t2.t2_c1, '4' from t1, t2", null },
		{ "select * from instab", new String[][] {
			{"1","2","1","4"},{"2","2","2","4"},{"1","2","1","4"},{"2","2","2","4"} } },
		{ "delete from instab", null },
		// test optimizations for join
		{ "select t1_c1 from t1, t2 where (case when t1_c1 = 1 then t2_c2 end) = t2_c2", new String[][] { {"1"},{"1"} } },
		{ "select t1_c1 from t1, t2 where CHAR(t1_c1) = t2_c2", new String[0][0] },
		{ "select t1_c1 from t1, t2 where t1_c1 = 1 or t2_c1 = 2", new String[][] { {"1"},{"1"},{"2"} } },
		{ "select t1_c1 from t1, t2 where t1_c1 = 2147483647 and 2147483647 = t2_c1", new String[0][0] },
		{ "select t1_c1 from t1, t2 where INT(t1_c1) = t2_c1", new String[][] { {"1"},{"2"} } },
		{ "select t1_c1 from t1, t2 where t1_c1 = INT(2147483647) and INT(2147483647) = t2_c1", new String[0][0] },
		// transitive closure - verify join condition doesn't get dropped
		{ "create table x(c1 int) partition by column(c1)", null },
		{ "create table y(c1 int) partition by column(c1) colocate with (x)", null },
		{ "insert into x values 1, 2, null", null },
		{ "insert into y values 1, 2, null", null },
		{ "select * from x,y where x.c1 = y.c1 and x.c1 = 1 and y.c1 = 2", new String[0][0] },
		{ "select * from x,y where x.c1 = y.c1 and x.c1 is null", new String[0][0] },
		{ "select * from x,y where x.c1 = y.c1 and x.c1 is null and y.c1 = 2", new String[0][0] },
		{ "select * from x,y where x.c1 = y.c1 and x.c1 is null and y.c1 is null", new String[0][0] },
		// join node flattening leads to incorrect transitive closure, which in turn results in incorrect results. Test this.
		{ "create table b2 (c1 int, c2 int, c3 char(1), c4 int, c5 int, c6 int) replicate", null },
		{ "create table b4 (c7 int, c4 int, c6 int) replicate", null },
		{ "create table b3 (c8 int, c9 int, c5 int, c6 int) replicate ", null },
		{ "create table b (c1 int, c2 int, c3 char(1), c4 int, c5 int, c6 int) replicate", null },
		{ "create view bvw (c5, c1 ,c2 ,c3 ,c4) as select c5, c1 ,c2 ,c3 ,c4 from b2 union select c5, c1 ,c2 ,c3 ,c4 from b", null },
		{ "create view bvw2 (c1 ,c2 ,c3 ,c4 ,c5) as select c1 ,c2 ,c3 ,c4 ,c5 from b2 union select c1 ,c2 ,c3 ,c4 ,c5 from b", null },
		{ "insert into b4 (c7,c4,c6) values (4, 42, 31)", null },
		{ "insert into b2 (c5,c1,c3,c4,c6) values (3,4, 'F',43,23)", null },
		{ "insert into b3 (c5,c8,c9,c6) values (2,3,19,28)", null },
		// Should see 1 row for *both* of these queries.
		{ "select b3.* from b3 join bvw on (b3.c8 = bvw.c5) join b4 on (bvw.c1 = b4.c7) where b4.c4 = 42", new String[][] { {"3","19","2","28"} } },
		{ "select b3.* from b3 join bvw2 on (b3.c8 = bvw2.c5) join b4 on (bvw2.c1 = b4.c7) where b4.c4 = 42", new String[][] { {"3","19","2","28"} } },
		{ "drop view bvw", null },
		{ "drop view bvw2", null },
		{ "drop table b", null },
		{ "drop table b2", null },
		{ "drop table b3", null },
		{ "drop table b4", null },
		// Make B1 REPLICATEd here to aid in joins below
		{ "create table b1 (c0 int) REPLICATE", null },
		{ "create table xx (c1 int, c2 int)", null },
		{ "create table b2 (c3 int, c4 int) REPLICATE", null },
		{ "insert into b1 values 1", null },
		{ "insert into xx values (0, 1)", null },
		{ "insert into b2 values (0, 2)", null },
		// Following should return 1 row.
		{ "select b1.* from b1 JOIN (select * from xx) VW(c1,c2) on (b1.c0 = vw.c2) JOIN b2 on (vw.c1 = b2.c3)", new String[][] { {"1"} } },
		{ "select b1.* from b1 JOIN (select * from xx) VW(ccx1,ccx2) on (b1.c0 = vw.ccx2) JOIN b2 on (vw.ccx1 = b2.c3)", new String[][] { {"1"} } },
		{ "select b1.* from b1 JOIN (select c1 as ccx1, c2 as ccx2 from xx) VW(ccx1,ccx2) on (b1.c0 = vw.ccx2) JOIN b2 on (vw.ccx1 = b2.c3)", new String[][] { {"1"} } },
		{ "select b1.* from b1 JOIN (select c1 as ccx1, c2 as ccx2 from xx) VW(x1,x2) on (b1.c0 = vw.x2) JOIN b2 on (vw.x1 = b2.c3)", new String[][] { {"1"} } },
		{ "select b1.* from    b1 JOIN (select c1 as ccx1, c2 as ccx2 from xx) VW(c1,c2) on (b1.c0 = vw.c2) JOIN b2 on (vw.c1 = b2.c3)", new String[][] { {"1"} } },
		{ "drop table b1", null },
		{ "drop table b2", null },
		{ "drop table xx", null },
		{ "CREATE TABLE d3023_t1 (A INTEGER, B INTEGER)", null },
		{ "insert into d3023_t1 values (1, 1), (-2, 2), (3, 3)", null },
		{ "CREATE TABLE d3023_t2 (C INTEGER, D INTEGER) REPLICATE", null },
		{ "insert into d3023_t2 values (1, -1), (2, -2), (3, -3)", null },
		{ "CREATE TABLE d3023_t3 (I INTEGER, J INTEGER) REPLICATE", null },
		{ "insert into d3023_t3 values (-2, 1), (-3, -2)", null },
		{ "CREATE TABLE d3023_t4 (X INTEGER, Y INTEGER) REPLICATE", null },
		{ "insert into d3023_t4 values (1, 1), (2, 2), (3, 3)", null },
		{ "select distinct * from d3023_t1 left outer join d3023_t2 on d3023_t1.a = d3023_t2.d", new String[][] {
			{"-2","2","2","-2"},{"1","1",null,null},{"3","3",null,null} } },
		{ "select distinct * from d3023_t1 left outer join d3023_t2 on d3023_t1.a = d3023_t2.d where d3023_t1.a = -2", new String[][] {
			{"-2","2","2","-2"} } },
		{ "select distinct * from d3023_t1 left outer join d3023_t2 on d3023_t1.a = d3023_t2.d inner join d3023_t3 on d3023_t1.a = d3023_t3.j", new String[][] {
			{"-2","2","2","-2","-3","-2"},
			{"1","1",null,null,"-2","1"} } },
		{ "select distinct * from d3023_t1 left outer join d3023_t2 on d3023_t1.a = d3023_t2.d inner join d3023_t3 on d3023_t1.a = d3023_t3.j where d3023_t1.a = -2", new String[][] {
			{"-2","2","2","-2","-3","-2"} } },
		//  This query only returns a single row, even without the explicit search predicate.
		{ "select distinct * from d3023_t1 left outer join d3023_t2 on d3023_t1.a = d3023_t2.d inner join d3023_t3 on d3023_t1.a = d3023_t3.j inner join d3023_t4 on d3023_t2.c = d3023_t4.x", new String[][] {
			{"-2","2","2","-2","-3","-2","2","2"} } },
		// Slight variation of the same query.  Add a search predicate enforcing "d3023_t1.a = -2" to the join condition.  Should see same row again.
		{ "select distinct * from d3023_t1 left outer join d3023_t2 on d3023_t1.a = d3023_t2.d AND d3023_t1.a = -2 inner join d3023_t3 on d3023_t1.a = d3023_t3.j inner join d3023_t4 on d3023_t2.c = d3023_t4.x", new String[][] {
			{"-2","2","2","-2","-3","-2","2","2"} } },
		// Full transitive closure query. Should return same row (Derby-3023 fixed query returning no rows)
		{ "select distinct * from d3023_t1 left outer join d3023_t2 on d3023_t1.a = d3023_t2.d inner join d3023_t3 on d3023_t1.a = d3023_t3.j inner join d3023_t4 on d3023_t2.c = d3023_t4.x where d3023_t1.a = -2", new String[][] {
			{"-2","2","2","-2","-3","-2","2","2"} } },
		{ "drop table d3023_t1", null },
		{ "drop table d3023_t2", null },
		{ "drop table d3023_t3", null },
		{ "drop table d3023_t4", null },
		{ "select t1_c1, t1_c2, t2_c1, t2_c2 from t1, t2 where t1_c1 = t2_c1 and t1_c1 = 1 and t2_c1 <> 1", new String[0][0] },
		{ "create table a (a1 int not null primary key, a2 int, a3 int, a4 int, a5 int, a6 int)", null },
		{ "create table b (b1 int not null primary key, b2 int, b3 int, b4 int, b5 int, b6 int) REPLICATE", null },
		{ "create table c (c1 int not null, c2 int, c3 int not null, c4 int, c5 int, c6 int) REPLICATE", null },
		{ "create table d (d1 int not null, d2 int, d3 int not null, d4 int, d5 int, d6 int) REPLICATE", null },
		{ "alter table c add primary key (c1,c3)", null },
		{ "alter table d add primary key (d1,d3)", null },
		{ "insert into a values (1,1,3,6,NULL,2),(2,3,2,4,2,2),(3,4,2,NULL,NULL,NULL),(4,NULL,4,2,5,2),(5,2,3,5,7,4),(7,1,4,2,3,4),(8,8,8,8,8,8),(6,7,3,2,3,4)", null },
		{ "insert into b values (6,7,2,3,NULL,1),(4,5,9,6,3,2),(1,4,2,NULL,NULL,NULL),(5,NULL,2,2,5,2),(3,2,3,3,1,4),(7,3,3,3,3,3),(9,3,3,3,3,3)", null },
		{ "insert into c values (3,7,7,3,NULL,1),(8,3,9,1,3,2),(1,4,1,NULL,NULL,NULL),(3,NULL,1,2,4,2),(2,2,5,3,2,4),(1,7,2,3,1,1),(3,8,4,2,4,6)", null },
		{ "insert into d values (1,7,2,3,NULL,3),(2,3,9,1,1,2),(2,2,2,NULL,3,2),(1,NULL,3,2,2,1),(2,2,5,3,2,3),(2,5,6,3,7,2)", null },
		{ "select a1,b1,c1,c3,d1,d3 from D join (A left outer join (B join C on b2=c2) on a1=b1) on d3=b3 and d1=a2", new String[][] {
			{"1","1","1","1","1","2"},
			{"7","7","8","9","1","3"} } },
		{ "select a1,b1,c1,c3,d1,d3 from D join ((B join C on b2=c2) right outer join A on a1=b1) on d3=b3 and d1=a2", new String[][] {
			{"1","1","1","1","1","2"},
			{"7","7","8","9","1","3"} } },
		// demonstrate that a table with an identity column generated always can be used as the target of an insert-as-select join:
		{ "create table j1089_source (source_id int)", null },
		{ "insert into j1089_source values (0)", null },
		{ "create table j1089_dest (dest_id int not null primary key generated always as identity,source_id_1 int not null,source_id_2 int not null)", null },
		{ "insert into j1089_dest (source_id_1, source_id_2) select s1.source_id, s2.source_id from j1089_source as s1 join j1089_source as s2 on 1 = 1", null },
		{ "select * from j1089_dest", new String[][] { {"1","0","0"} } },
		{ "drop table a", null },
		{ "drop table b", null },
		{ "drop table c", null },
		{ "drop table d", null },
		{ "drop table t1", null },
		{ "drop table t2", null },
		{ "drop table t3", null },
		{ "drop table t4", null },
		{ "drop table instab", null },
		{ "drop table y", null },
		{ "drop table x", null },
		{ "drop table j1089_source", null },
		{ "drop table j1089_dest", null }
	    };

	    // Start 1 client and 3 servers, use default partitioning
	    startVMs(1, 3);

	    Connection conn = TestUtil.getConnection();
	    Statement stmt = conn.createStatement();
	    // Go through the array, execute each string[0], check sqlstate [1]
	    // This will fail on the first one that succeeds where it shouldn't
	    // or throws unknown exception
	    JDBC.SQLUnitTestHelper(stmt,Script_JoinsUT);
	  }

}
