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

public class LangScripts_SubqueryTest extends JdbcTestBase {

  public static void main(String[] args) {
    TestRunner.run(new TestSuite(LangScripts_SubqueryTest.class));
  }
  
  public LangScripts_SubqueryTest(String name) {
    super(name); 
  }

  @Override
  protected String reduceLogging() {
    return "config";
  }

  // This test is the as-is LangScript conversion, without any partitioning clauses
  public void testLangScript_SubqueryTestNoPartitioning() throws Exception
  {
    // This is a JUnit conversion of the Derby Lang Subquery.sql script
    // without any GemFireXD extensions
	  
    // Catch exceptions from illegal syntax
    // Tests still not fixed marked FIXME
	  
    // Array of SQL text to execute and sqlstates to expect
    // The first object is a String, the second is either 
    // 1) null - this means query returns no rows and throws no exceptions
    // 2) a string - this means query returns no rows and throws expected SQLSTATE
    // 3) a String array - this means query returns rows which must match (unordered) given resultset
    //       - for an empty result set, an uninitialized size [0][0] array is used
    Object[][] Script_SubqueryUT = {
	// subquery tests
	{ "create table s (i int, s smallint, c char(30), vc char(30), b bigint)"+getOffHeapSuffix(), null },
	{ "create table t (i int, s smallint, c char(30), vc char(30), b bigint)"+getOffHeapSuffix(), null },
	{ "create table tt (ii int, ss smallint, cc char(30), vcvc char(30), b bigint)"+getOffHeapSuffix(), null },
	{ "create table ttt (iii int, sss smallint, ccc char(30), vcvcvc char(30))"+getOffHeapSuffix(), null },
	{ "insert into s values (null, null, null, null, null)", null },
	{ "insert into s values (0, 0, '0', '0', 0)", null },
	{ "insert into s values (1, 1, '1', '1', 1)", null },
	{ "insert into t values (null, null, null, null, null)", null },
	{ "insert into t values (0, 0, '0', '0', 0)", null },
	{ "insert into t values (1, 1, '1', '1', 1)", null },
	{ "insert into t values (1, 1, '1', '1', 1)", null },
	{ "insert into t values (2, 2, '2', '2', 1)", null },
	{ "insert into tt values (null, null, null, null, null)", null },
	{ "insert into tt values (0, 0, '0', '0', 0)", null },
	{ "insert into tt values (1, 1, '1', '1', 1)", null },
	{ "insert into tt values (1, 1, '1', '1', 1)", null },
	{ "insert into tt values (2, 2, '2', '2', 1)", null },
	{ "insert into ttt values (null, null, null, null)", null },
	{ "insert into ttt values (11, 11, '11', '11')", null },
	{ "insert into ttt values (11, 11, '11', '11')", null },
	{ "insert into ttt values (22, 22, '22', '22')", null },
	{ "select * from s where exists (select tt.* from t)", "42X10" },
	{ "select * from s where exists (select t.* from t tt)", "42X10" },
	{ "select * from s where exists (select i, s from t)", "42X39" },
	{ "select * from s where exists (select nosuchcolumn from t)", "42X04" },
	{ "select * from s where exists (select i from s, t)", "42X03" },
	//Original
	//{ "select * from s where exists (select ? from s)", "42X34" },
	{ "select * from s where exists (select ? from s)", "07000" },
	{ "select * from s where exists (select s.* from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
	{ "select * from s t where exists (select t.* from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
	{ "select * from s u where exists (select u.* from t)", "42X10" },
	{ "select * from s where exists (select i from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
	{ "select * from s where exists (select t.i from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
	{ "select * from s where exists (select * from t where i = -1)", new String[0][0] },
	{ "select * from s where exists (select t.* from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
	{ "select * from s where exists (select 0 from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
	{ "select * from (select * from s where exists (select * from t) and i = 0) a", new String[][] {
		{"0","0","0","0","0"} } },
	{ "select * from s where 0=1 or exists (select * from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
	{ "select * from s where 1=1 or exists (select * from t where 0=1)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
	{ "select * from s where exists (select * from t where 0=1) or exists (select * from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
	{ "select * from s where exists (select * from t where exists (select * from t where 0=1) or exists (select * from t))", "0A000" },  // GemFireXD restriction - only nesting of one level
	{ "select * from s where (exists (select * from t where 0=1)) is null", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
	{ "select * from s where not exists (select * from t)", new String[0][0] },
	{ "select * from s where not exists (select * from t where i = -1)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
	{ "select * from s where i = (select * from t)", "42X38" },
	{ "select * from s where i = (select i, s from t)", "42X39" },
	{ "select * from s where i = (select 1 from t)", "21000" },
	{ "select * from s where i = (select b from t)", "21000" },
	//Original
	//{ "select * from s where i = (select ? from t)", "42X34" },
	{ "select * from s where i = (select ? from t)", "07000" },
	{ "select * from s where i = (select i from t)", "21000" },
	{ "select * from s where s = (select s from t where s = 1)", "21000" },
	{ "update s set b = (select max(b) from t) where vc <> (select vc from t where vc = '1')", "21000" },
	{ "delete from s where c = (select c from t where c = '1')", "21000" },
	{ "select * from s where i = (select i from t where i = 0)", new String[][] { {"0","0","0","0","0"} } },
	{ "select * from s where s = (select s from t where s = 0)", new String[][] { {"0","0","0","0","0"} } },
	{ "select * from s where c = (select c from t where c = '0')", new String[][] { {"0","0","0","0","0"} } },
	{ "select * from s where vc = (select vc from t where vc = '0')", new String[][] { {"0","0","0","0","0"} } },
	{ "select * from s where b = (select max(b) from t where b = 0)", new String[][] { {"0","0","0","0","0"} } },
	{ "select * from s where b = (select max(b) from t where i = 2)", new String[][] { {"1","1","1","1","1"} } },
	{ "select * from s where i = (select s from t where s = 0)", new String[][] { {"0","0","0","0","0"} } },
	{ "select * from s where s = (select i from t where i = 0)", new String[][] { {"0","0","0","0","0"} } },
	{ "select * from s where c = (select vc from t where vc = '0')", new String[][] { {"0","0","0","0","0"} } },
	{ "select * from s where vc = (select c from t where c = '0')", new String[][] { {"0","0","0","0","0"} } },
	{ "select * from s where (select s from s where i is null) is null", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
	{ "select * from s where (select i from s where i is null) is null", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
	{ "select * from s where (select c from s where i is null) is null", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
	{ "select * from s where (select vc from s where i is null) is null", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
	{ "select * from s where (select b from s where i is null) is null", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
	{ "select * from s where (select 1 from t where exists (select * from t where 1 = 0) and s = -1) is null", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
	{ "select * from s where (select i from t where i = 0) = (select s from t where s = 0)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
	{ "select * from s where i = (select s from t where s = 0) and s = (select i from t where i = 2)", new String[0][0] },
	{ "select * from s where i = (select s from t where s = 0) and s = (select i from t where i = 0)", new String[][] { {"0","0","0","0","0"} } },
	{ "select * from s where i = (select i from t where s = (select i from t where s = 2))", "0A000" },
	{ "select * from s where i = (select i - 1 from t where s = (select i from t where s = 2))", "0A000" },
	{ "select (select i from t where 0=1) from s", new String[][] { {null},{null},{null} } },
	{ "select (select i from t where i = 2) * (select s from t where i = 2) from s where i > (select i from t where i = 0) - (select i from t where i = 0)", new String[][] { {"4"} } },
	{ "select * from s where s in (select * from s)", "42X38" },
	{ "select * from s where s in (select b from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"} } },         // Derby fails this query, GemFireXD does autoconvert smallint->bigint
	//FIXME
	//GemFireXD throws Java exception: 'target=10 binary types underlying value=80: java.lang.AssertionError'.
	//{ "select * from s where 1 in (select s from t)", new String[][] {
	//	{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
	{ "select * from s where -1 in (select i from t)", new String[0][0] },
	//{ "select * from s where '1' in (select vc from t)", new String[][] {
	//	{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
	//{ "select * from s where 0 in (select b from t)", new String[][] {
	//	{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
	{ "select * from s where i in (select 1 from t)", new String[][] { {"1","1","1","1","1"} } },
	{ "select * from s where i in (select -1 from t)", new String[0][0] },
	{ "select * from s where c in (select '1' from t)", new String[][] { {"1","1","1","1","1"} } },
	{ "select * from s where b in (select 0 from t)", new String[][] { {"0","0","0","0","0"} } },
	{ "select * from s where 1=1 in (select 0 from t)", new String[0][0] },
	{ "select * from s where 0 in (select 0 from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
	{ "select * from s where c in (select vc from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"} } },
	{ "select * from s where vc in (select c from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"} } },
	{ "select * from s where i in (select s from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"} } },
	{ "select * from s where s in (select i from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"} } },
	{ "select * from s where i in (select i from t where 1 = 0)", new String[0][0] },
	{ "select * from s where (i in (select i from t where i = 0)) is null", new String[0][0] },
	{ "select ( i in (select i from t) ) a from s order by a", "42X01" },
	{ "select ( i in (select i from t where 1 = 0) ) a from s order by a", "42X01" },
	{ "select ( (i in (select i from t where 1 = 0)) is null ) a from s order by a", "42X01" },
	{ "select i from s where i = -1 or i in (select i from t)", new String[][] { {"0"},{"1"} } },
	{ "select i from s where i = 0 or i in (select i from t where i = -1)", new String[][] { {"0"} } },
	{ "select i from s where i = -1 or i in (select i from t where i = -1 or i = 1)", new String[][] { {"1"} } },
	{ "select i from s where i in (select i from s)", new String[][] { {"1"},{"0"} } },
	{ "select i from s where i in (select distinct i from s)", new String[][] { {"1"},{"0"} } },
	{ "select i from s ss where i in (select i from s where s.i = ss.i)", new String[][] { {"1"},{"0"} } },
	{ "select i from s ss where i in (select distinct i from s where s.i = ss.i)", new String[][] { {"1"},{"0"} } },
	{ "select * from s, t where exists (select i from tt)", "42X03" },
	{ "select * from s ss (c1, c2, c3, c4, c5) where exists (select i from tt)", "42X04" },
	{ "select * from s ss (c1, c2, c3, c4, c5) where exists (select ss.i from tt)", "42X04" },
	{ "select * from s where exists (select s.i from tt s)", "42X04" },
	{ "select * from s where exists (select * from tt) and exists (select ii from t)", "42X04" },
	{ "select * from s where exists (select * from tt) and exists (select tt.ii from t)", "42X04" },
	{ "select * from s, (select * from tt where i = ii) a", "42X04" },
	{ "select * from s, (select * from tt where s.i = ii) a", "42X04" },
	{ "select (select i from tt where ii = i and ii <> 1) from s", new String[][] { {null},{"0"},{null} } },
	{ "select (select s.i from tt where ii = s.i and ii <> 1) from s",new String[][] { {null},{"0"},{null} } },
	{ "select (select s.i from ttt where iii = i) from s", new String[][] { {null},{null},{null} } },
	{ "select * from s where exists (select * from tt where i = ii and ii <> 1)", new String[][] { {"0","0","0","0","0"} } },
	{ "select * from s where exists (select * from tt where s.i = ii and ii <> 1)", new String[][] { {"0","0","0","0","0"} } },
	{ "select * from s where exists (select * from ttt where i = iii)", new String[0][0] },
	{ "select (select i from tt where ii = i) from s", "21000" },
	{ "select * from s where exists (select * from ttt where iii = (select 11 from tt where ii = i and ii <> 1))", new String[][] { {"0","0","0","0","0"} } },
	{ "select * from s where i in (select i from t, tt where s.i <> i and i = ii)", new String[0][0] },
	{ "select * from s where i in (select i from t, ttt where s.i < iii and s.i = t.i)", new String[][] { {"1","1","1","1","1"},{"0","0","0","0","0"} } },
	{ "select s.i, t.i from s, t where exists (select * from ttt where iii = 1)", new String[0][0] },
	{ "select s.i, t.i from s, t where t.i = (select iii from ttt, tt where iii = t.i)", new String[0][0] },
	{ "select s.i, t.i from s, t where t.i = (select ii from ttt, tt where s.i = t.i and t.i = tt.ii and iii = 22 and ii <> 1)", new String[][] { {"0","0"} } },
	{ "select * from (select (select iii from ttt where sss > i and sss = iii and iii <> 11) from s) a", new String[][] { {"22"},{"22"},{null} } },
	{ "create table li(i int, s smallint, l bigint)"+getOffHeapSuffix(), null },
	{ "insert into li values (null, null, null)", null },
	{ "insert into li values (1, 1, 1)", null },
	{ "insert into li values (2, 2, 2)", null },
	{ "select l from li o where l = (select i from li i where o.l = i.i)", new String[][] { {"2"},{"1"} } },
	{ "select l from li o where l = (select s from li i where o.l = i.s)", new String[][] { {"2"},{"1"} } },
	{ "select l from li o where l = (select l from li i where o.l = i.l)", new String[][] { {"2"},{"1"} } },
	{ "select l from li where l in (select i from li)", new String[][] { {"2"},{"1"} } },
	{ "select l from li where l in (select s from li)", new String[][] { {"2"},{"1"} } },
	{ "select l from li where l in (select l from li)", new String[][] { {"2"},{"1"} } },
	{ "select i in (1,2) from (select i from s) as tmp(i)", "42X01" },
	{ "select i = 1 ? 1 : i from (select i from s) as tmp(i)", "42X01" },
	{ "select * from s where i = (select min(i) from s where i is not null)", new String[][] { {"0","0","0","0","0"} } },
	{ "select * from s where i = (select min(i) from s group by i)", "21000" },
	{ "create table dist1 (c1 int)"+getOffHeapSuffix(), null },
	{ "create table dist2 (c1 int)"+getOffHeapSuffix(), null },
	{ "insert into dist1 values null, 1, 2", null },
	{ "insert into dist2 values null, null", null },
	{ "select * from dist1 where c1 = (select distinct c1 from dist2)", new String[0][0] },
	{ "insert into dist2 values 1", null },
	{ "select * from dist1 where c1 = (select distinct c1 from dist2)", "21000" },
	{ "update dist2 set c1 = 2", null },
	{ "select * from dist1 where c1 = (select distinct c1 from dist2)", new String[][] { {"2"} } },
	{ "drop table dist1", null },
	{ "drop table dist2", null },
	{ "create table u (i int, s smallint, c char(30), vc char(30), b bigint)"+getOffHeapSuffix(), null },
	{ "insert into u select * from s", null },
	{ "update u set b = exists (select b from t) where vc <> (select vc from s where vc = '1')", "42X01" },
	{ "delete from u where c < (select c from t where c = '2')", null },
	{ "select * from u", new String[][] { {null,null,null,null,null} } },
	{ "delete from u", null },
	{ "insert into u select * from s", null },
	{ "insert into u select * from s s_outer where i = (select s_inner.i/(s_inner.i-1) from s s_inner where s_outer.i = s_inner.i)", "22012" },
	{ "delete from u", null },
	{ "insert into u select * from s", null },
	{ "select (select (select (select i from s) from s) from s) from s", "21000" },
	{ "select distinct vc, i from t as myt1 where s <= (select max(myt1.s) from t as myt2 where myt1.vc = myt2.vc and myt1.s <= myt2.s group by s          having count(distinct s) <= 3)", new String[][] {
		{"0","0"},{"1","1"},{"2","2"} } },
	{ "select distinct vc, i from t as myt1 where s <= (select max(myt1.s) from t as myt2 where myt1.vc = myt2.vc and myt1.s <= myt2.s having count(distinct s) <= 3)", new String[][] {
		{"0","0"},{"1","1"},{"2","2"} } },
	{ "drop table li", null },
	{ "drop table s", null },
	{ "drop table t", null },
	{ "drop table tt", null },
	{ "drop table ttt", null },
	{ "drop table u", null },
	{ "create table t1 (i int, j int)"+getOffHeapSuffix(), null },
     { "insert into T1 values (1,1), (2,2), (3,3), (4,4), (5,5)", null },
     { "create table t3 (a int, b int)"+getOffHeapSuffix(), null },
     { "insert into T3 values (1,1), (2,2), (3,3), (4,4)", null },
     { "insert into t3 values (6, 24), (7, 28), (8, 32), (9, 36), (10, 40)", null },
     { "select x1.j, x2.b from (select distinct i,j from t1) x1,(select distinct a,b from t3) x2 where x1.i = x2.a order by x1.j, x2.b", new String[][] {
		{"1","1"},{"2","2"},{"3","3"},{"4","4"} } },
	{ "drop table t1", null },
     { "drop table t3", null },
	{ "create table t1 (i int, j int)"+getOffHeapSuffix(), null },
     { "create table t2 (i int, j int)"+getOffHeapSuffix(), null },
     { "insert into t1 values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)", null },
     { "insert into t2 values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)", null },
     { "create table t3 (a int, b int)"+getOffHeapSuffix(), null },
     { "create table t4 (a int, b int)"+getOffHeapSuffix(), null },
     { "insert into t3 values (2, 2), (4, 4), (5, 5)", null },
     { "insert into t4 values (2, 2), (4, 4), (5, 5)", null },
	{ "create view V1 as select distinct T1.i, T2.j from T1, T2 where T1.i = T2.i", null },
     { "create view V2 as select distinct T3.a, T4.b from T3, T4 where T3.a = T4.a", null },
	{ "select * from V1, V2 where V1.j = V2.b and V1.i in (1,2,3,4,5)", new String[][] {
		{"2","2","2","2"},{"4","4","4","4"},{"5","5","5","5"} } },
	{ "insert into t1 select * from t2", null },
     { "insert into t2 select * from t1", null },
     { "insert into t2 select * from t1", null },
     { "insert into t1 select * from t2", null },
     { "insert into t2 select * from t1", null },
     { "insert into t1 select * from t2", null },
     { "insert into t2 select * from t1", null },
     { "insert into t1 select * from t2", null },
     { "insert into t2 select * from t1", null },
     { "insert into t1 select * from t2", null },
     { "insert into t3 select * from t4", null },
     { "insert into t4 select * from t3", null },
     { "insert into t3 select * from t4", null },
     { "insert into t4 select * from t3", null },
     { "insert into t3 select * from t4", null },
     { "insert into t4 select * from t3", null },
     { "insert into t3 select * from t4", null },
     { "insert into t4 select * from t3", null },
     { "insert into t3 select * from t4", null },
     { "insert into t4 select * from t3", null },
     { "insert into t3 select * from t4", null },
	{ "drop view v1", null },
     { "drop view v2", null },
	{ "create view VV1 as select distinct T1.i, T2.j from T1, T2 where T1.i = T2.i", null },
     { "create view VV2 as select distinct T3.a, T4.b from T3, T4 where T3.a = T4.a", null },
	{ "select * from VV1, VV2 where VV1.j = VV2.b and VV1.i in (1,2,3,4,5)", new String[][] {
		{"2","2","2","2"},{"4","4","4","4"},{"5","5","5","5"} } },
	{ "drop view vv1", null },
     { "drop view vv2", null },
     { "drop table t1", null },
     { "drop table t2", null },
     { "drop table t3", null },
     { "drop table t4", null },
	{ "create table t1 (id int)"+getOffHeapSuffix(), null },
     { "create table t2 (i integer primary key, j int)"+getOffHeapSuffix(), null },
     { "insert into t1 values 1,2,3,4,5", null },
     { "insert into t2 values (1,1),(2,4),(3,9),(4,16)", null },
     { "update t1 set id = coalesce((select j from t2 where t2.i=t1.id), 0)", null },
     { "select * from t1", new String[][] { {"0"},{"16"},{"9"},{"4"},{"1"} } },
	{ "drop table t1", null },
     { "drop table t2", null },
	{ "create table t1 (i int)"+getOffHeapSuffix(), null },
     { "select * from t1 where i in (1, 2, (values cast(null as integer)))", new String[0][0] },
	{ "select * from t1 where i in (1, 2, (values null))", "42X07" },
     { "select * from t1 where i in (select i from t1 where i in (1, 2, (values null)))", "42X07" },
     { "select * from t1 where exists (values null)", "42X07"},
     { "select * from t1 where exists (select * from t1 where exists(values null))", "42X07" },
     { "select i from t1 where exists (select i from t1 where exists(values null))", "42X07" },
     { "select * from (values null) as t2", "42X07" },
     { "select * from t1 where exists (select 1 from (values null) as t2)", "42X07" },
     { "select * from t1 where exists (select * from (values null) as t2)", "42X07" },
	{ "drop table t1", null }
   };

    // Do not use partitioning as default, use replicate
    // (Some results are expected to be different with partitioning)
    skipDefaultPartitioned = true;

    Connection conn = TestUtil.getConnection();
    Statement stmt = conn.createStatement();
    // Go through the array, execute each string[0], check sqlstate [1]
    // This will fail on the first one that succeeds where it shouldn't
    // or throws unknown exception
    JDBC.SQLUnitTestHelper(stmt,Script_SubqueryUT);
  }
  
  // This test is the script enhanced with partitioning
  public void testLangScript_SubqueryWithPartitioning() throws Exception
  {
    // This form of the Subquery.sql test has partitioning activated
    // TODO : Create specialized partitioning for these tables for Subquery
    //  Currently, default partitioning is active
    Object[][] Script_SubqueryUTPartitioning = {
	// subquery tests
	{ "create table s (i int, s smallint, c char(30), vc char(30), b bigint)"+getOffHeapSuffix(), null },
	{ "create table t (i int, s smallint, c char(30), vc char(30), b bigint)"+getOffHeapSuffix(), null },
	{ "create table tt (ii int, ss smallint, cc char(30), vcvc char(30), b bigint)"+getOffHeapSuffix(), null },
	{ "create table ttt (iii int, sss smallint, ccc char(30), vcvcvc char(30))"+getOffHeapSuffix(), null },
	{ "insert into s values (null, null, null, null, null)", null },
	{ "insert into s values (0, 0, '0', '0', 0)", null },
	{ "insert into s values (1, 1, '1', '1', 1)", null },
	{ "insert into t values (null, null, null, null, null)", null },
	{ "insert into t values (0, 0, '0', '0', 0)", null },
	{ "insert into t values (1, 1, '1', '1', 1)", null },
	{ "insert into t values (1, 1, '1', '1', 1)", null },
	{ "insert into t values (2, 2, '2', '2', 1)", null },
	{ "insert into tt values (null, null, null, null, null)", null },
	{ "insert into tt values (0, 0, '0', '0', 0)", null },
	{ "insert into tt values (1, 1, '1', '1', 1)", null },
	{ "insert into tt values (1, 1, '1', '1', 1)", null },
	{ "insert into tt values (2, 2, '2', '2', 1)", null },
	{ "insert into ttt values (null, null, null, null)", null },
	{ "insert into ttt values (11, 11, '11', '11')", null },
	{ "insert into ttt values (11, 11, '11', '11')", null },
	{ "insert into ttt values (22, 22, '22', '22')", null },
	{ "select * from s where exists (select tt.* from t)", "42X10" },
	{ "select * from s where exists (select t.* from t tt)", "42X10" },
	{ "select * from s where exists (select i, s from t)", "42X39" },
	{ "select * from s where exists (select nosuchcolumn from t)", "42X04" },
	{ "select * from s where exists (select i from s, t)", "42X03" },
	//Original
	//{ "select * from s where i = (select ? from t)", "42X34" },
	{ "select * from s where i = (select ? from t)", "07000" },
	{ "select * from s where exists (select s.* from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
	{ "select * from s t where exists (select t.* from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
	{ "select * from s u where exists (select u.* from t)", "42X10" },
	{ "select * from s where exists (select i from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
	{ "select * from s where exists (select t.i from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
	{ "select * from s where exists (select * from t where i = -1)", new String[0][0] },
	{ "select * from s where exists (select t.* from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
	{ "select * from s where exists (select 0 from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
	{ "select * from (select * from s where exists (select * from t) and i = 0) a", new String[][] {
		{"0","0","0","0","0"} } },
	{ "select * from s where 0=1 or exists (select * from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
	{ "select * from s where 1=1 or exists (select * from t where 0=1)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
	{ "select * from s where exists (select * from t where 0=1) or exists (select * from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
	{ "select * from s where exists (select * from t where exists (select * from t where 0=1) or exists (select * from t))", "0A000" },  // GemFireXD restriction - only nesting of one level
	{ "select * from s where (exists (select * from t where 0=1)) is null", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
	{ "select * from s where not exists (select * from t)", new String[0][0] },
	{ "select * from s where not exists (select * from t where i = -1)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
	{ "select * from s where i = (select * from t)", "42X38" },
	{ "select * from s where i = (select i, s from t)", "42X39" },
	{ "select * from s where i = (select 1 from t)", "21000" },
	{ "select * from s where i = (select b from t)", "21000" },
	//Original
	//{ "select * from s where i = (select ? from t)", "42X34" },
	{ "select * from s where i = (select ? from t)", "07000" },
	{ "select * from s where i = (select i from t)", "21000" },
	{ "select * from s where s = (select s from t where s = 1)", "21000" },
	{ "update s set b = (select max(b) from t) where vc <> (select vc from t where vc = '1')", "21000" },
	{ "delete from s where c = (select c from t where c = '1')", "21000" },
	{ "select * from s where i = (select i from t where i = 0)", new String[][] { {"0","0","0","0","0"} } },
	{ "select * from s where s = (select s from t where s = 0)", new String[][] { {"0","0","0","0","0"} } },
	{ "select * from s where c = (select c from t where c = '0')", new String[][] { {"0","0","0","0","0"} } },
	{ "select * from s where vc = (select vc from t where vc = '0')", new String[][] { {"0","0","0","0","0"} } },
	{ "select * from s where b = (select max(b) from t where b = 0)", new String[][] { {"0","0","0","0","0"} } },
	{ "select * from s where b = (select max(b) from t where i = 2)", new String[][] { {"1","1","1","1","1"} } },
	{ "select * from s where i = (select s from t where s = 0)", new String[][] { {"0","0","0","0","0"} } },
	{ "select * from s where s = (select i from t where i = 0)", new String[][] { {"0","0","0","0","0"} } },
	{ "select * from s where c = (select vc from t where vc = '0')", new String[][] { {"0","0","0","0","0"} } },
	{ "select * from s where vc = (select c from t where c = '0')", new String[][] { {"0","0","0","0","0"} } },
	{ "select * from s where (select s from s where i is null) is null", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
	{ "select * from s where (select i from s where i is null) is null", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
	{ "select * from s where (select c from s where i is null) is null", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
	{ "select * from s where (select vc from s where i is null) is null", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
	{ "select * from s where (select b from s where i is null) is null", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
	{ "select * from s where (select 1 from t where exists (select * from t where 1 = 0) and s = -1) is null", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
	{ "select * from s where (select i from t where i = 0) = (select s from t where s = 0)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
	{ "select * from s where i = (select s from t where s = 0) and s = (select i from t where i = 2)", new String[0][0] },
	{ "select * from s where i = (select s from t where s = 0) and s = (select i from t where i = 0)", new String[][] { {"0","0","0","0","0"} } },
	{ "select * from s where i = (select i from t where s = (select i from t where s = 2))", "0A000" },
	{ "select * from s where i = (select i - 1 from t where s = (select i from t where s = 2))", "0A000" },
	{ "select (select i from t where 0=1) from s", new String[][] { {null},{null},{null} } },
	{ "select (select i from t where i = 2) * (select s from t where i = 2) from s where i > (select i from t where i = 0) - (select i from t where i = 0)", new String[][] { {"4"} } },
	{ "select * from s where s in (select * from s)", "42X38" },
	{ "select * from s where s in (select b from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"} } },         // Derby fails this query, GemFireXD does autoconvert smallint->bigint
	//FIXME
	//GemFireXD throws Java exception: 'target=10 binary types underlying value=80: java.lang.AssertionError'.
	//{ "select * from s where 1 in (select s from t)", new String[][] {
	//	{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
	{ "select * from s where -1 in (select i from t)", new String[0][0] },
	//{ "select * from s where '1' in (select vc from t)", new String[][] {
	//	{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
	//{ "select * from s where 0 in (select b from t)", new String[][] {
	//	{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
	{ "select * from s where i in (select 1 from t)", new String[][] { {"1","1","1","1","1"} } },
	{ "select * from s where i in (select -1 from t)", new String[0][0] },
	{ "select * from s where c in (select '1' from t)", new String[][] { {"1","1","1","1","1"} } },
	{ "select * from s where b in (select 0 from t)", new String[][] { {"0","0","0","0","0"} } },
	{ "select * from s where 1=1 in (select 0 from t)", new String[0][0] },
	{ "select * from s where 0 in (select 0 from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
	{ "select * from s where c in (select vc from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"} } },
	{ "select * from s where vc in (select c from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"} } },
	{ "select * from s where i in (select s from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"} } },
	{ "select * from s where s in (select i from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"} } },
	{ "select * from s where i in (select i from t where 1 = 0)", new String[0][0] },
	{ "select * from s where (i in (select i from t where i = 0)) is null", new String[0][0] },
	{ "select ( i in (select i from t) ) a from s order by a", "42X01" },
	{ "select ( i in (select i from t where 1 = 0) ) a from s order by a", "42X01" },
	{ "select ( (i in (select i from t where 1 = 0)) is null ) a from s order by a", "42X01" },
	{ "select i from s where i = -1 or i in (select i from t)", new String[][] { {"0"},{"1"} } },
	{ "select i from s where i = 0 or i in (select i from t where i = -1)", new String[][] { {"0"} } },
	{ "select i from s where i = -1 or i in (select i from t where i = -1 or i = 1)", new String[][] { {"1"} } },
	{ "select i from s where i in (select i from s)", new String[][] { {"1"},{"0"} } },
	{ "select i from s where i in (select distinct i from s)", new String[][] { {"1"},{"0"} } },
	{ "select i from s ss where i in (select i from s where s.i = ss.i)", new String[][] { {"1"},{"0"} } },
	{ "select i from s ss where i in (select distinct i from s where s.i = ss.i)", new String[][] { {"1"},{"0"} } },
	{ "select * from s, t where exists (select i from tt)", "42X03" },
	{ "select * from s ss (c1, c2, c3, c4, c5) where exists (select i from tt)", "42X04" },
	{ "select * from s ss (c1, c2, c3, c4, c5) where exists (select ss.i from tt)", "42X04" },
	{ "select * from s where exists (select s.i from tt s)", "42X04" },
	{ "select * from s where exists (select * from tt) and exists (select ii from t)", "42X04" },
	{ "select * from s where exists (select * from tt) and exists (select tt.ii from t)", "42X04" },
	{ "select * from s, (select * from tt where i = ii) a", "42X04" },
	{ "select * from s, (select * from tt where s.i = ii) a", "42X04" },
	{ "select (select i from tt where ii = i and ii <> 1) from s", new String[][] { {null},{"0"},{null} } },
	{ "select (select s.i from tt where ii = s.i and ii <> 1) from s",new String[][] { {null},{"0"},{null} } },
	{ "select (select s.i from ttt where iii = i) from s", new String[][] { {null},{null},{null} } },
	{ "select * from s where exists (select * from tt where i = ii and ii <> 1)", new String[][] { {"0","0","0","0","0"} } },
	{ "select * from s where exists (select * from tt where s.i = ii and ii <> 1)", new String[][] { {"0","0","0","0","0"} } },
	{ "select * from s where exists (select * from ttt where i = iii)", new String[0][0] },
	{ "select (select i from tt where ii = i) from s", "21000" },
	{ "select * from s where exists (select * from ttt where iii = (select 11 from tt where ii = i and ii <> 1))", new String[][] { {"0","0","0","0","0"} } },
	{ "select * from s where i in (select i from t, tt where s.i <> i and i = ii)", new String[0][0] },
	{ "select * from s where i in (select i from t, ttt where s.i < iii and s.i = t.i)", new String[][] { {"1","1","1","1","1"},{"0","0","0","0","0"} } },
	{ "select s.i, t.i from s, t where exists (select * from ttt where iii = 1)", new String[0][0] },
	{ "select s.i, t.i from s, t where t.i = (select iii from ttt, tt where iii = t.i)", new String[0][0] },
	{ "select s.i, t.i from s, t where t.i = (select ii from ttt, tt where s.i = t.i and t.i = tt.ii and iii = 22 and ii <> 1)", new String[][] { {"0","0"} } },
	{ "select * from (select (select iii from ttt where sss > i and sss = iii and iii <> 11) from s) a", new String[][] { {"22"},{"22"},{null} } },
	{ "create table li(i int, s smallint, l bigint)"+getOffHeapSuffix(), null },
	{ "insert into li values (null, null, null)", null },
	{ "insert into li values (1, 1, 1)", null },
	{ "insert into li values (2, 2, 2)", null },
	{ "select l from li o where l = (select i from li i where o.l = i.i)", new String[][] { {"2"},{"1"} } },
	{ "select l from li o where l = (select s from li i where o.l = i.s)", new String[][] { {"2"},{"1"} } },
	{ "select l from li o where l = (select l from li i where o.l = i.l)", new String[][] { {"2"},{"1"} } },
	{ "select l from li where l in (select i from li)", new String[][] { {"2"},{"1"} } },
	{ "select l from li where l in (select s from li)", new String[][] { {"2"},{"1"} } },
	{ "select l from li where l in (select l from li)", new String[][] { {"2"},{"1"} } },
	{ "select i in (1,2) from (select i from s) as tmp(i)", "42X01" },
	{ "select i = 1 ? 1 : i from (select i from s) as tmp(i)", "42X01" },
	{ "select * from s where i = (select min(i) from s where i is not null)", new String[][] { {"0","0","0","0","0"} } },
	{ "select * from s where i = (select min(i) from s group by i)", "21000" },
	{ "create table dist1 (c1 int)"+getOffHeapSuffix(), null },
	{ "create table dist2 (c1 int)"+getOffHeapSuffix(), null },
	{ "insert into dist1 values null, 1, 2", null },
	{ "insert into dist2 values null, null", null },
	{ "select * from dist1 where c1 = (select distinct c1 from dist2)", new String[0][0] },
	{ "insert into dist2 values 1", null },
	{ "select * from dist1 where c1 = (select distinct c1 from dist2)", "21000" },
	{ "update dist2 set c1 = 2", null },
	{ "select * from dist1 where c1 = (select distinct c1 from dist2)", new String[][] { {"2"} } },
	{ "drop table dist1", null },
	{ "drop table dist2", null },
	{ "create table u (i int, s smallint, c char(30), vc char(30), b bigint)"+getOffHeapSuffix(), null },
	{ "insert into u select * from s", null },
	{ "update u set b = exists (select b from t) where vc <> (select vc from s where vc = '1')", "42X01" },
	{ "delete from u where c < (select c from t where c = '2')", null },
	{ "select * from u", new String[][] { {null,null,null,null,null} } },
	{ "delete from u", null },
	{ "insert into u select * from s", null },
	{ "insert into u select * from s s_outer where i = (select s_inner.i/(s_inner.i-1) from s s_inner where s_outer.i = s_inner.i)", "22012" },
	{ "delete from u", null },
	{ "insert into u select * from s", null },
	{ "select (select (select (select i from s) from s) from s) from s", "21000" },
	{ "select distinct vc, i from t as myt1 where s <= (select max(myt1.s) from t as myt2 where myt1.vc = myt2.vc and myt1.s <= myt2.s group by s          having count(distinct s) <= 3)", new String[][] {
		{"0","0"},{"1","1"},{"2","2"} } },
	{ "select distinct vc, i from t as myt1 where s <= (select max(myt1.s) from t as myt2 where myt1.vc = myt2.vc and myt1.s <= myt2.s having count(distinct s) <= 3)", new String[][] {
		{"0","0"},{"1","1"},{"2","2"} } },
	{ "drop table li", null },
	{ "drop table s", null },
	{ "drop table t", null },
	{ "drop table tt", null },
	{ "drop table ttt", null },
	{ "drop table u", null },
	{ "create table t1 (i int, j int)"+getOffHeapSuffix(), null },
     { "insert into T1 values (1,1), (2,2), (3,3), (4,4), (5,5)", null },
     { "create table t3 (a int, b int)"+getOffHeapSuffix(), null },
     { "insert into T3 values (1,1), (2,2), (3,3), (4,4)", null },
     { "insert into t3 values (6, 24), (7, 28), (8, 32), (9, 36), (10, 40)", null },
     { "select x1.j, x2.b from (select distinct i,j from t1) x1,(select distinct a,b from t3) x2 where x1.i = x2.a order by x1.j, x2.b", new String[][] {
		{"1","1"},{"2","2"},{"3","3"},{"4","4"} } },
	{ "drop table t1", null },
     { "drop table t3", null },
	{ "create table t1 (i int, j int)"+getOffHeapSuffix(), null },
     { "create table t2 (i int, j int)"+getOffHeapSuffix(), null },
     { "insert into t1 values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)", null },
     { "insert into t2 values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)", null },
     { "create table t3 (a int, b int)"+getOffHeapSuffix(), null },
     { "create table t4 (a int, b int)"+getOffHeapSuffix(), null },
     { "insert into t3 values (2, 2), (4, 4), (5, 5)", null },
     { "insert into t4 values (2, 2), (4, 4), (5, 5)", null },
	{ "create view V1 as select distinct T1.i, T2.j from T1, T2 where T1.i = T2.i", null },
     { "create view V2 as select distinct T3.a, T4.b from T3, T4 where T3.a = T4.a", null },
	{ "select * from V1, V2 where V1.j = V2.b and V1.i in (1,2,3,4,5)", new String[][] {
		{"2","2","2","2"},{"4","4","4","4"},{"5","5","5","5"} } },
	{ "insert into t1 select * from t2", null },
     { "insert into t2 select * from t1", null },
     { "insert into t2 select * from t1", null },
     { "insert into t1 select * from t2", null },
     { "insert into t2 select * from t1", null },
     { "insert into t1 select * from t2", null },
     { "insert into t2 select * from t1", null },
     { "insert into t1 select * from t2", null },
     { "insert into t2 select * from t1", null },
     { "insert into t1 select * from t2", null },
     { "insert into t3 select * from t4", null },
     { "insert into t4 select * from t3", null },
     { "insert into t3 select * from t4", null },
     { "insert into t4 select * from t3", null },
     { "insert into t3 select * from t4", null },
     { "insert into t4 select * from t3", null },
     { "insert into t3 select * from t4", null },
     { "insert into t4 select * from t3", null },
     { "insert into t3 select * from t4", null },
     { "insert into t4 select * from t3", null },
     { "insert into t3 select * from t4", null },
	{ "drop view v1", null },
     { "drop view v2", null },
	{ "create view VV1 as select distinct T1.i, T2.j from T1, T2 where T1.i = T2.i", null },
     { "create view VV2 as select distinct T3.a, T4.b from T3, T4 where T3.a = T4.a", null },
	{ "select * from VV1, VV2 where VV1.j = VV2.b and VV1.i in (1,2,3,4,5)", new String[][] {
		{"2","2","2","2"},{"4","4","4","4"},{"5","5","5","5"} } },
	{ "drop view vv1", null },
     { "drop view vv2", null },
     { "drop table t1", null },
     { "drop table t2", null },
     { "drop table t3", null },
     { "drop table t4", null },
	{ "create table t1 (id int)"+getOffHeapSuffix(), null },
     { "create table t2 (i integer primary key, j int)"+getOffHeapSuffix(), null },
     { "insert into t1 values 1,2,3,4,5", null },
     { "insert into t2 values (1,1),(2,4),(3,9),(4,16)", null },
     { "update t1 set id = coalesce((select j from t2 where t2.i=t1.id), 0)", null },
     { "select * from t1", new String[][] { {"0"},{"16"},{"9"},{"4"},{"1"} } },
	{ "drop table t1", null },
     { "drop table t2", null },
	{ "create table t1 (i int)"+getOffHeapSuffix(), null },
     { "select * from t1 where i in (1, 2, (values cast(null as integer)))", new String[0][0] },
	{ "select * from t1 where i in (1, 2, (values null))", "42X07" },
     { "select * from t1 where i in (select i from t1 where i in (1, 2, (values null)))", "42X07" },
     { "select * from t1 where exists (values null)", "42X07"},
     { "select * from t1 where exists (select * from t1 where exists(values null))", "42X07" },
     { "select i from t1 where exists (select i from t1 where exists(values null))", "42X07" },
     { "select * from (values null) as t2", "42X07" },
     { "select * from t1 where exists (select 1 from (values null) as t2)", "42X07" },
     { "select * from t1 where exists (select * from (values null) as t2)", "42X07" },
	{ "drop table t1", null }
    };

    Connection conn = TestUtil.getConnection();
    Statement stmt = conn.createStatement();
    // Go through the array, execute each string[0], check sqlstate [1]
    // This will fail on the first one that succeeds where it shouldn't
    // or throws unknown exception
    JDBC.SQLUnitTestHelper(stmt,Script_SubqueryUTPartitioning);

  }
  
  protected String getOffHeapSuffix() {
    return "  ";
  }
}
