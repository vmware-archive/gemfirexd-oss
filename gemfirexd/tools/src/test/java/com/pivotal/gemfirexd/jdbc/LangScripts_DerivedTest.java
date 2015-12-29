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

public class LangScripts_DerivedTest extends JdbcTestBase {

  public static void main(String[] args) {
    TestRunner.run(new TestSuite(LangScripts_DerivedTest.class));
  }
  
  public LangScripts_DerivedTest(String name) {
    super(name); 
  }

  // This test is the as-is LangScript conversion, without any partitioning clauses
  public void testLangScript_DerivedTestNoPartitioning() throws Exception
  {
    // This is a JUnit conversion of the Derby Lang derived.sql script
    // without any GemFireXD extensions
	  
    // Catch exceptions from illegal syntax
    // Tests still not fixed marked FIXME
	  
    // Array of SQL text to execute and sqlstates to expect
    // The first object is a String, the second is either 
    // 1) null - this means query returns no rows and throws no exceptions
    // 2) a string - this means query returns no rows and throws expected SQLSTATE
    // 3) a String array - this means query returns rows which must match (unordered) given resultset
    //       - for an empty result set, an uninitialized size [0][0] array is used
    Object[][] Script_DerivedUT = {
	// Derived table tests
	{ "create table s (a int, b int, c int, d int, e int, f int)", null },
	{ "create table t (aa int, bb int, cc int, dd int, ee int, ff int)", null },
	{ "insert into s values (0,1,2,3,4,5)", null },
	{ "insert into s values (10,11,12,13,14,15)", null },
	// negative tests
	{ "select aa from s ss (aa)", "42X32" },
	{ "select aa from s ss (aa, bb, cc, dd, ee, ff, gg)", "42X32" },
	{ "select aa from s ss (aa, ee, bb, cc, dd, aa)", "42X33" },
	{ "select aa from s ss (aa, bb, cc, dd, ee, AA)", "42X33" },
	{ "select aa from s ss (aa, bb, cc, dd, ee, ff), t", "42X03" },
	{ "insert into t select aa from s aa (aa, bb, cc, dd, ee, ff), s bb (aa, bb, cc, dd, ee, ff)", "42X03" },
	{ "select a from s ss (aa, bb, cc, dd, ee, ff)", "42X04" },
	// positive tests
	{ "select * from s ss (f, e, d, c, b, a) where f = 0", new String [][] {
		{"0","1","2","3","4","5"} } },
	{ "select a, aa from s a, s b (aa, bb, cc, dd, ee, ff)	where f = ff and aa = 10", new String [][] { {"10","10"} } },
	{ "select a.a, b.aa from s a, s b (aa, bb, cc, dd, ee, ff) where f = ff and b.aa = 10", new String [][] { {"10","10"} } },
	{ "insert into t select * from s ss (aa, bb, cc, dd, ee, ff)", null },
	{ "select * from t", new String [][] {
		{"0","1","2","3","4","5"},
		{"10","11","12","13","14","15"} } },
	{ "delete from t", null },
	{ "insert into t (aa,bb) select ff, aa from s ss (aa, bb, cc, dd, ee, ff)", null },
	{ "select * from t", new String[][] { {"5","0",null,null,null,null}, {"15","10",null,null,null,null} } },
	{ "delete from t", null },
	// negative tests
	{ "select * from (select * from s)", "42X01" },
	{ "select aa from (select * from s) ss (aa)", "42X32" },
	{ "select aa from (select * from s) ss (aa, bb, cc, dd, ee, ff, gg)", "42X32" },
	{ "select aa from (select * from s) ss (aa, ee, bb, cc, dd, aa)", "42X33" },
	{ "select aa from (select * from s) ss (aa, bb, cc, dd, ee, AA)", "42X33" },
	{ "select aa from (select * from s) ss (aa, bb, cc, dd, ee, ff), t", "42X03" },
	{ "insert into t select aa from (select * from s) aa (aa, bb, cc, dd, ee, ff), 	 (select * from s) bb (aa, bb, cc, dd, ee, ff)", "42X03" },
	{ "select a from (select * from s) ss (aa, bb, cc, dd, ee, ff)", "42X04" },
	{ "select a from (select * from s a, s b) ss", "42Y34" },
	// positive tests
	{ "select a from (select a from s) a", new String [][] { {"0"},{"10"} } },
	{ "select * from (select * from s) a", new String [][] {
		{"0","1","2","3","4","5"},
		{"10","11","12","13","14","15"} } },
	{ "select * from (select a, b, c, d, e, f from s) a", new String [][] {
		{"0","1","2","3","4","5"},
		{"10","11","12","13","14","15"} } },
	{ "select * from (select a, b, c from s) a", new String[][] { {"0","1","2"},{"10","11","12"} } },
	{ "select a, b, c, d, e, f from (select * from s) a", new String [][] {
		{"0","1","2","3","4","5"},
		{"10","11","12","13","14","15"} } },
	{ "insert into t (aa) select a from (select a from s) a", null },
	{ "select * from t", new String[][] { {"0",null,null,null,null,null},{"10",null,null,null,null,null} } },
	{ "delete from t", null },
	{ "insert into t select * from (select * from s) a", null },
	{ "select * from t", new String [][] {
		{"0","1","2","3","4","5"},
		{"10","11","12","13","14","15"} } },
	{ "delete from t", null },
	{ "insert into t select * from (select a, b, c, d, e, f from s) a", null },
	{ "select * from t", new String [][] {
		{"0","1","2","3","4","5"},
		{"10","11","12","13","14","15"} } },
	{ "delete from t", null },
	{ "insert into t (aa, bb, cc) select * from (select a, b, c from s) a", null },
	{ "select * from t", new String [][] {
		{"0","1","2",null,null,null}, {"10","11","12",null,null,null} } },
	{ "delete from t", null },
	{ "insert into t select a, b, c, d, e, f from (select * from s) a", null },
	{ "select * from t", new String [][] {
		{"0","1","2","3","4","5"},
		{"10","11","12","13","14","15"} } },
	{ "delete from t", null },
	{ "select a from (select a from s) a (a)", new String[][] { {"0"},{"10"} } },
	{ "select * from (select * from s) a (f, e, d, c, b, a)", new String[][] {
		{"0","1","2","3","4","5"},
		{"10","11","12","13","14","15"} } },
	{ "select * from (select a, b, c, d, e, f from s) a (f, e, d, c, b, a)", new String [][] {
		{"0","1","2","3","4","5"},
		{"10","11","12","13","14","15"} } },
	{ "select * from (select a, b, c from s) a (c, f, e)", new String [][] { {"0","1","2"},{"10","11","12"} } },
	{ "select a, b, c, d, e, f from (select * from s) a (a, b, c, d, e, f)", new String [][] {
		{"0","1","2","3","4","5"},
		{"10","11","12","13","14","15"} } },
	{ "insert into t (aa) select a from (select a from s) a (a)", null },
	{ "select * from t", new String[][] { {"0",null,null,null,null,null},{"10",null,null,null,null,null} } },
	{ "delete from t", null },
	{ "insert into t select * from (select * from s) a (c, b, a, e, f, d)", null },
	{ "select * from t", new String [][] {
		{"0","1","2","3","4","5"},
		{"10","11","12","13","14","15"} } },
	{ "delete from t", null },
	{ "insert into t select * from (select a, b, c, d, e, f from s) a (f, a, c, b, e, d)", null },
	{ "select * from t", new String [][] {
		{"0","1","2","3","4","5"},
		{"10","11","12","13","14","15"} } },
	{ "delete from t", null },
	{ "insert into t (aa, bb, cc) select * from (select a, b, c from s) a (f, e, a)", null },
	{ "select * from t", new String [][] {
		{"0","1","2",null,null,null},
		{"10","11","12",null,null,null} } },
	{ "delete from t", null },
	{ "select a, f from (select * from s) a (b, c, d, e, f, a)", new String [][] { {"5","4"},{"15","14"} } },
	{ "insert into t (aa, bb) select a, f from (select * from s) a (b, c, d, e, f, a)", null },
	{ "select * from t", new String [][] { {"5","4",null,null,null,null},{"15","14",null,null,null,null} } },
	{ "delete from t", null },
	{ "select * from (select * from s) a (a, b, c, d, e, f) where a = 0", new String [][] { {"0","1","2","3","4","5"} } },
	{ "select * from (select * from s) a (f, e, d, c, b, a) where f = 0", new String[][] { {"0","1","2","3","4","5"} } },
	{ "insert into t select * from (select * from s) a (a, b, c, d, e, f) where a = 0", null },
	{ "select * from t", new String [][] { {"0","1","2","3","4","5"} } },
	{ "delete from t", null },
	{ "insert into t select * from (select * from s) a (f, e, d, c, b, a) where f = 0", null },
	{ "select * from t", new String [][] { {"0","1","2","3","4","5"} } },
	{ "delete from t", null },
	{ "select * from (select a from s) a, (select a from s) b", new String [][] {
		{"0","0"},{"0","10"},{"10","0"},{"10","10"} } },
	{ "select * from (select a from s) a, (select a from s) b where a.a = b.a", new String [][] {
		{"0","0"},{"10","10"} } },
	{ "insert into t (aa, bb) select * from (select a from s) a, (select a from s) b where a.a = b.a", null },
	{ "select * from t", new String [][] { {"0","0",null,null,null,null},{"10","10",null,null,null,null} } },
	{ "delete from t", null },
	{ "select * from (select a.a, b.a from s a, s b) a (b, a) where b = a", new String[][] { {"0","0"},{"10","10"} } },
	{ "select * from (select a.a, b.a from s a, s b) a (b, a), (select a.a, b.a from s a, s b) b (b, a) where a.b = b.b", new String[][] {
		{"0","0","0","0"}, {"0","0","0","10"},
		{"0","10","0","0"}, {"0","10","0","10"},
		{"10","0","10","0"}, {"10","0","10","10"},
		{"10","10","10","0"},{"10","10","10","10"} } },
	{ "select * from (select (select 1 from s where 1 = 0), b.a from s a, s b) a (b, a),(select (select 1 from s where 1 = 0), b.a from s a, s b) b (b, a) where a.b = b.b", new String[0][0] },
	{ "insert into t (aa, bb) select * from (select a.a, b.a from s a, s b) a (b, a) where b = a", null },
	{ "select * from t", new String[][] { {"0","0",null,null,null,null},{"10","10",null,null,null,null} } },
	{ "delete from t", null },
	{ "select * from (select a.a, b.a from s a, s b) a (b, a) where b = a and a = 0 and b = 0", new String[][] { {"0","0"} } },
	{ "insert into t (aa, bb) select * from (select a.a, b.a from s a, s b) a (b, a) where b = a and a = 0 and b = 0", null },
	{ "select * from t", new String[][] { {"0","0",null,null,null,null} } },
	{ "delete from t", null },
	{ "select * from (select * from (select * from s) a ) a", new String[][] {
		{"0","1","2","3","4","5"},
		{"10","11","12","13","14","15"} } },
	{ "select * from (select * from (select * from 	(select * from (select * from (select * from (select * from (select * from (select * from	(select * from(select * from(select * from		(select * from	(select * from (select * from (select * from s) a) a	) a	) a	) a	) a	) a	) a	) a ) a ) a ) a	) a	) a	) a", new String[][] {
		{"0","1","2","3","4","5"},
		{"10","11","12","13","14","15"} } },
	{ "select * from (select a.a as a1, b.a as a2 from s a, s b) a where a.a1 = 0 and a.a2 = 10", new String[][] { {"0","10"} } },
	{ "select * from (select a, a from s) a (x, y) where x = y", new String[][] { {"0","0"},{"10","10"} } },
	{ "select * from (select a, a from s) a (x, y) where x + y = x * y", new String[][] { {"0","0"} } },
	{ "select * from (select 1 from s) a", new String[][] { {"1"},{"1"} } },
	{ "select * from (select 1 from s) a (x) where x = 1", new String[][] { {"1"},{"1"} } },
	{ "select * from (select 1 from s a, s b where a.a = b.a) a (x)", new String[][] { {"1"},{"1"} } },
	{ "select * from (select 1 from s a, s b where a.a = b.a) a (x) where x = 1", new String[][] { {"1"},{"1"} } },
	{ "select * from (select a + 1 from s) a", new String[][] { {"1"},{"11"} } },
	{ "select * from (select a + 1 from s) a (x) where x = 1", new String [][] { {"1"} } },
	{ "select * from (select a.a + 1 from s a, s b where a.a = b.a) a (x) where x = 1", new String [][] { {"1"} } },
	{ "create table tab1(tab1_c1 int, tab1_c2 int)", null },
	{ "create table tab2(tab2_c1 int, tab2_c2 int)", null },
	{ "insert into tab1 values (1, 1), (2, 2)", null },
	{ "insert into tab2 values (1, 1), (2, 2)", null },
	{ "select * from (select * from tab1, tab2) c where tab1_c1 in (1, 3)", new String[][] {
		{"1","1","1","1"},{"1","1","2","2"} } },
	{ "drop table tab1", null },
	{ "drop table tab2", null },
	{ "drop table s", null },
	{ "drop table t", null }
    };

    // Do not use partitioning as default, use replicate
    // (Some results are expected to be different with partitioning)
    skipDefaultPartitioned = true;

    Connection conn = TestUtil.getConnection();
    Statement stmt = conn.createStatement();
    // Go through the array, execute each string[0], check sqlstate [1]
    // This will fail on the first one that succeeds where it shouldn't
    // or throws unknown exception
    JDBC.SQLUnitTestHelper(stmt,Script_DerivedUT);
  }
  
  // This test is the script enhanced with partitioning
  public void testLangScript_DerivedWithPartitioning() throws Exception
  {
    // This form of the derived.sql test has partitioning clauses
    Object[][] Script_DerivedUTPartitioning = {
	// Derived table tests
	{ "create table s (a int, b int, c int, d int, e int, f int) partition by column(a)", null },
	{ "create table t (aa int, bb int, cc int, dd int, ee int, ff int) partition by column(aa)", null },
	{ "insert into s values (0,1,2,3,4,5)", null },
	{ "insert into s values (10,11,12,13,14,15)", null },
	// negative tests
	{ "select aa from s ss (aa)", "42X32" },
	{ "select aa from s ss (aa, bb, cc, dd, ee, ff, gg)", "42X32" },
	{ "select aa from s ss (aa, ee, bb, cc, dd, aa)", "42X33" },
	{ "select aa from s ss (aa, bb, cc, dd, ee, AA)", "42X33" },
	{ "select aa from s ss (aa, bb, cc, dd, ee, ff), t", "42X03" },
	{ "insert into t select aa from s aa (aa, bb, cc, dd, ee, ff), s bb (aa, bb, cc, dd, ee, ff)", "42X03" },
	{ "select a from s ss (aa, bb, cc, dd, ee, ff)", "42X04" },
	// positive tests
	{ "select * from s ss (f, e, d, c, b, a) where f = 0", new String [][] {
		{"0","1","2","3","4","5"} } },
	{ "select a, aa from s a, s b (aa, bb, cc, dd, ee, ff)	where f = ff and aa = 10", new String [][] { {"10","10"} } },
	{ "select a.a, b.aa from s a, s b (aa, bb, cc, dd, ee, ff) where f = ff and b.aa = 10", new String [][] { {"10","10"} } },
	{ "insert into t select * from s ss (aa, bb, cc, dd, ee, ff)", null },
	{ "select * from t", new String [][] {
		{"0","1","2","3","4","5"},
		{"10","11","12","13","14","15"} } },
	{ "delete from t", null },
	{ "insert into t (aa,bb) select ff, aa from s ss (aa, bb, cc, dd, ee, ff)", null },
	{ "select * from t", new String[][] { {"5","0",null,null,null,null}, {"15","10",null,null,null,null} } },
	{ "delete from t", null },
	// negative tests
	{ "select * from (select * from s)", "42X01" },
	{ "select aa from (select * from s) ss (aa)", "42X32" },
	{ "select aa from (select * from s) ss (aa, bb, cc, dd, ee, ff, gg)", "42X32" },
	{ "select aa from (select * from s) ss (aa, ee, bb, cc, dd, aa)", "42X33" },
	{ "select aa from (select * from s) ss (aa, bb, cc, dd, ee, AA)", "42X33" },
	{ "select aa from (select * from s) ss (aa, bb, cc, dd, ee, ff), t", "42X03" },
	{ "insert into t select aa from (select * from s) aa (aa, bb, cc, dd, ee, ff), 	 (select * from s) bb (aa, bb, cc, dd, ee, ff)", "42X03" },
	{ "select a from (select * from s) ss (aa, bb, cc, dd, ee, ff)", "42X04" },
	{ "select a from (select * from s a, s b) ss", "42Y34" },
	// positive tests
	{ "select a from (select a from s) a", new String [][] { {"0"},{"10"} } },
	{ "select * from (select * from s) a", new String [][] {
		{"0","1","2","3","4","5"},
		{"10","11","12","13","14","15"} } },
	{ "select * from (select a, b, c, d, e, f from s) a", new String [][] {
		{"0","1","2","3","4","5"},
		{"10","11","12","13","14","15"} } },
	{ "select * from (select a, b, c from s) a", new String[][] { {"0","1","2"},{"10","11","12"} } },
	{ "select a, b, c, d, e, f from (select * from s) a", new String [][] {
		{"0","1","2","3","4","5"},
		{"10","11","12","13","14","15"} } },
	{ "insert into t (aa) select a from (select a from s) a", null },
	{ "select * from t", new String[][] { {"0",null,null,null,null,null},{"10",null,null,null,null,null} } },
	{ "delete from t", null },
	{ "insert into t select * from (select * from s) a", null },
	{ "select * from t", new String [][] {
		{"0","1","2","3","4","5"},
		{"10","11","12","13","14","15"} } },
	{ "delete from t", null },
	{ "insert into t select * from (select a, b, c, d, e, f from s) a", null },
	{ "select * from t", new String [][] {
		{"0","1","2","3","4","5"},
		{"10","11","12","13","14","15"} } },
	{ "delete from t", null },
	{ "insert into t (aa, bb, cc) select * from (select a, b, c from s) a", null },
	{ "select * from t", new String [][] {
		{"0","1","2",null,null,null}, {"10","11","12",null,null,null} } },
	{ "delete from t", null },
	{ "insert into t select a, b, c, d, e, f from (select * from s) a", null },
	{ "select * from t", new String [][] {
		{"0","1","2","3","4","5"},
		{"10","11","12","13","14","15"} } },
	{ "delete from t", null },
	{ "select a from (select a from s) a (a)", new String[][] { {"0"},{"10"} } },
	{ "select * from (select * from s) a (f, e, d, c, b, a)", new String[][] {
		{"0","1","2","3","4","5"},
		{"10","11","12","13","14","15"} } },
	{ "select * from (select a, b, c, d, e, f from s) a (f, e, d, c, b, a)", new String [][] {
		{"0","1","2","3","4","5"},
		{"10","11","12","13","14","15"} } },
	{ "select * from (select a, b, c from s) a (c, f, e)", new String [][] { {"0","1","2"},{"10","11","12"} } },
	{ "select a, b, c, d, e, f from (select * from s) a (a, b, c, d, e, f)", new String [][] {
		{"0","1","2","3","4","5"},
		{"10","11","12","13","14","15"} } },
	{ "insert into t (aa) select a from (select a from s) a (a)", null },
	{ "select * from t", new String[][] { {"0",null,null,null,null,null},{"10",null,null,null,null,null} } },
	{ "delete from t", null },
	{ "insert into t select * from (select * from s) a (c, b, a, e, f, d)", null },
	{ "select * from t", new String [][] {
		{"0","1","2","3","4","5"},
		{"10","11","12","13","14","15"} } },
	{ "delete from t", null },
	{ "insert into t select * from (select a, b, c, d, e, f from s) a (f, a, c, b, e, d)", null },
	{ "select * from t", new String [][] {
		{"0","1","2","3","4","5"},
		{"10","11","12","13","14","15"} } },
	{ "delete from t", null },
	{ "insert into t (aa, bb, cc) select * from (select a, b, c from s) a (f, e, a)", null },
	{ "select * from t", new String [][] {
		{"0","1","2",null,null,null},
		{"10","11","12",null,null,null} } },
	{ "delete from t", null },
	{ "select a, f from (select * from s) a (b, c, d, e, f, a)", new String [][] { {"5","4"},{"15","14"} } },
	{ "insert into t (aa, bb) select a, f from (select * from s) a (b, c, d, e, f, a)", null },
	{ "select * from t", new String [][] { {"5","4",null,null,null,null},{"15","14",null,null,null,null} } },
	{ "delete from t", null },
	{ "select * from (select * from s) a (a, b, c, d, e, f) where a = 0", new String [][] { {"0","1","2","3","4","5"} } },
	{ "select * from (select * from s) a (f, e, d, c, b, a) where f = 0", new String[][] { {"0","1","2","3","4","5"} } },
	{ "insert into t select * from (select * from s) a (a, b, c, d, e, f) where a = 0", null },
	{ "select * from t", new String [][] { {"0","1","2","3","4","5"} } },
	{ "delete from t", null },
	{ "insert into t select * from (select * from s) a (f, e, d, c, b, a) where f = 0", null },
	{ "select * from t", new String [][] { {"0","1","2","3","4","5"} } },
	{ "delete from t", null },
	{ "select * from (select a from s) a, (select a from s) b", new String [][] {
		{"0","0"},{"0","10"},{"10","0"},{"10","10"} } },
	{ "select * from (select a from s) a, (select a from s) b where a.a = b.a", new String [][] {
		{"0","0"},{"10","10"} } },
	{ "insert into t (aa, bb) select * from (select a from s) a, (select a from s) b where a.a = b.a", null },
	{ "select * from t", new String [][] { {"0","0",null,null,null,null},{"10","10",null,null,null,null} } },
	{ "delete from t", null },
	{ "select * from (select a.a, b.a from s a, s b) a (b, a) where b = a", new String[][] { {"0","0"},{"10","10"} } },
	{ "select * from (select a.a, b.a from s a, s b) a (b, a), (select a.a, b.a from s a, s b) b (b, a) where a.b = b.b", new String[][] {
		{"0","0","0","0"}, {"0","0","0","10"},
		{"0","10","0","0"}, {"0","10","0","10"},
		{"10","0","10","0"}, {"10","0","10","10"},
		{"10","10","10","0"},{"10","10","10","10"} } },
	{ "select * from (select (select 1 from s where 1 = 0), b.a from s a, s b) a (b, a),(select (select 1 from s where 1 = 0), b.a from s a, s b) b (b, a) where a.b = b.b", new String[0][0] },
	{ "insert into t (aa, bb) select * from (select a.a, b.a from s a, s b) a (b, a) where b = a", null },
	{ "select * from t", new String[][] { {"0","0",null,null,null,null},{"10","10",null,null,null,null} } },
	{ "delete from t", null },
	{ "select * from (select a.a, b.a from s a, s b) a (b, a) where b = a and a = 0 and b = 0", new String[][] { {"0","0"} } },
	{ "insert into t (aa, bb) select * from (select a.a, b.a from s a, s b) a (b, a) where b = a and a = 0 and b = 0", null },
	{ "select * from t", new String[][] { {"0","0",null,null,null,null} } },
	{ "delete from t", null },
	{ "select * from (select * from (select * from s) a ) a", new String[][] {
		{"0","1","2","3","4","5"},
		{"10","11","12","13","14","15"} } },
	{ "select * from (select * from (select * from 	(select * from (select * from (select * from (select * from (select * from (select * from	(select * from(select * from(select * from		(select * from	(select * from (select * from (select * from s) a) a	) a	) a	) a	) a	) a	) a	) a ) a ) a ) a	) a	) a	) a", new String[][] {
		{"0","1","2","3","4","5"},
		{"10","11","12","13","14","15"} } },
	{ "select * from (select a.a as a1, b.a as a2 from s a, s b) a where a.a1 = 0 and a.a2 = 10", new String[][] { {"0","10"} } },
	{ "select * from (select a, a from s) a (x, y) where x = y", new String[][] { {"0","0"},{"10","10"} } },
	{ "select * from (select a, a from s) a (x, y) where x + y = x * y", new String[][] { {"0","0"} } },
	{ "select * from (select 1 from s) a", new String[][] { {"1"},{"1"} } },
	{ "select * from (select 1 from s) a (x) where x = 1", new String[][] { {"1"},{"1"} } },
	{ "select * from (select 1 from s a, s b where a.a = b.a) a (x)", new String[][] { {"1"},{"1"} } },
	{ "select * from (select 1 from s a, s b where a.a = b.a) a (x) where x = 1", new String[][] { {"1"},{"1"} } },
	{ "select * from (select a + 1 from s) a", new String[][] { {"1"},{"11"} } },
	{ "select * from (select a + 1 from s) a (x) where x = 1", new String [][] { {"1"} } },
	{ "select * from (select a.a + 1 from s a, s b where a.a = b.a) a (x) where x = 1", new String [][] { {"1"} } },
	{ "create table tab1(tab1_c1 int, tab1_c2 int) partition by column(tab1_c1)", null },
	{ "create table tab2(tab2_c1 int, tab2_c2 int) partition by column(tab2_c1)", null },
	{ "insert into tab1 values (1, 1), (2, 2)", null },
	{ "insert into tab2 values (1, 1), (2, 2)", null },
	{ "select * from (select * from tab1, tab2) c where tab1_c1 in (1, 3)", new String[][] {
		{"1","1","1","1"},{"1","1","2","2"} } },
	{ "drop table tab1", null },
	{ "drop table tab2", null },
	{ "drop table s", null },
	{ "drop table t", null }
    };

    Connection conn = TestUtil.getConnection();
    Statement stmt = conn.createStatement();
    // Go through the array, execute each string[0], check sqlstate [1]
    // This will fail on the first one that succeeds where it shouldn't
    // or throws unknown exception
    JDBC.SQLUnitTestHelper(stmt,Script_DerivedUTPartitioning);

  }
}
