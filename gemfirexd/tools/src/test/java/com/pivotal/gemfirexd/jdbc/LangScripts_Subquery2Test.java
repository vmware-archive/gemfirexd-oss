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

import java.sql.Connection;
import java.sql.Statement;
import org.apache.derbyTesting.junit.JDBC;

import com.pivotal.gemfirexd.TestUtil;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

public class LangScripts_Subquery2Test extends JdbcTestBase {

  public static void main(String[] args) {
    TestRunner.run(new TestSuite(LangScripts_Subquery2Test.class));
  }
  
  public LangScripts_Subquery2Test(String name) {
    super(name); 
  }

  @Override
  protected String reduceLogging() {
    // these tests generate lots of logs, so reducing them
    return "config";
  }

  // This test is the as-is LangScript conversion, without any partitioning clauses
  public void testLangScript_Subquery2TestNoPartitioning() throws Exception
  {
    // This is a JUnit conversion of the Derby Lang Subquery2.sql script
    // without any GemFireXD extensions
	  
    // Catch exceptions from illegal syntax
    // Tests still not fixed marked FIXME
	  
    // Array of SQL text to execute and sqlstates to expect
    // The first object is a String, the second is either 
    // 1) null - this means query returns no rows and throws no exceptions
    // 2) a string - this means query returns no rows and throws expected SQLSTATE
    // 3) a String array - this means query returns rows which must match (unordered) given resultset
    //       - for an empty result set, an uninitialized size [0][0] array is used
    Object[][] Script_Subquery2UT = {
	// subquery tests (ANY and ALL subqueries)
	{ "create table s (i int, s smallint, c char(30), vc char(30), b bigint)" + getOffHeapSuffix(), null },
     { "create table t (i int, s smallint, c char(30), vc char(30), b bigint)"+ getOffHeapSuffix(), null },
     { "create table tt (ii int, ss smallint, cc char(30), vcvc char(30), b bigint)"+ getOffHeapSuffix(), null },
     { "create table ttt (iii int, sss smallint, ccc char(30), vcvcvc char(30))"+ getOffHeapSuffix(), null },
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
     { "select * from s where s = ANY (select * from s)", "42X38" },
     { "select * from s where s >= ANY (select b from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"} } },       //Derby throws error, GemFireXD autoconverts
     { "select * from s where s * ANY (select c from t)", "42X01" },
     //Original
     //{ "select * from s where s = ANY (select ? from s)", "42X34" },
     { "select * from s where s = ANY (select ? from s)", "07000" },
     { "select * from s where 1 = ANY (select s from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
     { "select * from s where -1 = ANY (select i from t)", new String[0][0] },
     { "select * from s where '1' = ANY (select vc from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
     { "select * from s where 0 = ANY (select b from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
     { "select * from s where 1 <> ANY (select s from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
     { "select * from s where -1 <> ANY (select i from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
     { "select * from s where '1' <> ANY (select vc from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
     { "select * from s where 0 <> ANY (select b from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
     { "select * from s where 1 >= ANY (select s from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
     { "select * from s where -1 >= ANY (select i from t)", new String[0][0] },
     { "select * from s where '1' >= ANY (select vc from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
     { "select * from s where 0 >= ANY (select b from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
     { "select * from s where 1 > ANY (select s from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
     { "select * from s where -1 > ANY (select i from t)", new String[0][0] },
     { "select * from s where '1' > ANY (select vc from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
     { "select * from s where 0 > ANY (select b from t)", new String[0][0] },
     { "select * from s where 1 <= ANY (select s from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
     { "select * from s where -1 <= ANY (select i from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
     { "select * from s where '1' <= ANY (select vc from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
     { "select * from s where 0 <= ANY (select b from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
     { "select * from s where 1 < ANY (select s from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
     { "select * from s where -1 < ANY (select i from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
     { "select * from s where '1' < ANY (select vc from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
     { "select * from s where 0 < ANY (select b from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
	{ "select * from s where i = ANY (select 1 from t)", new String[][] { {"1","1","1","1","1"} } },
     { "select * from s where i = ANY (select -1 from t)", new String[0][0] },
     { "select * from s where c = ANY (select '1' from t)", new String[][] { {"1","1","1","1","1"} } },
     { "select * from s where b = ANY (select 1 from t)", new String[][] { {"1","1","1","1","1"} } },
     { "select * from s where i <> ANY (select 1 from t)", new String[][] { {"0","0","0","0","0"} } },
     { "select * from s where i <> ANY (select -1 from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"} } },
     { "select * from s where c <> ANY (select '1' from t)", new String[][] { {"0","0","0","0","0"} } },
     { "select * from s where b <> ANY (select 1 from t)", new String[][] { {"0","0","0","0","0"} } },
     { "select * from s where i >= ANY (select 1 from t)", new String[][] { {"1","1","1","1","1"} } },
     { "select * from s where i >= ANY (select -1 from t)", new String[][] {
			{"1","1","1","1","1"},{"0","0","0","0","0"} } },
     { "select * from s where c >= ANY (select '1' from t)", new String[][] { {"1","1","1","1","1"} } },
     { "select * from s where b >= ANY (select 1 from t)", new String[][] { {"1","1","1","1","1"} } },
     { "select * from s where i > ANY (select 1 from t)", new String[0][0] },
     { "select * from s where i > ANY (select -1 from t)", new String[][] {
			{"1","1","1","1","1"},{"0","0","0","0","0"} } },
     { "select * from s where c > ANY (select '1' from t)", new String[0][0] },
     { "select * from s where b > ANY (select 1 from t)", new String[0][0] },
     { "select * from s where i <= ANY (select 1 from t)", new String[][] {
			{"1","1","1","1","1"},{"0","0","0","0","0"} } },
     { "select * from s where i <= ANY (select -1 from t)", new String[0][0] },
     { "select * from s where c <= ANY (select '1' from t)", new String[][] {
			{"1","1","1","1","1"},{"0","0","0","0","0"} } },
     { "select * from s where b <= ANY (select 1 from t)", new String[][] {
			{"1","1","1","1","1"},{"0","0","0","0","0"} } },
     { "select * from s where i < ANY (select 1 from t)", new String[][] { {"0","0","0","0","0"} } },
     { "select * from s where i < ANY (select -1 from t)", new String[0][0] },
     { "select * from s where c < ANY (select '1' from t)", new String[][] { {"0","0","0","0","0"} } },
     { "select * from s where b < ANY (select 1 from t)", new String[][] { {"0","0","0","0","0"} } },
     { "select * from s where 1 = ANY (select 0 from t)", new String[0][0] },
     { "select * from s where 0 = ANY (select 0 from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
     { "select * from s where 1 <> ANY (select 0 from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
     { "select * from s where 0 <> ANY (select 0 from t)", new String[0][0] },
     { "select * from s where 1 >= ANY (select 0 from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
     { "select * from s where 0 >= ANY (select 0 from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
     { "select * from s where 1 > ANY (select 0 from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
     { "select * from s where 0 > ANY (select 0 from t)", new String[0][0] },
     { "select * from s where 1 <= ANY (select 0 from t)", new String[0][0] },
     { "select * from s where 0 <= ANY (select 0 from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
     { "select * from s where 1 < ANY (select 0 from t)", new String[0][0] },
     { "select * from s where 0 < ANY (select 0 from t)", new String[0][0] },
     { "select * from s where c = ANY (select vc from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"} } },
     { "select * from s where vc = ANY (select c from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"} } },
     { "select * from s where i = ANY (select s from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"} } },
     { "select * from s where s = ANY (select i from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"} } },
     { "select * from s where c <> ANY (select vc from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"} } },
     { "select * from s where vc <> ANY (select c from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"} } },
     { "select * from s where i <> ANY (select s from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"} } },
     { "select * from s where s <> ANY (select i from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"} } },
     { "select * from s where c >= ANY (select vc from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"} } },
     { "select * from s where vc >= ANY (select c from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"} } },
     { "select * from s where i >= ANY (select s from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"} } },
     { "select * from s where s >= ANY (select i from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"} } },
     { "select * from s where c > ANY (select vc from t)", new String[][] {
		{"1","1","1","1","1"} } },
     { "select * from s where vc > ANY (select c from t)", new String[][] {
		{"1","1","1","1","1"} } },
     { "select * from s where i > ANY (select s from t)", new String[][] {
		{"1","1","1","1","1"} } },
     { "select * from s where s > ANY (select i from t)", new String[][] {
		{"1","1","1","1","1"} } },
     { "select * from s where c <= ANY (select vc from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"} } },
     { "select * from s where vc <= ANY (select c from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"} } },
     { "select * from s where i <= ANY (select s from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"} } },
     { "select * from s where s <= ANY (select i from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"} } },
     { "select * from s where c < ANY (select vc from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"} } },
     { "select * from s where vc < ANY (select c from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"} } },
     { "select * from s where i < ANY (select s from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"} } },
     { "select * from s where s < ANY (select i from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"} } },
     { "select * from s where i = ANY (select i from t where 1 = 0)", new String[0][0] },
     { "select * from s where i <> ANY (select i from t where 1 = 0)", new String[0][0] },
     { "select * from s where i >= ANY (select i from t where 1 = 0)", new String[0][0] },
     { "select * from s where i > ANY (select i from t where 1 = 0)", new String[0][0] },
     { "select * from s where i <= ANY (select i from t where 1 = 0)", new String[0][0] },
     { "select * from s where i < ANY (select i from t where 1 = 0)", new String[0][0] },
     { "select i from s where i = -1 or i = ANY (select i from t)", new String[][] { {"1"},{"0"} } },
     { "select i from s where i = 0 or i = ANY (select i from t where i = -1)", new String[][] { {"0"} } },
     { "select i from s where i = -1 or i = ANY (select i from t where i = -1 or i = 1)", new String[][] { {"1"} } },
     { "select i from s where i = -1 or i <> ANY (select i from t)", new String [][] { {"1"},{"0"} } },
     { "select i from s where i = 0 or i >= ANY (select i from t where i = -1)", new String[][] { {"0"} } },
     { "select i from s where i = -1 or i < ANY (select i from t where i = -1 or i = 1)", new String[][] { {"0"} } },
     { "select i from s where i = -1 or i >= ANY (select i from t)", new String [][] { {"1"},{"0"} } },
     { "select i from s where i = 0 or i > ANY (select i from t where i = -1)", new String[][] { {"0"} } },
     { "select i from s where i = -1 or i <> ANY (select i from t where i = -1 or i = 1)", new String[][] { {"0"} } },
     { "select * from s where i > ANY (select i from t where s.s > t.s)", new String[][] {
		{"1","1","1","1","1"} } },
     { "select * from s where i >= ANY (select i from t where s.s >= t.s)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"} } },
     { "select * from s where i < ANY (select i from t where s.s < t.s)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"} } },
     { "select * from s where i <= ANY (select i from t where s.s <= t.s)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"} } },
     { "select * from s where i = ANY (select i from t where s.s = t.s)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"} } },
     { "select * from s where i <> ANY (select i from t where s.s <> t.s)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"} } },
     { "create table s_3rows (i int)"+ getOffHeapSuffix(), null },
     { "create table t_1 (i int)"+ getOffHeapSuffix(), null },
     { "create table u_null (i int)"+ getOffHeapSuffix(), null },
     { "create table v_empty (i int)"+ getOffHeapSuffix(), null },
     { "create table w_2 (i int)"+ getOffHeapSuffix(), null },
     { "insert into s_3rows values(NULL)", null },
     { "insert into s_3rows values(1)", null },
     { "insert into s_3rows values(2)", null },
     { "insert into u_null values(NULL)", null },
     { "insert into t_1 values(1)", null },
     { "insert into w_2 values(2)", null },
     { "select * from s_3rows where s_3rows.i not in (select i from t_1)", new String[][] { {"2"} } },
     { "select * from s_3rows where s_3rows.i <> ALL (select i from t_1)", new String[][] { {"2"} } },
     { "select * from s_3rows where s_3rows.i >= ALL (select i from t_1)", new String[][] { {"2"},{"1"} } },
     { "select * from s_3rows where s_3rows.i > ALL (select i from t_1)", new String[][] { {"2"} } },
     { "select * from s_3rows where s_3rows.i <= ALL (select i from t_1)", new String[][] { {"1"} } },
     { "select * from s_3rows where s_3rows.i < ALL (select i from t_1)", new String[0][0] },
     { "select * from s_3rows where s_3rows.i = ALL (select i from t_1)", new String[][] { {"1"} } },
     { "select * from s_3rows where s_3rows.i not in (select i from u_null)", new String[0][0] },
     { "select * from s_3rows where s_3rows.i <> ALL (select i from u_null)", new String[0][0] },
     { "select * from s_3rows where s_3rows.i >= ALL (select i from u_null)", new String[0][0] },
     { "select * from s_3rows where s_3rows.i > ALL (select i from u_null)", new String[0][0] },
     { "select * from s_3rows where s_3rows.i <= ALL (select i from u_null)", new String[0][0] },
     { "select * from s_3rows where s_3rows.i < ALL (select i from u_null)", new String[0][0] },
     { "select * from s_3rows where s_3rows.i = ALL (select i from u_null)", new String[0][0] },
     { "select * from s_3rows where s_3rows.i not in (select i from v_empty)", new String[][] { {"2"},{"1"},{null} } },
     { "select * from s_3rows where s_3rows.i <> ALL (select i from v_empty)", new String[][] { {"2"},{"1"},{null} } },
     { "select * from s_3rows where s_3rows.i >= ALL (select i from v_empty)", new String[][] { {"2"},{"1"},{null} } },
     { "select * from s_3rows where s_3rows.i > ALL (select i from v_empty)", new String[][] { {"2"},{"1"},{null} } },
     { "select * from s_3rows where s_3rows.i <= ALL (select i from v_empty)", new String[][] { {"2"},{"1"},{null} } },
     { "select * from s_3rows where s_3rows.i < ALL (select i from v_empty)", new String[][] { {"2"},{"1"},{null} } },
     { "select * from s_3rows where s_3rows.i = ALL (select i from v_empty)", new String[][] { {"2"},{"1"},{null} } },
     { "select * from s_3rows where s_3rows.i not in (select i from w_2)", new String[][] { {"1"} } },
     { "select * from s_3rows where s_3rows.i <> ALL (select i from w_2)", new String[][] { {"1"} } },
     { "select * from s_3rows where s_3rows.i >= ALL (select i from w_2)", new String[][] { {"2"} } },
     { "select * from s_3rows where s_3rows.i > ALL (select i from w_2)", new String[0][0] },
     { "select * from s_3rows where s_3rows.i <= ALL (select i from w_2)", new String[][] { {"2"},{"1"} } },
     { "select * from s_3rows where s_3rows.i < ALL (select i from w_2)", new String[][] { {"1"} } },
     { "select * from s_3rows where s_3rows.i = ALL (select i from w_2)", new String[][] { {"2"} } },
     { "select * from w_2 where w_2.i = ALL (select i from w_2)", new String[][] { {"2"} } },
     { "select * from s_3rows where NOT s_3rows.i = ANY (select i from w_2)", new String[][] { {"1"} } },
     { "select * from s_3rows where s_3rows.i <> ALL (select i from w_2)", new String[][] { {"1"} } },
     { "select * from s_3rows where NOT s_3rows.i = ANY (select i from v_empty)", new String[][] { {"2"},{"1"},{null} } },
     { "select * from s_3rows where s_3rows.i <> ALL (select i from v_empty)", new String[][] { {"2"},{"1"},{null} } },
     { "select * from s_3rows where NOT s_3rows.i <> ANY (select i from w_2)", new String[][] { {"2"} } },
     { "select * from s_3rows where s_3rows.i = ALL (select i from w_2)", new String[][] { {"2"} } },
     { "select * from s_3rows where NOT s_3rows.i <> ANY (select i from v_empty)", new String[][] { {"2"},{"1"},{null} } },
     { "select * from s_3rows where s_3rows.i = ALL (select i from v_empty)", new String[][] { {"2"},{"1"},{null} } },
     { "select * from s_3rows where NOT s_3rows.i >= ANY (select i from w_2)", new String[][] { {"1"} } },
     { "select * from s_3rows where s_3rows.i < ALL (select i from w_2)", new String[][] { {"1"} } },
     { "select * from s_3rows where NOT s_3rows.i >= ANY (select i from v_empty)", new String[][] { {"2"},{"1"},{null} } },
     { "select * from s_3rows where s_3rows.i < ALL (select i from v_empty)", new String[][] { {"2"},{"1"},{null} } },
     { "select * from s_3rows where NOT s_3rows.i > ANY (select i from w_2)", new String[][] { {"2"},{"1"} } },
     { "select * from s_3rows where s_3rows.i <= ALL (select i from w_2)", new String[][] { {"2"},{"1"} } },
     { "select * from s_3rows where NOT s_3rows.i > ANY (select i from v_empty)", new String[][] { {"2"},{"1"},{null} } },
     { "select * from s_3rows where s_3rows.i <= ALL (select i from v_empty)", new String[][] { {"2"},{"1"},{null} } },
     { "select * from s_3rows where NOT s_3rows.i <= ANY (select i from w_2)", new String[0][0] },
     { "select * from s_3rows where s_3rows.i > ALL (select i from w_2)", new String[0][0] },
     { "select * from s_3rows where NOT s_3rows.i <= ANY (select i from v_empty)", new String[][] { {"2"},{"1"},{null} } },
     { "select * from s_3rows where s_3rows.i > ALL (select i from v_empty)", new String[][] { {"2"},{"1"},{null} } },
     { "select * from s_3rows where NOT s_3rows.i < ANY (select i from w_2)", new String[][] { {"2"} } },
     { "select * from s_3rows where s_3rows.i >= ALL (select i from w_2)", new String[][] { {"2"} } },
     { "select * from s_3rows where NOT s_3rows.i < ANY (select i from v_empty)", new String[][] { {"2"},{"1"},{null} } },
     { "select * from s_3rows where s_3rows.i >= ALL (select i from v_empty)", new String[][] { {"2"},{"1"},{null} } },
     { "select * from s_3rows where NOT s_3rows.i = ALL (select i from w_2)", new String[][] { {"1"} } },
     { "select * from s_3rows where s_3rows.i <> ANY (select i from w_2)", new String[][] { {"1"} } },
     { "select * from s_3rows where NOT s_3rows.i = ALL (select i from v_empty)", new String[0][0] },
     { "select * from s_3rows where s_3rows.i <> ANY (select i from v_empty)", new String[0][0] },
     { "select * from s_3rows where NOT s_3rows.i <> ALL (select i from w_2)", new String[][] { {"2"} } },
     { "select * from s_3rows where s_3rows.i = ANY (select i from w_2)", new String[][] { {"2"} } },
     { "select * from s_3rows where NOT s_3rows.i <> ALL (select i from v_empty)", new String[0][0] },
     { "select * from s_3rows where s_3rows.i = ANY (select i from v_empty)", new String[0][0] },
     { "select * from s_3rows where NOT s_3rows.i >= ALL (select i from w_2)", new String[][] { {"1"} } },
     { "select * from s_3rows where s_3rows.i < ANY (select i from w_2)", new String[][] { {"1"} } },
     { "select * from s_3rows where NOT s_3rows.i >= ALL (select i from v_empty)", new String[0][0] },
     { "select * from s_3rows where s_3rows.i < ANY (select i from v_empty)", new String[0][0] },
     { "select * from s_3rows where NOT s_3rows.i > ALL (select i from w_2)", new String[][] { {"2"},{"1"} } },
     { "select * from s_3rows where s_3rows.i <= ANY (select i from w_2)", new String[][] { {"2"},{"1"} } },
     { "select * from s_3rows where NOT s_3rows.i > ALL (select i from v_empty)", new String[0][0] },
     { "select * from s_3rows where s_3rows.i <= ANY (select i from v_empty)", new String[0][0] },
     { "select * from s_3rows where NOT s_3rows.i <= ALL (select i from w_2)", new String[0][0] },
     { "select * from s_3rows where s_3rows.i > ANY (select i from w_2)", new String[0][0] },
     { "select * from s_3rows where NOT s_3rows.i <= ALL (select i from v_empty)", new String[0][0] },
     { "select * from s_3rows where s_3rows.i > ANY (select i from v_empty)", new String[0][0] },
     { "select * from s_3rows where NOT s_3rows.i < ALL (select i from w_2)", new String[][] { {"2"} } },
     { "select * from s_3rows where s_3rows.i >= ANY (select i from w_2)", new String[][] { {"2"} } },
     { "select * from s_3rows where NOT s_3rows.i < ALL (select i from v_empty)", new String[0][0] },
     { "select * from s_3rows where s_3rows.i >= ANY (select i from v_empty)", new String[0][0] },
     { "create table t1 (c1 int not null, c2 int)"+ getOffHeapSuffix(), null },
     { "create table t2 (c1 int not null, c2 int)"+ getOffHeapSuffix(), null },
     { "insert into t1 values(1, 2)", null },
     { "insert into t2 values(0, 3)", null },
     { "select * from t1 where c1 not in (select c2 from t2)", new String[][] { {"1","2"} } },
     { "select * from t1 where c2 not in (select c1 from t2)", new String[][] { {"1","2"} } },
     { "select * from t1 where c1 not in (select c1 from t2)", new String[][] { {"1","2"} } },
     { "drop table t1", null },
     { "drop table t2", null },
     { "create table u (i int, s smallint, c char(30), vc char(30), b bigint)"+ getOffHeapSuffix(), null },
     { "insert into u select * from s", null },
	{ "update u set b = exists (select * from t) where vc < ANY (select vc from s)", "42X01" },
     { "delete from u where c < ANY (select c from t)", null },
     { "select * from u", new String[][] { {null,null,null,null,null} } },
	{ "drop table s", null },
     { "drop table t", null },
     { "drop table tt", null },
     { "drop table ttt", null },
     { "drop table u", null },
     { "drop table s_3rows", null },
     { "drop table t_1", null },
     { "drop table u_null", null },
     { "drop table v_empty", null },
     { "drop table w_2", null },
	{ "create table parentT ( i int, j int, k int)"+ getOffHeapSuffix(), null },
     { "create table childT ( i int, j int, k int)"+ getOffHeapSuffix(), null },
	{ "insert into parentT values (1,1,1), (2,2,2), (3,3,3), (4,4,4)", null },
     { "insert into parentT select i+4, j+4, k+4 from parentT", null },
     { "insert into parentT select i+8, j+8, k+8 from parentT", null },
     { "insert into parentT select i+16, j+16, k+16 from parentT", null },
     { "insert into parentT select i+32, j+32, k+32 from parentT", null },
     { "insert into parentT select i+64, j+64, k+64 from parentT", null },
     { "insert into parentT select i+128, j+128, k+128 from parentT", null },
     { "insert into parentT select i+256, j+256, k+256 from parentT", null },
     { "insert into parentT select i+512, j+512, k+512 from parentT", null },
     { "insert into parentT select i+1024, j+1024, k+1024 from parentT", null },
     { "insert into parentT select i+2048, j+2048, k+2048 from parentT", null },
     { "insert into parentT select i+4096, j+4096, k+4096 from parentT", null },
     { "insert into parentT select i+8192, j+8192, k+8192 from parentT", null },
	{ "update parentT set j = j /10", null },
     { "update parentT set k = k /100", null },
     { "create unique index parentIdx on parentT(i)", null },
     { "insert into childT select * from parentT", null },
     { "select count(*) from parentT where i < 10 and i not in (select i from childT)", new String[][] { {"0"} } },
     { "select count(*) from parentT where i< 10 and exists (select i from childT where childT.i=parentT.i)", new String[][] { {"9"} } },
     { "select count(*) from parentT where i< 10 and j not in (select distinct j from childT)", new String[][] { {"0"} } },
     { "select count(*) from parentT where i< 10 and exists (select distinct j from childT where childT.j=parentT.j)", new String[][] { {"9"} } },
     { "select count(*) from parentT where i< 10 and k not in (select distinct k from childT)", new String[][] { {"0"} } },
     { "select count(*) from parentT where i< 10 and exists (select distinct k from childT where childT.k=parentT.k)", new String[][] { {"9"} } },
     { "drop table childT", null },
     { "drop table parentT", null }
   };

    // Do not use partitioning as default, use replicate
    // (Some results are expected to be different with partitioning)
    skipDefaultPartitioned = true;

    Connection conn = TestUtil.getConnection();
    Statement stmt = conn.createStatement();
    // Go through the array, execute each string[0], check sqlstate [1]
    // This will fail on the first one that succeeds where it shouldn't
    // or throws unknown exception
    JDBC.SQLUnitTestHelper(stmt,Script_Subquery2UT);
  }
  
  // This test is the script enhanced with partitioning
  public void testLangScript_Subquery2WithPartitioning() throws Exception
  {
    // This form of the Subquery.sql test has partitioning activated
    // TODO : Create specialized partitioning for these tables for Subquery
    //  Currently, default partitioning is active
    Object[][] Script_Subquery2UTPartitioning = {
	// subquery tests (ANY and ALL subqueries)
	{ "create table s (i int, s smallint, c char(30), vc char(30), b bigint)"+ getOffHeapSuffix(), null },
     { "create table t (i int, s smallint, c char(30), vc char(30), b bigint)"+ getOffHeapSuffix(), null },
     { "create table tt (ii int, ss smallint, cc char(30), vcvc char(30), b bigint)"+ getOffHeapSuffix(), null },
     { "create table ttt (iii int, sss smallint, ccc char(30), vcvcvc char(30))"+ getOffHeapSuffix(), null },
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
     { "select * from s where s = ANY (select * from s)", "42X38" },
     { "select * from s where s >= ANY (select b from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"} } },       //Derby throws error, GemFireXD autoconverts
     { "select * from s where s * ANY (select c from t)", "42X01" },
     //Original
     //{ "select * from s where s = ANY (select ? from s)", "42X34" },
     { "select * from s where s = ANY (select ? from s)", "07000" },
     { "select * from s where 1 = ANY (select s from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
     { "select * from s where -1 = ANY (select i from t)", new String[0][0] },
     { "select * from s where '1' = ANY (select vc from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
     { "select * from s where 0 = ANY (select b from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
     { "select * from s where 1 <> ANY (select s from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
     { "select * from s where -1 <> ANY (select i from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
     { "select * from s where '1' <> ANY (select vc from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
     { "select * from s where 0 <> ANY (select b from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
     { "select * from s where 1 >= ANY (select s from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
     { "select * from s where -1 >= ANY (select i from t)", new String[0][0] },
     { "select * from s where '1' >= ANY (select vc from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
     { "select * from s where 0 >= ANY (select b from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
     { "select * from s where 1 > ANY (select s from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
     { "select * from s where -1 > ANY (select i from t)", new String[0][0] },
     { "select * from s where '1' > ANY (select vc from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
     { "select * from s where 0 > ANY (select b from t)", new String[0][0] },
     { "select * from s where 1 <= ANY (select s from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
     { "select * from s where -1 <= ANY (select i from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
     { "select * from s where '1' <= ANY (select vc from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
     { "select * from s where 0 <= ANY (select b from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
     { "select * from s where 1 < ANY (select s from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
     { "select * from s where -1 < ANY (select i from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
     { "select * from s where '1' < ANY (select vc from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
     { "select * from s where 0 < ANY (select b from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
	{ "select * from s where i = ANY (select 1 from t)", new String[][] { {"1","1","1","1","1"} } },
     { "select * from s where i = ANY (select -1 from t)", new String[0][0] },
     { "select * from s where c = ANY (select '1' from t)", new String[][] { {"1","1","1","1","1"} } },
     { "select * from s where b = ANY (select 1 from t)", new String[][] { {"1","1","1","1","1"} } },
     { "select * from s where i <> ANY (select 1 from t)", new String[][] { {"0","0","0","0","0"} } },
     { "select * from s where i <> ANY (select -1 from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"} } },
     { "select * from s where c <> ANY (select '1' from t)", new String[][] { {"0","0","0","0","0"} } },
     { "select * from s where b <> ANY (select 1 from t)", new String[][] { {"0","0","0","0","0"} } },
     { "select * from s where i >= ANY (select 1 from t)", new String[][] { {"1","1","1","1","1"} } },
     { "select * from s where i >= ANY (select -1 from t)", new String[][] {
			{"1","1","1","1","1"},{"0","0","0","0","0"} } },
     { "select * from s where c >= ANY (select '1' from t)", new String[][] { {"1","1","1","1","1"} } },
     { "select * from s where b >= ANY (select 1 from t)", new String[][] { {"1","1","1","1","1"} } },
     { "select * from s where i > ANY (select 1 from t)", new String[0][0] },
     { "select * from s where i > ANY (select -1 from t)", new String[][] {
			{"1","1","1","1","1"},{"0","0","0","0","0"} } },
     { "select * from s where c > ANY (select '1' from t)", new String[0][0] },
     { "select * from s where b > ANY (select 1 from t)", new String[0][0] },
     { "select * from s where i <= ANY (select 1 from t)", new String[][] {
			{"1","1","1","1","1"},{"0","0","0","0","0"} } },
     { "select * from s where i <= ANY (select -1 from t)", new String[0][0] },
     { "select * from s where c <= ANY (select '1' from t)", new String[][] {
			{"1","1","1","1","1"},{"0","0","0","0","0"} } },
     { "select * from s where b <= ANY (select 1 from t)", new String[][] {
			{"1","1","1","1","1"},{"0","0","0","0","0"} } },
     { "select * from s where i < ANY (select 1 from t)", new String[][] { {"0","0","0","0","0"} } },
     { "select * from s where i < ANY (select -1 from t)", new String[0][0] },
     { "select * from s where c < ANY (select '1' from t)", new String[][] { {"0","0","0","0","0"} } },
     { "select * from s where b < ANY (select 1 from t)", new String[][] { {"0","0","0","0","0"} } },
     { "select * from s where 1 = ANY (select 0 from t)", new String[0][0] },
     { "select * from s where 0 = ANY (select 0 from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
     { "select * from s where 1 <> ANY (select 0 from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
     { "select * from s where 0 <> ANY (select 0 from t)", new String[0][0] },
     { "select * from s where 1 >= ANY (select 0 from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
     { "select * from s where 0 >= ANY (select 0 from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
     { "select * from s where 1 > ANY (select 0 from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
     { "select * from s where 0 > ANY (select 0 from t)", new String[0][0] },
     { "select * from s where 1 <= ANY (select 0 from t)", new String[0][0] },
     { "select * from s where 0 <= ANY (select 0 from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"},{null,null,null,null,null} } },
     { "select * from s where 1 < ANY (select 0 from t)", new String[0][0] },
     { "select * from s where 0 < ANY (select 0 from t)", new String[0][0] },
     { "select * from s where c = ANY (select vc from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"} } },
     { "select * from s where vc = ANY (select c from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"} } },
     { "select * from s where i = ANY (select s from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"} } },
     { "select * from s where s = ANY (select i from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"} } },
     { "select * from s where c <> ANY (select vc from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"} } },
     { "select * from s where vc <> ANY (select c from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"} } },
     { "select * from s where i <> ANY (select s from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"} } },
     { "select * from s where s <> ANY (select i from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"} } },
     { "select * from s where c >= ANY (select vc from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"} } },
     { "select * from s where vc >= ANY (select c from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"} } },
     { "select * from s where i >= ANY (select s from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"} } },
     { "select * from s where s >= ANY (select i from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"} } },
     { "select * from s where c > ANY (select vc from t)", new String[][] {
		{"1","1","1","1","1"} } },
     { "select * from s where vc > ANY (select c from t)", new String[][] {
		{"1","1","1","1","1"} } },
     { "select * from s where i > ANY (select s from t)", new String[][] {
		{"1","1","1","1","1"} } },
     { "select * from s where s > ANY (select i from t)", new String[][] {
		{"1","1","1","1","1"} } },
     { "select * from s where c <= ANY (select vc from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"} } },
     { "select * from s where vc <= ANY (select c from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"} } },
     { "select * from s where i <= ANY (select s from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"} } },
     { "select * from s where s <= ANY (select i from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"} } },
     { "select * from s where c < ANY (select vc from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"} } },
     { "select * from s where vc < ANY (select c from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"} } },
     { "select * from s where i < ANY (select s from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"} } },
     { "select * from s where s < ANY (select i from t)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"} } },
     { "select * from s where i = ANY (select i from t where 1 = 0)", new String[0][0] },
     { "select * from s where i <> ANY (select i from t where 1 = 0)", new String[0][0] },
     { "select * from s where i >= ANY (select i from t where 1 = 0)", new String[0][0] },
     { "select * from s where i > ANY (select i from t where 1 = 0)", new String[0][0] },
     { "select * from s where i <= ANY (select i from t where 1 = 0)", new String[0][0] },
     { "select * from s where i < ANY (select i from t where 1 = 0)", new String[0][0] },
     { "select i from s where i = -1 or i = ANY (select i from t)", new String[][] { {"1"},{"0"} } },
     { "select i from s where i = 0 or i = ANY (select i from t where i = -1)", new String[][] { {"0"} } },
     { "select i from s where i = -1 or i = ANY (select i from t where i = -1 or i = 1)", new String[][] { {"1"} } },
     { "select i from s where i = -1 or i <> ANY (select i from t)", new String [][] { {"1"},{"0"} } },
     { "select i from s where i = 0 or i >= ANY (select i from t where i = -1)", new String[][] { {"0"} } },
     { "select i from s where i = -1 or i < ANY (select i from t where i = -1 or i = 1)", new String[][] { {"0"} } },
     { "select i from s where i = -1 or i >= ANY (select i from t)", new String [][] { {"1"},{"0"} } },
     { "select i from s where i = 0 or i > ANY (select i from t where i = -1)", new String[][] { {"0"} } },
     { "select i from s where i = -1 or i <> ANY (select i from t where i = -1 or i = 1)", new String[][] { {"0"} } },
     { "select * from s where i > ANY (select i from t where s.s > t.s)", new String[][] {
		{"1","1","1","1","1"} } },
     { "select * from s where i >= ANY (select i from t where s.s >= t.s)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"} } },
     { "select * from s where i < ANY (select i from t where s.s < t.s)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"} } },
     { "select * from s where i <= ANY (select i from t where s.s <= t.s)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"} } },
     { "select * from s where i = ANY (select i from t where s.s = t.s)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"} } },
     { "select * from s where i <> ANY (select i from t where s.s <> t.s)", new String[][] {
		{"1","1","1","1","1"},{"0","0","0","0","0"} } },
     { "create table s_3rows (i int)"+ getOffHeapSuffix(), null },
     { "create table t_1 (i int)"+ getOffHeapSuffix(), null },
     { "create table u_null (i int)"+ getOffHeapSuffix(), null },
     { "create table v_empty (i int)"+ getOffHeapSuffix(), null },
     { "create table w_2 (i int)"+ getOffHeapSuffix(), null },
     { "insert into s_3rows values(NULL)", null },
     { "insert into s_3rows values(1)", null },
     { "insert into s_3rows values(2)", null },
     { "insert into u_null values(NULL)", null },
     { "insert into t_1 values(1)", null },
     { "insert into w_2 values(2)", null },
     { "select * from s_3rows where s_3rows.i not in (select i from t_1)", new String[][] { {"2"} } },
     { "select * from s_3rows where s_3rows.i <> ALL (select i from t_1)", new String[][] { {"2"} } },
     { "select * from s_3rows where s_3rows.i >= ALL (select i from t_1)", new String[][] { {"2"},{"1"} } },
     { "select * from s_3rows where s_3rows.i > ALL (select i from t_1)", new String[][] { {"2"} } },
     { "select * from s_3rows where s_3rows.i <= ALL (select i from t_1)", new String[][] { {"1"} } },
     { "select * from s_3rows where s_3rows.i < ALL (select i from t_1)", new String[0][0] },
     { "select * from s_3rows where s_3rows.i = ALL (select i from t_1)", new String[][] { {"1"} } },
     { "select * from s_3rows where s_3rows.i not in (select i from u_null)", new String[0][0] },
     { "select * from s_3rows where s_3rows.i <> ALL (select i from u_null)", new String[0][0] },
     { "select * from s_3rows where s_3rows.i >= ALL (select i from u_null)", new String[0][0] },
     { "select * from s_3rows where s_3rows.i > ALL (select i from u_null)", new String[0][0] },
     { "select * from s_3rows where s_3rows.i <= ALL (select i from u_null)", new String[0][0] },
     { "select * from s_3rows where s_3rows.i < ALL (select i from u_null)", new String[0][0] },
     { "select * from s_3rows where s_3rows.i = ALL (select i from u_null)", new String[0][0] },
     { "select * from s_3rows where s_3rows.i not in (select i from v_empty)", new String[][] { {"2"},{"1"},{null} } },
     { "select * from s_3rows where s_3rows.i <> ALL (select i from v_empty)", new String[][] { {"2"},{"1"},{null} } },
     { "select * from s_3rows where s_3rows.i >= ALL (select i from v_empty)", new String[][] { {"2"},{"1"},{null} } },
     { "select * from s_3rows where s_3rows.i > ALL (select i from v_empty)", new String[][] { {"2"},{"1"},{null} } },
     { "select * from s_3rows where s_3rows.i <= ALL (select i from v_empty)", new String[][] { {"2"},{"1"},{null} } },
     { "select * from s_3rows where s_3rows.i < ALL (select i from v_empty)", new String[][] { {"2"},{"1"},{null} } },
     { "select * from s_3rows where s_3rows.i = ALL (select i from v_empty)", new String[][] { {"2"},{"1"},{null} } },
     { "select * from s_3rows where s_3rows.i not in (select i from w_2)", new String[][] { {"1"} } },
     { "select * from s_3rows where s_3rows.i <> ALL (select i from w_2)", new String[][] { {"1"} } },
     { "select * from s_3rows where s_3rows.i >= ALL (select i from w_2)", new String[][] { {"2"} } },
     { "select * from s_3rows where s_3rows.i > ALL (select i from w_2)", new String[0][0] },
     { "select * from s_3rows where s_3rows.i <= ALL (select i from w_2)", new String[][] { {"2"},{"1"} } },
     { "select * from s_3rows where s_3rows.i < ALL (select i from w_2)", new String[][] { {"1"} } },
     { "select * from s_3rows where s_3rows.i = ALL (select i from w_2)", new String[][] { {"2"} } },
     { "select * from w_2 where w_2.i = ALL (select i from w_2)", new String[][] { {"2"} } },
     { "select * from s_3rows where NOT s_3rows.i = ANY (select i from w_2)", new String[][] { {"1"} } },
     { "select * from s_3rows where s_3rows.i <> ALL (select i from w_2)", new String[][] { {"1"} } },
     { "select * from s_3rows where NOT s_3rows.i = ANY (select i from v_empty)", new String[][] { {"2"},{"1"},{null} } },
     { "select * from s_3rows where s_3rows.i <> ALL (select i from v_empty)", new String[][] { {"2"},{"1"},{null} } },
     { "select * from s_3rows where NOT s_3rows.i <> ANY (select i from w_2)", new String[][] { {"2"} } },
     { "select * from s_3rows where s_3rows.i = ALL (select i from w_2)", new String[][] { {"2"} } },
     { "select * from s_3rows where NOT s_3rows.i <> ANY (select i from v_empty)", new String[][] { {"2"},{"1"},{null} } },
     { "select * from s_3rows where s_3rows.i = ALL (select i from v_empty)", new String[][] { {"2"},{"1"},{null} } },
     { "select * from s_3rows where NOT s_3rows.i >= ANY (select i from w_2)", new String[][] { {"1"} } },
     { "select * from s_3rows where s_3rows.i < ALL (select i from w_2)", new String[][] { {"1"} } },
     { "select * from s_3rows where NOT s_3rows.i >= ANY (select i from v_empty)", new String[][] { {"2"},{"1"},{null} } },
     { "select * from s_3rows where s_3rows.i < ALL (select i from v_empty)", new String[][] { {"2"},{"1"},{null} } },
     { "select * from s_3rows where NOT s_3rows.i > ANY (select i from w_2)", new String[][] { {"2"},{"1"} } },
     { "select * from s_3rows where s_3rows.i <= ALL (select i from w_2)", new String[][] { {"2"},{"1"} } },
     { "select * from s_3rows where NOT s_3rows.i > ANY (select i from v_empty)", new String[][] { {"2"},{"1"},{null} } },
     { "select * from s_3rows where s_3rows.i <= ALL (select i from v_empty)", new String[][] { {"2"},{"1"},{null} } },
     { "select * from s_3rows where NOT s_3rows.i <= ANY (select i from w_2)", new String[0][0] },
     { "select * from s_3rows where s_3rows.i > ALL (select i from w_2)", new String[0][0] },
     { "select * from s_3rows where NOT s_3rows.i <= ANY (select i from v_empty)", new String[][] { {"2"},{"1"},{null} } },
     { "select * from s_3rows where s_3rows.i > ALL (select i from v_empty)", new String[][] { {"2"},{"1"},{null} } },
     { "select * from s_3rows where NOT s_3rows.i < ANY (select i from w_2)", new String[][] { {"2"} } },
     { "select * from s_3rows where s_3rows.i >= ALL (select i from w_2)", new String[][] { {"2"} } },
     { "select * from s_3rows where NOT s_3rows.i < ANY (select i from v_empty)", new String[][] { {"2"},{"1"},{null} } },
     { "select * from s_3rows where s_3rows.i >= ALL (select i from v_empty)", new String[][] { {"2"},{"1"},{null} } },
     { "select * from s_3rows where NOT s_3rows.i = ALL (select i from w_2)", new String[][] { {"1"} } },
     { "select * from s_3rows where s_3rows.i <> ANY (select i from w_2)", new String[][] { {"1"} } },
     { "select * from s_3rows where NOT s_3rows.i = ALL (select i from v_empty)", new String[0][0] },
     { "select * from s_3rows where s_3rows.i <> ANY (select i from v_empty)", new String[0][0] },
     { "select * from s_3rows where NOT s_3rows.i <> ALL (select i from w_2)", new String[][] { {"2"} } },
     { "select * from s_3rows where s_3rows.i = ANY (select i from w_2)", new String[][] { {"2"} } },
     { "select * from s_3rows where NOT s_3rows.i <> ALL (select i from v_empty)", new String[0][0] },
     { "select * from s_3rows where s_3rows.i = ANY (select i from v_empty)", new String[0][0] },
     { "select * from s_3rows where NOT s_3rows.i >= ALL (select i from w_2)", new String[][] { {"1"} } },
     { "select * from s_3rows where s_3rows.i < ANY (select i from w_2)", new String[][] { {"1"} } },
     { "select * from s_3rows where NOT s_3rows.i >= ALL (select i from v_empty)", new String[0][0] },
     { "select * from s_3rows where s_3rows.i < ANY (select i from v_empty)", new String[0][0] },
     { "select * from s_3rows where NOT s_3rows.i > ALL (select i from w_2)", new String[][] { {"2"},{"1"} } },
     { "select * from s_3rows where s_3rows.i <= ANY (select i from w_2)", new String[][] { {"2"},{"1"} } },
     { "select * from s_3rows where NOT s_3rows.i > ALL (select i from v_empty)", new String[0][0] },
     { "select * from s_3rows where s_3rows.i <= ANY (select i from v_empty)", new String[0][0] },
     { "select * from s_3rows where NOT s_3rows.i <= ALL (select i from w_2)", new String[0][0] },
     { "select * from s_3rows where s_3rows.i > ANY (select i from w_2)", new String[0][0] },
     { "select * from s_3rows where NOT s_3rows.i <= ALL (select i from v_empty)", new String[0][0] },
     { "select * from s_3rows where s_3rows.i > ANY (select i from v_empty)", new String[0][0] },
     { "select * from s_3rows where NOT s_3rows.i < ALL (select i from w_2)", new String[][] { {"2"} } },
     { "select * from s_3rows where s_3rows.i >= ANY (select i from w_2)", new String[][] { {"2"} } },
     { "select * from s_3rows where NOT s_3rows.i < ALL (select i from v_empty)", new String[0][0] },
     { "select * from s_3rows where s_3rows.i >= ANY (select i from v_empty)", new String[0][0] },
     { "create table t1 (c1 int not null, c2 int)"+ getOffHeapSuffix(), null },
     { "create table t2 (c1 int not null, c2 int)"+ getOffHeapSuffix(), null },
     { "insert into t1 values(1, 2)", null },
     { "insert into t2 values(0, 3)", null },
     { "select * from t1 where c1 not in (select c2 from t2)", new String[][] { {"1","2"} } },
     { "select * from t1 where c2 not in (select c1 from t2)", new String[][] { {"1","2"} } },
     { "select * from t1 where c1 not in (select c1 from t2)", new String[][] { {"1","2"} } },
     { "drop table t1", null },
     { "drop table t2", null },
     { "create table u (i int, s smallint, c char(30), vc char(30), b bigint)"+ getOffHeapSuffix(), null },
     { "insert into u select * from s", null },
	{ "update u set b = exists (select * from t) where vc < ANY (select vc from s)", "42X01" },
     { "delete from u where c < ANY (select c from t)", null },
     { "select * from u", new String[][] { {null,null,null,null,null} } },
	{ "drop table s", null },
     { "drop table t", null },
     { "drop table tt", null },
     { "drop table ttt", null },
     { "drop table u", null },
     { "drop table s_3rows", null },
     { "drop table t_1", null },
     { "drop table u_null", null },
     { "drop table v_empty", null },
     { "drop table w_2", null },
	{ "create table parentT ( i int, j int, k int)"+ getOffHeapSuffix(), null },
     { "create table childT ( i int, j int, k int)"+ getOffHeapSuffix(), null },
	{ "insert into parentT values (1,1,1), (2,2,2), (3,3,3), (4,4,4)", null },
     { "insert into parentT select i+4, j+4, k+4 from parentT", null },
     { "insert into parentT select i+8, j+8, k+8 from parentT", null },
     { "insert into parentT select i+16, j+16, k+16 from parentT", null },
     { "insert into parentT select i+32, j+32, k+32 from parentT", null },
     { "insert into parentT select i+64, j+64, k+64 from parentT", null },
     { "insert into parentT select i+128, j+128, k+128 from parentT", null },
     { "insert into parentT select i+256, j+256, k+256 from parentT", null },
     { "insert into parentT select i+512, j+512, k+512 from parentT", null },
     { "insert into parentT select i+1024, j+1024, k+1024 from parentT", null },
     { "insert into parentT select i+2048, j+2048, k+2048 from parentT", null },
     { "insert into parentT select i+4096, j+4096, k+4096 from parentT", null },
     { "insert into parentT select i+8192, j+8192, k+8192 from parentT", null },
	{ "update parentT set j = j /10", null },
     { "update parentT set k = k /100", null },
     { "create unique index parentIdx on parentT(i)", null },
     { "insert into childT select * from parentT", null },
     { "select count(*) from parentT where i < 10 and i not in (select i from childT)", new String[][] { {"0"} } },
     { "select count(*) from parentT where i< 10 and exists (select i from childT where childT.i=parentT.i)", new String[][] { {"9"} } },
     { "select count(*) from parentT where i< 10 and j not in (select distinct j from childT)", new String[][] { {"0"} } },
     { "select count(*) from parentT where i< 10 and exists (select distinct j from childT where childT.j=parentT.j)", new String[][] { {"9"} } },
     { "select count(*) from parentT where i< 10 and k not in (select distinct k from childT)", new String[][] { {"0"} } },
     { "select count(*) from parentT where i< 10 and exists (select distinct k from childT where childT.k=parentT.k)", new String[][] { {"9"} } },
     { "drop table childT", null },
     { "drop table parentT", null }

    };

    Connection conn = TestUtil.getConnection();
    Statement stmt = conn.createStatement();
    // Go through the array, execute each string[0], check sqlstate [1]
    // This will fail on the first one that succeeds where it shouldn't
    // or throws unknown exception
    JDBC.SQLUnitTestHelper(stmt,Script_Subquery2UTPartitioning);

  }
  
  
  protected String getOffHeapSuffix() {
    return "  ";
  }
}
