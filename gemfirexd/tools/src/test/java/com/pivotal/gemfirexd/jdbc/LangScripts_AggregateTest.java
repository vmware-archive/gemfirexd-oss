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

public class LangScripts_AggregateTest extends JdbcTestBase {

  public static void main(String[] args) {
    TestRunner.run(new TestSuite(LangScripts_AggregateTest.class));
  }
  
  public LangScripts_AggregateTest(String name) {
    super(name); 
  }

  @Override
  protected String reduceLogging() {
    return "config";
  }

  // This test is the as-is LangScript conversion, without any partitioning clauses
  public void testLangScript_AggregateTestNoPartitioning() throws Exception
  {
    // This is a JUnit conversion of the Derby Lang aggregate.sql script
    // without any GemFireXD extensions
	  
    // Catch exceptions from illegal syntax
    // Tests still not fixed marked FIXME
	  
    // Array of SQL text to execute and sqlstates to expect
    // The first object is a String, the second is either 
    // 1) null - this means query returns no rows and throws no exceptions
    // 2) a string - this means query returns no rows and throws expected SQLSTATE
    // 3) a String array - this means query returns rows which must match (unordered) given resultset
    //       - for an empty result set, an uninitialized size [0][0] array is used
    Object[][] Script_AggregateUT = {
	// General aggregate tests.  
	{ "create table t1 (c1 int, c2 int)" + getOffHeapSuffix(), null },
	{ "create table t2 (c1 int, c2 int)"+ getOffHeapSuffix(), null },
	{ "create table oneRow (c1 int, c2 int)"+ getOffHeapSuffix(), null },
	{ "insert into oneRow values(1,1)", null },
	{ "create table empty (c1 int, c2 int)"+ getOffHeapSuffix(), null },
	{ "create table emptyNull (c1 int, c2 int)"+ getOffHeapSuffix(), null },
	{ "insert into emptyNull values (null, null)", null },
	{ "insert into t1 values (null, null), (1,1), (null, null), (2,1), (3,1), (10,10)", null },
	{ "insert into t2 values (null, null), (1,1), (null, null), (2,1), (3,1), (10,10)", null },
	{ "select * from t1", new String [][] {
		{null,null}, {"1","1"}, {null,null},
		{"2","1"}, {"3","1"}, {"10","10"} } },
	// Expressions within an aggregate
	{ "select max(c1+10) from t1", new String [][] { {"20"} } },
	{ "select max(c1+10) from t1 group by c2", new String [][] { {"13"}, {"20"}, {null} } },
	{ "select max(2*10) from t1", new String [][] { {"20"} } },
	{ "select max(2*10) from t1 group by c2", new String [][] { {"20"}, {"20"}, {"20"} } },
	// conditional operator within aggregate
	{ "select max(case when c1 <> 1 then 666 else 999 end) from oneRow", new String [][] { {"999"} } },
	{ "select max(case when c1 = 1 then 666 else c2 end) from oneRow", new String [][] { {"666"} } },
	{ "select max(case when c1 = 1 then 666 else c1 end) from oneRow", new String [][] { {"666"} } },
	// subquery in aggregate
	{ "select max((select c1 from empty)) from t1", new String [][] { {null} } },
	{ "select max(cast (c1 as char(1))) from oneRow", new String [][] { {"1"} } },
	{ "select max(cast(c1 as char(1)) || cast (c2 as char(1))) from oneRow", new String [][] { {"11"} } },
	{ "select max(-c1) from t1", new String [][] { {"-1"} } },
	// count
	{ "select count(c1) from t1", new String [][] { {"4"} } },
	{ "select count(cast (null as int)) from t1", new String [][] { {"0"} } },
	// avg
	{ "select avg(2147483647) from t1", new String [][] { {"2147483647"} } },
	// Expressions on an aggregates/with aggregates
	{ "select 10+sum(c1) from t1", new String [][] { {"26"} } },
	{ "select 10+sum(c1+10) from t1", new String [][] { {"66"} } },
	{ "select (case when max(c1) = 1 then 666 else 1 end) from t1", new String [][] { {"1"} } },
	{ "select (case when max(c1) = 1 then 666 else c1 end) from t1 group by c1", new String [][] {
		{"666"}, {"2"}, {"3"}, {"10"}, {null} } },
	// FIXME
	// Crashes with NPE
	//{ "select cast (max(c1) as char(1)) from oneRow", new String [][] { {"1"} } },
	//{ "select cast (max(c1) as char(1)) from oneRow group by c1", new String [][] { {"1"} } },
	//{ "select (cast(c1 as char(1)) || (cast (max(c2) as char(1)))) from oneRow group by c1", new String [][] { {"11"} } },
	{ "select (select max(c1) from t2)from t1", new String [][] {
		{"10"}, {"10"}, {"10"}, {"10"}, {"10"}, {"10"} } },
	{ "select (select max(c1) from oneRow group by c2)from t1", new String [][] {
		{"1"}, {"1"}, {"1"}, {"1"}, {"1"}, {"1"} } },
	{ "select -max(c1) from t1", new String [][] { {"-10"} } },
	{ "select -max(c1) from t1 group by c1", new String [][] { 
		{"-1"}, {"-2"}, {"-3"}, {"-10"}, {null} } },
	{ "select cast (null as int), count(c1) from t1 group by c1", new String [][] {
		{null, "1"}, {null, "1"}, {null, "1"}, {null, "1"}, {null, "0"} } },
	{ "select count(cast (null as int)) from t1 group by c1", new String [][] {
		{"0"}, {"0"}, {"0"}, {"0"}, {"0"} } },
	// binary list operator
	{ "select (1 in (1,2)), count(c1) from t1 group by c1", "42X01" },
	{ "select count((1 in (1,2))) from t1 group by c1", new String [][] {
		{"1"}, {"1"}, {"1"}, {"1"}, {"2"} } },
	{ "select c2, 10+sum(c1), c2 from t1 group by c2", new String [][] {
		{"1","16","1"}, {"10","20","10"}, {null,null,null} } },
	{ "select c2, 10+sum(c1+10), c2*2 from t1 group by c2", new String [][] {
		{"1", "46", "2"}, {"10","30","20"}, {null,null,null} } },
	{ "select c2+sum(c1)+c2 from t1 group by c2", new String [][] { {"8"}, {"30"}, {null} } },
	{ "select (c2+sum(c1)+c2)+10, c1, c2 from t1 group by c1, c2", new String [][] {
		{"13","1","1"}, {"14","2","1"}, {"15","3","1"}, {"40","10","10"}, {null,null,null} } },
	{ "select c1+10, c2, c1*1, c1, c2*5 from t1 group by c1, c2", new String [][] {
		{"11","1","1","1","5"},
		{"12","1","2","2","5"},
		{"13","1","3","3","5"},
		{"20","10","10","10","50"},
		{null,null,null,null,null} } },
	// Distincts
	{ "select sum(c1) from t1", new String [][] { {"16"} } },
	{ "select sum(distinct c1) from t1", new String [][] { {"16"} } },
	{ "select sum(distinct c1), sum(c1) from t1", new String [][] { {"16","16"} } },
	{ "select sum(distinct c1), sum(c1) from oneRow", new String [][] { {"1","1"} } },
	{ "select max(c1), sum(distinct c1), sum(c1) from t1", new String [][] { {"10","16","16"} } },
	{ "select sum(distinct c1) from empty", new String [][] { {null} } },
	{ "select sum(distinct c1) from emptyNull", new String [][] { {null} } },
	{ "select sum(c1) from t1 group by c2", new String [][] { {"6"}, {"10"}, {null} } },
	{ "select sum(distinct c1) from t1 group by c2", new String [][] { {"6"}, {"10"}, {null} } },
	{ "select sum(distinct c1), sum(c1) from t1 group by c2", new String [][] {
		{"6","6"}, {"10","10"}, {null,null} } },
	{ "select sum(distinct c1), sum(c1) from oneRow group by c2", new String [][] { {"1","1"} } },
	{ "select max(c1), sum(distinct c1), sum(c1) from t1 group by c2", new String [][] {
		{"3","6","6"}, {"10","10","10"}, {null,null,null} } },
	{ "select c2, max(c1), c2+1, sum(distinct c1), c2+2, sum(c1) from t1 group by c2", new String [][] {
		{"1","3","2","6","3","6"},
		{"10","10","11","10","12","10"},
		{null,null,null,null,null,null} } },
	{ "select sum(distinct c1) from empty group by c2", new String[0][0] },
	{ "select sum(distinct c1) from emptyNull group by c2", new String[][] { {null} } },
	// Subqueries in where clause
	{ "select c1 from t1 where c1 not in (select sum(c1) from t2)", new String [][] {
		{"1"}, {"2"}, {"3"}, {"10"} } },
	{ "select c1 from t1 where c1 not in (select sum(distinct c1) from t2)", new String [][] {
		{"1"}, {"2"}, {"3"}, {"10"} } },
	{ "select c1 from t1 where c1 not in (select sum(distinct c1)+10 from t2)", new String [][] {
		{"1"}, {"2"}, {"3"}, {"10"} } },
	{ "select c1 from t1 where c1 in (select max(c1) from t2 group by c2)", new String [][] {
		{"3"}, {"10"} } },
	{ "select c1 from t1 where c1 in (select max(distinct c1) from t2 group by c2)", new String [][] {
		{"3"}, {"10"} } },
	{ "select c1 from t1 where c1 in (select max(distinct c1)+10 from t2 group by c2)", new String [0][0] },
	// subqueries that return 1 row
	{ "select c1 from t1 where c1 = (select max(c1) from t2)", new String [][] { {"10"} } },
	{ "select c1 from t1 where c1 = (select max(distinct c1) from t2)", new String [][] { {"10"} } },
	{ "select c1 from t1 where c1 = (select max(distinct c1)+10 from t2)", new String [0][0] },
	{ "select c1 from t1 where c1 = (select max(c1) from oneRow group by c2)", new String [][] { {"1"} } },
	{ "select c1 from t1 where c1 = (select max(distinct c1) from oneRow group by c2)", new String [][] { {"1"} } },
	{ "select c1 from t1 where c1 = (select max(distinct c1)+10 from oneRow group by c2)", new String [0][0] },
	// From Subqueries (aka table expressions)
	{ "select tmpC1 from 	(select max(c1+10) from t1) as tmp (tmpC1)", new String [][] { {"20"} } },
	{ "select max(tmpC1) from 	(select max(c1+10) from t1) as tmp (tmpC1)", new String [][] { {"20"} } },
	{ "select tmpC1 from 	(select max(c1+10) from t1 group by c2) as tmp (tmpC1)", new String [][] {
		{"13"}, {"20"}, {null} } },
	{ "select max(tmpC1) from 	(select max(c1+10) from t1 group by c2) as tmp (tmpC1)", new String [][] { {"20"} } },
	{ "select max(tmpC1), tmpC2 from (select max(c1+10), c2 from t1 group by c2) as tmp (tmpC1, tmpC2) group by tmpC2", new String [][] {
		{"13","1"}, {"20","10"}, {null,null} } },
	//Cartesian product on from subquery: forces multiple opens/closes on the sort result set
	{ "select * from t1, (select max(c1) from t1) as mytab(c1)", new String [][] {
		{null,null,"10"}, {"1","1","10"}, {null,null,"10"},
		{"2","1","10"}, {"3","1","10"}, {"10","10","10"} } },
	{ "select * from t1, (select max(c1) from t1 group by c1) as mytab(c1)", new String [][] {
		{null,null,"1"}, {"1","1","1"}, {null,null,"1"},
		{"2","1","1"}, {"3","1","1"}, {"10","10","1"},
		{null,null,"2"}, {"1","1","2"}, {null,null,"2"},
		{"2","1","2"}, {"3","1","2"}, {"10","10","2"},
		{null,null,"3"}, {"1","1","3"}, {null,null,"3"},
		{"2","1","3"}, {"3","1","3"}, {"10","10","3"},
		{null,null,"10"}, {"1","1","10"}, {null,null,"10"},
		{"2","1","10"}, {"3","1","10"}, {"10","10","10"},
		{null,null,null}, {"1","1",null}, {null,null,null},
		{"2","1",null}, {"3","1",null}, {"10","10",null} } },
	// Union
	{ "select max(c1) from t1 union all select max(c1) from t2", new String [][] {
		{"10"}, {"10"} } },
	// Joins
	{ "select max(t1.c1), max(t2.c2) from t1, t2 where t1.c1 = t2.c1", new String [][] { {"10","10"} } },
	{ "select max(t1.c1), max(t2.c2) from t1, t2 where t1.c1 = t2.c1 group by t1.c1", new String [][] {
		{"1","1"}, {"2","1"}, {"3","1"}, {"10","10"} } },
	// Having
	{ "select max(t1.c1), max(t2.c2) from t1, t2 where t1.c1 = t2.c1 group by t1.c1 having count(*) > 0", new String [][] {
		{"1","1"}, {"2","1"}, {"3","1"}, {"10","10"} } },
	{ "select c1 from t1 group by c1 having max(c2) in (select c1 from t2)", new String [][] {
		{"1"},{"2"},{"3"},{"10"} } },
	{ "select c1 from t1 group by c1 having max(c2) in (select c1 from t2)", new String [][] {
		{"1"},{"2"},{"3"},{"10"} } },
	{ "select c1 from t1 group by c1 having avg(c2) in (select max(t2.c1) from t2)", new String [][] { {"10"} } },
	{ "select c1 from t1 group by c1 having (select max(t2.c1) from t2) = avg(c2)", new String [][] { {"10"} } },
	{ "select c1 from t1 group by c1 having max(c2) > (select avg(t2.c1 + t1.c1)-20 from t2)", new String [][] {
		{"1"}, {"2"}, {"3"}, {"10"} } },
	{ "select c1 from t1 group by c1 having (max(c2) in (select c1 from t2)) OR (max(c1) in (select c2-999 from t2)) OR (count(*) > 0)", new String [][] {
		{"1"}, {"2"}, {"3"}, {"10"}, {null} } },
	{ "select max(c1), (select c1 from oneRow) from t1", new String [][] { {"10","1"} } },
	{ "select max(c1), (select c1 from oneRow) from t1 group by c1", new String [][] {
		{"1","1"}, {"2","1"}, {"3","1"}, {"10","1"}, {null,"1"} } },
	// tests of exact numeric results
	{ "create table bd (i decimal(31,30))"+ getOffHeapSuffix(), null },
	{ "insert into bd values(0.1)", null },
	{ "insert into bd values(0.2)", null },
	{ "select * from bd", new String [][] { {"0.100000000000000000000000000000"}, {"0.200000000000000000000000000000"} } },
	{ "select avg(i), sum(i)/count(i) from bd", new String [][] { {"0.150000000000000000000000000000", "0.150000000000000000000000000000"} } },
	{ "drop table bd", null },
	{ "create table it (i int)"+ getOffHeapSuffix(), null },
	{ "insert into it values (1)", null },
	{ "insert into it values (0)", null },
	{ "insert into it values (0)", null },
	{ "insert into it values (0)", null },
	{ "insert into it values (0)", null },
	{ "insert into it values (0)", null },
	{ "insert into it values (0)", null },
	{ "insert into it values (0)", null },
	{ "insert into it values (0)", null },
	{ "insert into it values (0)", null },
	{ "insert into it values (200001)", null },
	{ "select avg(i), sum(i)/count(i), sum(i), count(i) from it", new String [][] {
		{"18182","18182","200002","11"} } },         
	{ "drop table it", null },
	// test avg cases where the sum will overflow
	{ "create table ovf_int (i int)"+ getOffHeapSuffix(), null },
	{ "insert into ovf_int values (2147483647)", null },
	{ "insert into ovf_int values (2147483647 - 1)", null },
	{ "insert into ovf_int values (2147483647 - 2)", null },
	{ "select avg(i), 2147483647 - 1 from ovf_int", new String [][] { {"2147483646","2147483646"} } },
	{ "drop table ovf_int", null },
	{ "create table ovf_small (i smallint)"+ getOffHeapSuffix(), null },
	{ "insert into ovf_small values (32767)", null },
	{ "insert into ovf_small values (32767 - 1)", null },
	{ "insert into ovf_small values (32767 - 2)", null },
	{ "select avg(i), 32767 - 1 from ovf_small", new String [][] { {"32766","32766"} } },
	{ "drop table ovf_small", null },
	{ "create table ovf_long (i bigint)"+ getOffHeapSuffix(), null },
	{ "insert into ovf_long values (9223372036854775807)", null },
	{ "insert into ovf_long values (9223372036854775807 - 1)", null },
	{ "insert into ovf_long values (9223372036854775807 - 2)", null },
	// FIXME
	// This crashes with NPE
	//{ "select avg(i), 9223372036854775807 - 1 from ovf_long", new String [][] {
	//	{"9223372036854775806", "9223372036854775806"} } },
	//{ "select avg(i), 9223372036854775807 from ovf_long", new String [][] {
	//	{"9223372036854775806", "9223372036854775807"} } },
	//{ "select avg(i) from ovf_long", new String [][] {
	//	{"9223372036854775806"} } },
	//{ "select avg(i) - 1  from ovf_long", new String [][] {
	//	{"9223372036854775805"} } },
	{ "drop table ovf_long", null },
	// Test that AVG is not limited by columns type precision
	{ "create table ovf_real (i real)"+ getOffHeapSuffix(), null },
	{ "insert into ovf_real values (+3.402E+38)", null },
	{ "insert into ovf_real values (+3.402E+38 - 1)", null },
	{ "insert into ovf_real values (+3.402E+38 - 2)", null },
	{ "select avg(i) from ovf_real", new String [][] {
		{"3.4020000005553803E38"} } },  // GemFireXD autopromotes
	{ "drop table ovf_real", null },
	{ "create table ovf_double (i double precision)"+ getOffHeapSuffix(), null },
	{ "insert into ovf_double values (+1.79769E+308)", null },
	{ "insert into ovf_double values (+1.79769E+308 - 1)", null },
	{ "insert into ovf_double values (+1.79769E+308 - 2)", null },
	// FIXME
	// This throws NPE
	//{ "select avg(i) from ovf_double", new String [][] { {"1.79769E308"} } },
	{ "drop table ovf_double", null },
	// CLEAN UP
	{ "drop table t1", null },
	{ "drop table t2", null },
	{ "drop table oneRow", null },
	{ "drop table empty", null },
	{ "drop table emptyNull", null },
	{ "create table t (i int, l bigint)"+ getOffHeapSuffix(), null },
	{ "create table t1 (c1 int)"+ getOffHeapSuffix(), null },
	{ "create table t2 (c1 int)"+ getOffHeapSuffix(), null },
	// NEGATIVE TESTS
	// only a single distinct is supported
	{ "select sum(distinct i), sum(distinct l) from t", "42Z02"} ,
	//  aggregates in aggregates
	{ "select max(max(i)) from t", "42Y33" },
	{ "select max(1+1+1+max(i)) from t", "42Y33" },
	{ "select max(c1), (select max(c1) from t2) from t1", "42Y29" },
	{ "select max(c1), (select max(t1.c1) from t2) from t1", "42Y29" },
	{ "select max(c1), max(c1), (select max(c1) from t1) from t1", "42Y29" },
	// max over a join on a column with an index -- Beetle 4423
	{ "create table t3(a int)"+ getOffHeapSuffix(), null },
	{ "insert into t3 values(1),(2),(3),(4),(5)", null },
	{ "create table t4(a int)"+ getOffHeapSuffix(), null },
	{ "insert into t4 select a from t3", null },
	{ "create index tindex on t3(a)", null },
	{ "select max(t3.a) from t3, t4 where t3.a = t4.a and t3.a = 1", new String [][] { {"1"} } },
	{ "drop table t", null },
	{ "drop table t1", null },
	{ "drop table t2", null },
	{ "drop table t3", null },
	{ "drop table t4", null },
	//beetle 5122, aggregate on JoinNode
	{ "CREATE TABLE DOCUMENT_VERSION(DOCUMENT_ID INT,DOCUMENT_STATUS_ID INT)"+ getOffHeapSuffix(), null },
	{ "insert into DOCUMENT_VERSION values (2,2),(9,9),(5,5),(1,3),(10,5),(1,6),(10,8),(1,10)", null },
	{ "CREATE VIEW MAX_DOCUMENT_VERSION AS SELECT  DOCUMENT_ID  FROM DOCUMENT_VERSION", null },
	{ "CREATE VIEW MAX_DOCUMENT_VERSION_AND_STATUS_ID AS SELECT  MAX(DV.DOCUMENT_STATUS_ID) AS MAX_DOCUMENT_STATUS_ID FROM DOCUMENT_VERSION AS DV , MAX_DOCUMENT_VERSION WHERE DV.DOCUMENT_ID = 1", null },
	{ "CREATE VIEW LATEST_DOC_VERSION AS SELECT DOCUMENT_ID FROM DOCUMENT_VERSION AS DV, MAX_DOCUMENT_VERSION_AND_STATUS_ID AS MDVASID WHERE DV.DOCUMENT_ID = MDVASID.MAX_DOCUMENT_STATUS_ID", null },
	{ "select * from LATEST_DOC_VERSION", new String [][] { {"10"}, {"10"} } },
	{ "drop view LATEST_DOC_VERSION", null },
	{ "drop view MAX_DOCUMENT_VERSION_AND_STATUS_ID", null },
	{ "drop view  MAX_DOCUMENT_VERSION", null },
	{ "drop table DOCUMENT_VERSION", null },
	// Prevent aggregates being used in VALUES clause or WHERE clause.
	{ "create table tmax(i int)"+ getOffHeapSuffix(), null },
	{ "values sum(1)", "42903" },
	{ "values max(3)", "42903" },
	{ "select * from tmax where sum(i)=1", "42903" },
	{ "select i from tmax where substr('abc', sum(1), 3) = 'abc'", "42903" },
	{ "drop table tmax", null }
    };

    // Do not use partitioning as default, use replicate
    // (Some results are expected to be different with partitioning)
    skipDefaultPartitioned = true;

    Connection conn = TestUtil.getConnection();
    Statement stmt = conn.createStatement();
    // Go through the array, execute each string[0], check sqlstate [1]
    // This will fail on the first one that succeeds where it shouldn't
    // or throws unknown exception
    JDBC.SQLUnitTestHelper(stmt,Script_AggregateUT);
  }
  
  // This test is the script enhanced with partitioning
  public void testLangScript_AggregateWithPartitioning() throws Exception
  {
    // This form of the aggregate.sql test has partitioning clauses
    Object[][] Script_AggregateUTPartitioning = {
	// General aggregate tests.  
	{ "create table t1 (c1 int, c2 int) partition by column(c1)"+ getOffHeapSuffix(), null },
	{ "create table t2 (c1 int, c2 int) partition by column(c1) colocate with(t1)"+ getOffHeapSuffix(), null },
	{ "create table oneRow (c1 int, c2 int) partition by column(c1)"+ getOffHeapSuffix(), null },
	{ "insert into oneRow values(1,1)", null },
	{ "create table empty (c1 int, c2 int) partition by column(c1)"+ getOffHeapSuffix(), null },
	{ "create table emptyNull (c1 int, c2 int) partition by column(c1)"+ getOffHeapSuffix(), null },
	{ "insert into emptyNull values (null, null)", null },
	{ "insert into t1 values (null, null), (1,1), (null, null), (2,1), (3,1), (10,10)", null },
	{ "insert into t2 values (null, null), (1,1), (null, null), (2,1), (3,1), (10,10)", null },
	{ "select * from t1", new String [][] {
		{null,null}, {"1","1"}, {null,null},
		{"2","1"}, {"3","1"}, {"10","10"} } },
	// Expressions within an aggregate
	{ "select max(c1+10) from t1", new String [][] { {"20"} } },
	{ "select max(c1+10) from t1 group by c2", new String [][] { {"13"}, {"20"}, {null} } },
	{ "select max(2*10) from t1", new String [][] { {"20"} } },
	{ "select max(2*10) from t1 group by c2", new String [][] { {"20"}, {"20"}, {"20"} } },
	// conditional operator within aggregate
	{ "select max(case when c1 <> 1 then 666 else 999 end) from oneRow", new String [][] { {"999"} } },
	{ "select max(case when c1 = 1 then 666 else c2 end) from oneRow", new String [][] { {"666"} } },
	{ "select max(case when c1 = 1 then 666 else c1 end) from oneRow", new String [][] { {"666"} } },
	// subquery in aggregate
	{ "select max((select c1 from empty)) from t1", new String [][] { {null} } },
	{ "select max(cast (c1 as char(1))) from oneRow", new String [][] { {"1"} } },
	{ "select max(cast(c1 as char(1)) || cast (c2 as char(1))) from oneRow", new String [][] { {"11"} } },
	{ "select max(-c1) from t1", new String [][] { {"-1"} } },
	// count
	{ "select count(c1) from t1", new String [][] { {"4"} } },
	{ "select count(cast (null as int)) from t1", new String [][] { {"0"} } },
	// avg
	{ "select avg(2147483647) from t1", new String [][] { {"2147483647"} } },
	// Expressions on an aggregates/with aggregates
	{ "select 10+sum(c1) from t1", new String [][] { {"26"} } },
	{ "select 10+sum(c1+10) from t1", new String [][] { {"66"} } },
	{ "select (case when max(c1) = 1 then 666 else 1 end) from t1", new String [][] { {"1"} } },
	{ "select (case when max(c1) = 1 then 666 else c1 end) from t1 group by c1", new String [][] {
		{"666"}, {"2"}, {"3"}, {"10"}, {null} } },
	// FIXME
	// Crashes with NPE
	//{ "select cast (max(c1) as char(1)) from oneRow", new String [][] { {"1"} } },
	//{ "select cast (max(c1) as char(1)) from oneRow group by c1", new String [][] { {"1"} } },
	//{ "select (cast(c1 as char(1)) || (cast (max(c2) as char(1)))) from oneRow group by c1", new String [][] { {"11"} } },
	{ "select (select max(c1) from t2)from t1", new String [][] {
		{"10"}, {"10"}, {"10"}, {"10"}, {"10"}, {"10"} } },
	{ "select (select max(c1) from oneRow group by c2)from t1", new String [][] {
		{"1"}, {"1"}, {"1"}, {"1"}, {"1"}, {"1"} } },
	{ "select -max(c1) from t1", new String [][] { {"-10"} } },
	{ "select -max(c1) from t1 group by c1", new String [][] { 
		{"-1"}, {"-2"}, {"-3"}, {"-10"}, {null} } },
	{ "select cast (null as int), count(c1) from t1 group by c1", new String [][] {
		{null, "1"}, {null, "1"}, {null, "1"}, {null, "1"}, {null, "0"} } },
	{ "select count(cast (null as int)) from t1 group by c1", new String [][] {
		{"0"}, {"0"}, {"0"}, {"0"}, {"0"} } },
	// binary list operator
	{ "select (1 in (1,2)), count(c1) from t1 group by c1", "42X01" },
	{ "select count((1 in (1,2))) from t1 group by c1", new String [][] {
		{"1"}, {"1"}, {"1"}, {"1"}, {"2"} } },
	{ "select c2, 10+sum(c1), c2 from t1 group by c2", new String [][] {
		{"1","16","1"}, {"10","20","10"}, {null,null,null} } },
	{ "select c2, 10+sum(c1+10), c2*2 from t1 group by c2", new String [][] {
		{"1", "46", "2"}, {"10","30","20"}, {null,null,null} } },
	{ "select c2+sum(c1)+c2 from t1 group by c2", new String [][] { {"8"}, {"30"}, {null} } },
	{ "select (c2+sum(c1)+c2)+10, c1, c2 from t1 group by c1, c2", new String [][] {
		{"13","1","1"}, {"14","2","1"}, {"15","3","1"}, {"40","10","10"}, {null,null,null} } },
	{ "select c1+10, c2, c1*1, c1, c2*5 from t1 group by c1, c2", new String [][] {
		{"11","1","1","1","5"},
		{"12","1","2","2","5"},
		{"13","1","3","3","5"},
		{"20","10","10","10","50"},
		{null,null,null,null,null} } },
	// Distincts
	{ "select sum(c1) from t1", new String [][] { {"16"} } },
	{ "select sum(distinct c1) from t1", new String [][] { {"16"} } },
	{ "select sum(distinct c1), sum(c1) from t1", new String [][] { {"16","16"} } },
	{ "select sum(distinct c1), sum(c1) from oneRow", new String [][] { {"1","1"} } },
	{ "select max(c1), sum(distinct c1), sum(c1) from t1", new String [][] { {"10","16","16"} } },
	{ "select sum(distinct c1) from empty", new String [][] { {null} } },
	{ "select sum(distinct c1) from emptyNull", new String [][] { {null} } },
	{ "select sum(c1) from t1 group by c2", new String [][] { {"6"}, {"10"}, {null} } },
	{ "select sum(distinct c1) from t1 group by c2", new String [][] { {"6"}, {"10"}, {null} } },
	{ "select sum(distinct c1), sum(c1) from t1 group by c2", new String [][] {
		{"6","6"}, {"10","10"}, {null,null} } },
	{ "select sum(distinct c1), sum(c1) from oneRow group by c2", new String [][] { {"1","1"} } },
	{ "select max(c1), sum(distinct c1), sum(c1) from t1 group by c2", new String [][] {
		{"3","6","6"}, {"10","10","10"}, {null,null,null} } },
	{ "select c2, max(c1), c2+1, sum(distinct c1), c2+2, sum(c1) from t1 group by c2", new String [][] {
		{"1","3","2","6","3","6"},
		{"10","10","11","10","12","10"},
		{null,null,null,null,null,null} } },
	{ "select sum(distinct c1) from empty group by c2", new String[0][0] },
	{ "select sum(distinct c1) from emptyNull group by c2", new String[][] { {null} } },
	// Subqueries in where clause
	{ "select c1 from t1 where c1 not in (select sum(c1) from t2)", new String [][] {
		{"1"}, {"2"}, {"3"}, {"10"} } },
	{ "select c1 from t1 where c1 not in (select sum(distinct c1) from t2)", new String [][] {
		{"1"}, {"2"}, {"3"}, {"10"} } },
	{ "select c1 from t1 where c1 not in (select sum(distinct c1)+10 from t2)", new String [][] {
		{"1"}, {"2"}, {"3"}, {"10"} } },
	{ "select c1 from t1 where c1 in (select max(c1) from t2 group by c2)", new String [][] {
		{"3"}, {"10"} } },
	{ "select c1 from t1 where c1 in (select max(distinct c1) from t2 group by c2)", new String [][] {
		{"3"}, {"10"} } },
	{ "select c1 from t1 where c1 in (select max(distinct c1)+10 from t2 group by c2)", new String [0][0] },
	// subqueries that return 1 row
	{ "select c1 from t1 where c1 = (select max(c1) from t2)", new String [][] { {"10"} } },
	{ "select c1 from t1 where c1 = (select max(distinct c1) from t2)", new String [][] { {"10"} } },
	{ "select c1 from t1 where c1 = (select max(distinct c1)+10 from t2)", new String [0][0] },
	{ "select c1 from t1 where c1 = (select max(c1) from oneRow group by c2)", new String [][] { {"1"} } },
	{ "select c1 from t1 where c1 = (select max(distinct c1) from oneRow group by c2)", new String [][] { {"1"} } },
	{ "select c1 from t1 where c1 = (select max(distinct c1)+10 from oneRow group by c2)", new String [0][0] },
	// From Subqueries (aka table expressions)
	{ "select tmpC1 from 	(select max(c1+10) from t1) as tmp (tmpC1)", new String [][] { {"20"} } },
	{ "select max(tmpC1) from 	(select max(c1+10) from t1) as tmp (tmpC1)", new String [][] { {"20"} } },
	{ "select tmpC1 from 	(select max(c1+10) from t1 group by c2) as tmp (tmpC1)", "0A000" },   // GemFireXD not supported on partitioned tables
	{ "select max(tmpC1) from 	(select max(c1+10) from t1 group by c2) as tmp (tmpC1)", "0A000" },
	{ "select max(tmpC1), tmpC2 from (select max(c1+10), c2 from t1 group by c2) as tmp (tmpC1, tmpC2) group by tmpC2", "0A000" },
	//Cartesian product on from subquery: forces multiple opens/closes on the sort result set
	{ "select * from t1, (select max(c1) from t1) as mytab(c1)", new String [][] {
		{null,null,"10"}, {"1","1","10"}, {null,null,"10"},
		{"2","1","10"}, {"3","1","10"}, {"10","10","10"} } },
	{ "select * from t1, (select max(c1) from t1 group by c1) as mytab(c1)", "0A000" },
	// Union
	{ "select max(c1) from t1 union all select max(c1) from t2", new String [][] {
		{"10"}, {"10"} } },
	// Joins
	{ "select max(t1.c1), max(t2.c2) from t1, t2 where t1.c1 = t2.c1", new String [][] { {"10","10"} } },
	{ "select max(t1.c1), max(t2.c2) from t1, t2 where t1.c1 = t2.c1 group by t1.c1", new String [][] {
		{"1","1"}, {"2","1"}, {"3","1"}, {"10","10"} } },
	// Having
	{ "select max(t1.c1), max(t2.c2) from t1, t2 where t1.c1 = t2.c1 group by t1.c1 having count(*) > 0", new String [][] {
		{"1","1"}, {"2","1"}, {"3","1"}, {"10","10"} } },
	{ "select c1 from t1 group by c1 having max(c2) in (select c1 from t2)", new String [][] {
		{"1"},{"2"},{"3"},{"10"} } },
	{ "select c1 from t1 group by c1 having max(c2) in (select c1 from t2)", new String [][] {
		{"1"},{"2"},{"3"},{"10"} } },
	{ "select c1 from t1 group by c1 having avg(c2) in (select max(t2.c1) from t2)", new String [][] { {"10"} } },
	{ "select c1 from t1 group by c1 having (select max(t2.c1) from t2) = avg(c2)", new String [][] { {"10"} } },
	{ "select c1 from t1 group by c1 having max(c2) > (select avg(t2.c1 + t1.c1)-20 from t2)", new String [][] {
		{"1"}, {"2"}, {"3"}, {"10"} } },
	{ "select c1 from t1 group by c1 having (max(c2) in (select c1 from t2)) OR (max(c1) in (select c2-999 from t2)) OR (count(*) > 0)", new String [][] {
		{"1"}, {"2"}, {"3"}, {"10"}, {null} } },
	{ "select max(c1), (select c1 from oneRow) from t1", new String [][] { {"10","1"} } },
	{ "select max(c1), (select c1 from oneRow) from t1 group by c1", new String [][] {
		{"1","1"}, {"2","1"}, {"3","1"}, {"10","1"}, {null,"1"} } },
	// tests of exact numeric results
	{ "create table bd (i decimal(31,30)) partition by column(i)"+ getOffHeapSuffix(), null },
	{ "insert into bd values(0.1)", null },
	{ "insert into bd values(0.2)", null },
	{ "select * from bd", new String [][] { {"0.100000000000000000000000000000"}, {"0.200000000000000000000000000000"} } },
	{ "select avg(i), sum(i)/count(i) from bd", new String [][] { {"0.150000000000000000000000000000", "0.150000000000000000000000000000"} } },
	{ "drop table bd", null },
	{ "create table it (i int) partition by column(i)"+ getOffHeapSuffix(), null },
	{ "insert into it values (1)", null },
	{ "insert into it values (0)", null },
	{ "insert into it values (0)", null },
	{ "insert into it values (0)", null },
	{ "insert into it values (0)", null },
	{ "insert into it values (0)", null },
	{ "insert into it values (0)", null },
	{ "insert into it values (0)", null },
	{ "insert into it values (0)", null },
	{ "insert into it values (0)", null },
	{ "insert into it values (200001)", null },
	{ "select avg(i), sum(i)/count(i), sum(i), count(i) from it", new String [][] {
		{"18182","18182","200002","11"} } },         
	{ "drop table it", null },
	// test avg cases where the sum will overflow
	{ "create table ovf_int (i int) partition by column(i)"+ getOffHeapSuffix(), null },
	{ "insert into ovf_int values (2147483647)", null },
	{ "insert into ovf_int values (2147483647 - 1)", null },
	{ "insert into ovf_int values (2147483647 - 2)", null },
	{ "select avg(i), 2147483647 - 1 from ovf_int", new String [][] { {"2147483646","2147483646"} } },
	{ "drop table ovf_int", null },
	{ "create table ovf_small (i smallint) partition by column(i)"+ getOffHeapSuffix(), null },
	{ "insert into ovf_small values (32767)", null },
	{ "insert into ovf_small values (32767 - 1)", null },
	{ "insert into ovf_small values (32767 - 2)", null },
	{ "select avg(i), 32767 - 1 from ovf_small", new String [][] { {"32766","32766"} } },
	{ "drop table ovf_small", null },
	{ "create table ovf_long (i bigint) partition by column(i)"+ getOffHeapSuffix(), null },
	{ "insert into ovf_long values (9223372036854775807)", null },
	{ "insert into ovf_long values (9223372036854775807 - 1)", null },
	{ "insert into ovf_long values (9223372036854775807 - 2)", null },
	// FIXME
	// This crashes with NPE
	//{ "select avg(i), 9223372036854775807 - 1 from ovf_long", new String [][] {
	//	{"9223372036854775806", "9223372036854775806"} } },
	//{ "select avg(i), 9223372036854775807 from ovf_long", new String [][] {
	//	{"9223372036854775806", "9223372036854775807"} } },
	//{ "select avg(i) from ovf_long", new String [][] {
	//	{"9223372036854775806"} } },
	//{ "select avg(i) - 1  from ovf_long", new String [][] {
	//	{"9223372036854775805"} } },
	{ "drop table ovf_long", null },
	// Test that AVG is not limited by columns type precision
	{ "create table ovf_real (i real) partition by column(i)"+ getOffHeapSuffix(), null },
	{ "insert into ovf_real values (+3.402E+38)", null },
	{ "insert into ovf_real values (+3.402E+38 - 1)", null },
	{ "insert into ovf_real values (+3.402E+38 - 2)", null },
	{ "select avg(i) from ovf_real", new String [][] {
		{"3.4020000005553803E38"} } },  // GemFireXD autopromotes
	{ "drop table ovf_real", null },
	{ "create table ovf_double (i double precision) partition by column(i)"+ getOffHeapSuffix(), null },
	{ "insert into ovf_double values (+1.79769E+308)", null },
	{ "insert into ovf_double values (+1.79769E+308 - 1)", null },
	{ "insert into ovf_double values (+1.79769E+308 - 2)", null },
	// FIXME
	// This throws NPE
	//{ "select avg(i) from ovf_double", new String [][] { {"1.79769E308"} } },
	{ "drop table ovf_double", null },
	// CLEAN UP
	{ "drop table t2", null },
	{ "drop table t1", null },
	{ "drop table oneRow", null },
	{ "drop table empty", null },
	{ "drop table emptyNull", null },
	{ "create table t (i int, l bigint) partition by column(i)"+ getOffHeapSuffix(), null },
	{ "create table t1 (c1 int) partition by column(c1)"+ getOffHeapSuffix(), null },
	{ "create table t2 (c1 int) partition by column(c1)"+ getOffHeapSuffix(), null },
	// NEGATIVE TESTS
	// only a single distinct is supported
	{ "select sum(distinct i), sum(distinct l) from t", "42Z02"} ,
	//  aggregates in aggregates
	{ "select max(max(i)) from t", "42Y33" },
	{ "select max(1+1+1+max(i)) from t", "42Y33" },
	{ "select max(c1), (select max(c1) from t2) from t1", "42Y29" },
	{ "select max(c1), (select max(t1.c1) from t2) from t1", "42Y29" },
	{ "select max(c1), max(c1), (select max(c1) from t1) from t1", "42Y29" },
	// max over a join on a column with an index -- Beetle 4423
	{ "create table t3(a int) partition by column(a)"+ getOffHeapSuffix(), null },
	{ "insert into t3 values(1),(2),(3),(4),(5)", null },
	{ "create table t4(a int) partition by column(a)"+ getOffHeapSuffix(), null },
	{ "insert into t4 select a from t3", null },
	{ "create index tindex on t3(a)", null },
	{ "select max(t3.a) from t3, t4 where t3.a = t4.a and t3.a = 1", new String [][] { {"1"} } },
	{ "drop table t", null },
	{ "drop table t1", null },
	{ "drop table t2", null },
	{ "drop table t3", null },
	{ "drop table t4", null },
	//beetle 5122, aggregate on JoinNode
	{ "CREATE TABLE DOCUMENT_VERSION(DOCUMENT_ID INT,DOCUMENT_STATUS_ID INT) partition by column(document_id)"+ getOffHeapSuffix(), null },
	{ "insert into DOCUMENT_VERSION values (2,2),(9,9),(5,5),(1,3),(10,5),(1,6),(10,8),(1,10)", null },
	{ "CREATE VIEW MAX_DOCUMENT_VERSION AS SELECT  DOCUMENT_ID  FROM DOCUMENT_VERSION", null },
	{ "CREATE VIEW MAX_DOCUMENT_VERSION_AND_STATUS_ID AS SELECT  MAX(DV.DOCUMENT_STATUS_ID) AS MAX_DOCUMENT_STATUS_ID FROM DOCUMENT_VERSION AS DV , MAX_DOCUMENT_VERSION WHERE DV.DOCUMENT_ID = 1", null },
	{ "CREATE VIEW LATEST_DOC_VERSION AS SELECT DOCUMENT_ID FROM DOCUMENT_VERSION AS DV, MAX_DOCUMENT_VERSION_AND_STATUS_ID AS MDVASID WHERE DV.DOCUMENT_ID = MDVASID.MAX_DOCUMENT_STATUS_ID", null },
	{ "select * from LATEST_DOC_VERSION", new String [][] { {"10"}, {"10"} } },
	{ "drop view LATEST_DOC_VERSION", null },
	{ "drop view MAX_DOCUMENT_VERSION_AND_STATUS_ID", null },
	{ "drop view  MAX_DOCUMENT_VERSION", null },
	{ "drop table DOCUMENT_VERSION", null },
	// Prevent aggregates being used in VALUES clause or WHERE clause.
	{ "create table tmax(i int) partition by column(i)"+ getOffHeapSuffix(), null },
	{ "values sum(1)", "42903" },
	{ "values max(3)", "42903" },
	{ "select * from tmax where sum(i)=1", "42903" },
	{ "select i from tmax where substr('abc', sum(1), 3) = 'abc'", "42903" },
	{ "drop table tmax", null }
    };

    Connection conn = TestUtil.getConnection();
    Statement stmt = conn.createStatement();
    // Go through the array, execute each string[0], check sqlstate [1]
    // This will fail on the first one that succeeds where it shouldn't
    // or throws unknown exception
    JDBC.SQLUnitTestHelper(stmt,Script_AggregateUTPartitioning);

  }
  
  
  protected String getOffHeapSuffix() {
    return "  ";
  }

}
