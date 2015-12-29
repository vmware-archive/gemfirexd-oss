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

public class LangScripts_ArithmeticTest extends JdbcTestBase {

  public static void main(String[] args) {
    TestRunner.run(new TestSuite(LangScripts_ArithmeticTest.class));
  }
  
  public LangScripts_ArithmeticTest(String name) {
    super(name); 
  }

  // This test is the as-is LangScript conversion, without any partitioning clauses
  public void testLangScript_ArithmeticTestNoPartitioning() throws Exception
  {
    // This is a JUnit conversion of the Derby Lang arithmetic.sql script
    // without any GemFireXD extensions
	  
    // Catch exceptions from illegal syntax
    // Tests still not fixed marked FIXME
	  
    // Array of SQL text to execute and sqlstates to expect
    // The first object is a String, the second is either 
    // 1) null - this means query returns no rows and throws no exceptions
    // 2) a string - this means query returns no rows and throws expected SQLSTATE
    // 3) a String array - this means query returns rows which must match (unordered) given resultset
    //       - for an empty result set, an uninitialized size [0][0] array is used
    Object[][] Script_ArithmeticUT = {
	{ "create table t (i int, j int)", null },
	{ "insert into t values (null, null)", null },
	{ "insert into t values (0, 100)", null },
	{ "insert into t values (1, 101)", null },
	{ "insert into t values (-2, -102)", null },
	{ "select i + j from t", new String [][] { {null},{"100"},{"102"},{"-104"} } },
	{ "select i, i + 10 + 20, j, j + 100 + 200 from t", new String [][] {
		{null,null,null,null},
		{"0","30","100","400"},
		{"1","31","101","401"},
		{"-2","28","-102","198"} } },
	{ "select i - j, j - i from t", new String [][] { {null,null}, {"-100","100"},
		{"-100","100"}, {"100","-100"} } },
	{ "select i, i - 10 - 20, 20 - 10 - i, j, j - 100 - 200, 200 - 100 - j from t", new String [][] {
		{null,null,null,null,null,null},
		{"0","-30","10","100","-200","0"},
		{"1","-29","9","101","-199","-1"},
		{"-2","-32","12","-102","-402","202"} } },
	{ "select i, j, i * j, j * i from t", new String [][] {
		{null,null,null,null},
		{"0","100","0","0"},
		{"1","101","101","101"},
		{"-2","-102","204","204"} } },
	{ "select i, j, i * 10 * -20, j * 100 * -200 from t", new String [][] {
		{null,null,null,null},
		{"0","100","0","-2000000"},
		{"1","101","-200","-2020000"},
		{"-2","-102","400","2040000"} } },
	{ "select -i, -j, -(i * 10 * -20), -(j * 100 * -200) from t", new String [][] {
		{null,null,null,null},
		{"0","-100","0","2000000"},
		{"-1","-101","200","2020000"},
		{"2","102","-400","-2040000"} } },
	{ "select +i, +j, +(+i * +10 * -20), +(+j * +100 * -200) from t", new String [][] {
		{null,null,null,null},
		{"0","100","0","-2000000"},
		{"1","101","-200","-2020000"},
		{"-2","-102","400","2040000"} } },
	// test null/null, constant/null, null/constant
	{ "select i, j, i / j, 10 / j, j / 10 from t", new String [][] {
		{null,null,null,null,null},
		{"0","100","0","0","10"},
		{"1","101","0","0","10"},
		{"-2","-102","0","0","-10"} } },
	// test for divide by 0
	{ "select j / i from t", "22012" },      // Math div by zero error during result set scan
	{ "select (j - 1) / (i + 4), 20 / 5 / 4, 20 / 4 / 5 from t", new String [][] {
		{null, "1","1"},
		{"24","1","1"},
		{"20","1","1"},
		{"-51","1","1"} } },
	{ "select j, j / (0 - j), (0 - j) / j, (0 - j) / (0 - j) from t", new String [][] {
		{null,null,null,null},
		{"100","-1","-1","1"},
		{"101","-1","-1","1"},
		{"-102","-1","-1","1"} } },
	{ "select i, i + 10, i - (10 - 20), i - 10, i - (20 - 10) from t", new String [][] {
		{null,null,null,null,null},
		{"0","10","10","-10","-10"},
		{"1","11","11","-9","-9"},
		{"-2","8","8","-12","-12"} } },
	{ "select 'The next 2 columns should agree', 2 + 3 * 4 + 5, 2 + (3 * 4) + 5 from t", new String [][] {
		{"The next 2 columns should agree","19","19"},
		{"The next 2 columns should agree","19","19"},
		{"The next 2 columns should agree","19","19"},
		{"The next 2 columns should agree","19","19"} } },
	{ "select 'The next column should be 45', (2 + 3) * (4 + 5) from t", new String [][] {
		{"The next column should be 45","45"},
		{"The next column should be 45","45"},
		{"The next column should be 45","45"},
		{"The next column should be 45","45"} } },
	{ "delete from t", null },
	{ "insert into t values (null, null)", null },
	{ "insert into t values (0, 100)", null },
	{ "insert into t values (1, 101)", null },
	{ "select i + 2147483647 from t", "22003" },
	{ "select i - 2147483647 - 1, 'This query should work' from t", new String [][] {
		{null, "This query should work"},
		{"-2147483648","This query should work"},
		{"-2147483647","This query should work"} } },
	{ "select i - 2147483647 - 2, 'This query should fail' from t", "22003" },
	{ "select j * 2147483647 from t", "22003" },
	{ "select j * -2147483647 from t", "22003" },
	{ "insert into t values (-2147483648, 0)", null },
	{ "select -i from t", "22003" },
	// test the arithmetic operators on a type we know they don't work on
	{ "create table s (x char(10), y char(10))", null },
	{ "select x + y from s", "42Y95" },
	{ "select x - y from s", "42Y95" },
	{ "select x * y from s", "42Y95" },
	{ "select x / y from s", "42Y95" },
	{ "select -x from s", "42X37" },
	{ "create table smallint_t (i smallint, j smallint)", null },
	{ "create table smallint_s (i smallint, j smallint)", null },
	{ "insert into smallint_t values (null, null)", null },
	{ "insert into smallint_t values (0, 100)", null },
	{ "insert into smallint_t values (1, 101)", null },
	{ "insert into smallint_t values (-2, -102)", null },
	{ "select i + j from smallint_t", new String [][] { {null},{"100"},{"102"},{"-104"} } },
	{ "select i, j, i + i + j, j + j + i from smallint_t", new String [][] {
		{null,null,null,null},
		{"0","100","100","200"},
		{"1","101","103","203"},
		{"-2","-102","-106","-206"} } },
	{ "select i - j, j - i from smallint_t", new String [][] {
		{null,null},{"-100","100"},{"-100","100"},{"100","-100"} } },
	{ "select i, i - j - j, j - j - i, j, j - i - i, i - i - j from smallint_t", new String [][] {
		{null,null,null,null,null,null},
		{"0","-200","0","100","100","-100"},
		{"1","-201","-1","101","99","-101"},
		{"-2","202","2","-102","-98","102"} } },
	{ "select i, j, i * j, j * i from smallint_t", new String [][] {
		{null,null,null,null},
		{"0","100","0","0"},
		{"1","101","101","101"},
		{"-2","-102","204","204"} } },
	{ "select i, j, i * i * (i - j), j * i * (i - j) from smallint_t", new String [][] {
		{null,null,null,null},
		{"0","100","0","0"},
		{"1","101","-100","-10100"},
		{"-2","-102","400","20400"} } },
	{ "select -i, -j, -(i * i * (i - j)), -(j * i * (i - j)) from smallint_t", new String [][] {
		{null,null,null,null},
		{"0","-100","0","0"},
		{"-1","-101","100","10100"},
		{"2","102","-400","-20400"} } },
	{ "select j / i from smallint_t", "22012" },
	{ "insert into smallint_s values (1, 32767)", null },
	{ "select i + j from smallint_s", "22003" },
	{ "select i - j - j from smallint_s", "22003" },
	{ "select j + j from smallint_s", "22003" },
	{ "select j * j from smallint_s", "22003" },
	{ "insert into smallint_s values (-32768, 0)", null },
	{ "select -i from smallint_s", "22003" },
	// test mixed types: int and smallint
	{ "create table smallint_r (y smallint)", null },
	{ "insert into smallint_r values (2)", null },
	{ "select 65535 + y from smallint_r", new String [][] { {"65537"} } },
	// FIXME
	// This throws datatype range exception 22003 (even though previous statement works!)
	// All these fail with Y on the LHS of the math operator
	//{ "select y + 65535 from smallint_r", new String [][] { {"65537"} } },
	{ "select 65535 - y from smallint_r", new String [][] { {"65533"} } },
	//{ "select y - 65535 from smallint_r", new String [][] { {"-65533"} } },
	{ "select 65535 * y from smallint_r", new String [][] { {"131070"} } },
	//{ "select y * 65535 from smallint_r", new String [][] { {"131070"} } },
	{ "select 65535 / y from smallint_r", new String [][] { {"32767"} } },
	//{ "select y / 65535 from smallint_r", new String [][] { {"0"} } },  // This shouldn't fail at all
	// do the same thing with bigints
	{ "create table bigint_t (i bigint, j bigint)", null },
	{ "create table bigint_s (i bigint, j bigint)", null },
	{ "insert into bigint_t values (null, null)", null },
	{ "insert into bigint_t values (0, 100)", null },
	{ "insert into bigint_t values (1, 101)", null },
	{ "insert into bigint_t values (-2, -102)", null },
	{ "select i + j from bigint_t", new String [][] { {null},{"100"},{"102"},{"-104"} } },
	{ "select i, j, i + i + j, j + j + i from bigint_t", new String [][] {
		{null,null,null,null},
		{"0","100","100","200"},
		{"1","101","103","203"},
		{"-2","-102","-106","-206"} } },
	{ "select i - j, j - i from bigint_t", new String [][] {
		{null,null}, {"-100","100"}, {"-100","100"}, {"100","-100"} } },
	{ "select i, i - j - j, j - j - i, j, j - i - i, i - i - j from bigint_t", new String [][] {
		{null,null,null,null,null,null},
		{"0","-200","0","100","100","-100"},
		{"1","-201","-1","101","99","-101"},
		{"-2","202","2","-102","-98","102"} } },
	{ "select i, j, i * j, j * i from bigint_t", new String [][] {
		{null,null,null,null},
		{"0","100","0","0"},
		{"1","101","101","101"},
		{"-2","-102","204","204"} } },
	{ "select i, j, i * i * (i - j), j * i * (i - j) from bigint_t", new String [][] {
		{null,null,null,null},
		{"0","100","0","0"},
		{"1","101","-100","-10100"},
		{"-2","-102","400","20400"} } },
	{ "select -i, -j, -(i * i * (i - j)), -(j * i * (i - j)) from bigint_t", new String [][] {
		{null,null,null,null},
		{"0","-100","0","0"},
		{"-1","-101","100","10100"},
		{"2","102","-400","-20400"} } },
	{ "select j / i from bigint_t", "22012" },
	{ "insert into bigint_s values (1, 9223372036854775807)", null },
	{ "select i + j from bigint_s", "22003" },
	{ "select i - j - j from bigint_s", "22003" },
	{ "select j + j from bigint_s", "22003" },
	{ "select j * j from bigint_s", "22003" },
	{ "select 2 * (9223372036854775807 / 2 + 1) from bigint_s", "22003" },
	{ "select -2 * (9223372036854775807 / 2 + 2) from bigint_s", "22003" },
	{ "select 2 * (-9223372036854775808 / 2 - 1) from bigint_s", "22003" },
	{ "select -2 * (-9223372036854775808 / 2 - 1) from bigint_s", "22003" },
	{ "insert into bigint_s values (-9223372036854775808, 0)", null },
	{ "select -i from bigint_s", "22003" },
	{ "select -j from bigint_s", new String [][] { {"-9223372036854775807"},{"0"} } },
	{ "select i / 2 * 2 + 1 from bigint_s", new String [][] { {"1"},{"-9223372036854775807"} } },
	{ "select j / 2 * 2 from bigint_s", new String [][] { {"9223372036854775806"}, {"0"} } },
	// test mixed types: int and bigint
	{ "create table bigint_r (y bigint)", null },
	{ "insert into bigint_r values (2)", null },
	{ "select 2147483647 + y from bigint_r", new String [][] { {"2147483649"} } },
	{ "select y + 2147483647 from bigint_r", new String [][] { {"2147483649"} } },
	{ "select 2147483647 - y from bigint_r", new String [][] { {"2147483645"} } },
	{ "select y - 2147483647 from bigint_r", new String [][] { {"-2147483645"} } },
	{ "select 2147483647 * y from bigint_r", new String [][] { {"4294967294"} } },
	{ "select y * 2147483647 from bigint_r", new String [][] { {"4294967294"} } },
	{ "select 2147483647 / y from bigint_r", new String [][] { {"1073741823"} } },
	{ "select y / 2147483647 from bigint_r", new String [][] { {"0"} } },
	// test precedence and associativity
	{ "create table r (x int)", null },
	{ "insert into r values (1)", null },
	{ "select 2 + 3 * 4 from r", new String [][] { {"14"} } },
	{ "select (2 + 3) * 4 from r", new String [][] { {"20"} } },
	{ "select 3 * 4 + 2 from r", new String [][] { {"14"} } },
	{ "select 3 * (4 + 2) from r", new String [][] { {"18"} } },
	{ "select 2 - 3 * 4 from r", new String [][] { {"-10"} } },
	{ "select (2 - 3) * 4 from r", new String [][] { {"-4"} } },
	{ "select 3 * 4 - 2 from r", new String [][] { {"10"} } },
	{ "select 3 * (4 - 2) from r", new String [][] { {"6"} } },
	{ "select 4 + 3 / 2 from r", new String [][] { {"5"} } },
	{ "select (4 + 3) / 2 from r", new String [][] { {"3"} } },
	{ "select 3 / 2 + 4 from r", new String [][] { {"5"} } },
	{ "select 3 / (2 + 4) from r", new String [][] { {"0"} } },
	{ "select 4 - 3 / 2 from r", new String [][] { {"3"} } },
	{ "select (4 - 3) / 2 from r", new String [][] { {"0"} } },
	{ "select 1 + 2147483647 - 2 from r", "22003" },
	{ "select 1 + (2147483647 - 2) from r", new String [][] { {"2147483646"} } },
	{ "select 4 * 3 / 2 from r", new String [][] { {"6"} } },
	{ "select 4 * (3 / 2) from r", new String [][] { {"4"} } },
	// Test associativity of unary - versus the binary operators
	{ "select -(1 + 2) from r", new String [][] { {"-3"} } },
	{ "select -(1 - 2) from r", new String [][] { {"1"} } },
	// FIXME
	// This also gets 22003 overflow error
	//{ "select -1073741824 * 2 from r", new String [][] { {"-2147483648"} } },
	{ "select -(1073741824 * 2) from r", "22003" },
	// This should not get an overflow
	// FIXME - it does!
	//{ "select -2147483648 / 2 from r", new String [][] { {"-1073741824"} } },
	{ "create table u (c1 int, c2 char(10))", null },
	{ "insert into u (c2) values 'asdf'", null },
	{ "insert into u (c1) values null", null },
	{ "insert into u (c1) values 1", null },
	{ "insert into u (c1) values null", null },
	{ "insert into u (c1) values 2", null },
	{ "select c1 + c1 from u", new String [][] { {null},{null},{"2"},{null},{"4"} } },
	{ "select c1 / c1 from u", new String [][] { {null},{null},{"1"},{null},{"1"} } },
	{ "select c1 + c2 from u", "22018" },
	{ "drop table t", null },
	{ "drop table s", null },
	{ "drop table r", null },
	{ "drop table u", null },
	{ "drop table smallint_t", null },
	{ "drop table smallint_s", null },
	{ "drop table smallint_r", null },
	{ "drop table bigint_t", null },
	{ "drop table bigint_s", null },
	{ "drop table bigint_r", null }
    };

    // Do not use partitioning as default, use replicate
    // (Some results are expected to be different with partitioning)
    skipDefaultPartitioned = true;

    Connection conn = TestUtil.getConnection();
    Statement stmt = conn.createStatement();
    // Go through the array, execute each string[0], check sqlstate [1]
    // This will fail on the first one that succeeds where it shouldn't
    // or throws unknown exception
    JDBC.SQLUnitTestHelper(stmt,Script_ArithmeticUT);
  }
  
  // This test is the script enhanced with partitioning
  public void testLangScript_ArithmeticWithPartitioning() throws Exception
  {
    // This form of the arithmetic.sql test has partitioning clauses
    Object[][] Script_ArithmeticUTPartitioning = {
	{ "create table t (i int, j int) partition by column(i)", null },
	{ "insert into t values (null, null)", null },
	{ "insert into t values (0, 100)", null },
	{ "insert into t values (1, 101)", null },
	{ "insert into t values (-2, -102)", null },
	{ "select i + j from t", new String [][] { {null},{"100"},{"102"},{"-104"} } },
	{ "select i, i + 10 + 20, j, j + 100 + 200 from t", new String [][] {
		{null,null,null,null},
		{"0","30","100","400"},
		{"1","31","101","401"},
		{"-2","28","-102","198"} } },
	{ "select i - j, j - i from t", new String [][] { {null,null}, {"-100","100"},
		{"-100","100"}, {"100","-100"} } },
	{ "select i, i - 10 - 20, 20 - 10 - i, j, j - 100 - 200, 200 - 100 - j from t", new String [][] {
		{null,null,null,null,null,null},
		{"0","-30","10","100","-200","0"},
		{"1","-29","9","101","-199","-1"},
		{"-2","-32","12","-102","-402","202"} } },
	{ "select i, j, i * j, j * i from t", new String [][] {
		{null,null,null,null},
		{"0","100","0","0"},
		{"1","101","101","101"},
		{"-2","-102","204","204"} } },
	{ "select i, j, i * 10 * -20, j * 100 * -200 from t", new String [][] {
		{null,null,null,null},
		{"0","100","0","-2000000"},
		{"1","101","-200","-2020000"},
		{"-2","-102","400","2040000"} } },
	{ "select -i, -j, -(i * 10 * -20), -(j * 100 * -200) from t", new String [][] {
		{null,null,null,null},
		{"0","-100","0","2000000"},
		{"-1","-101","200","2020000"},
		{"2","102","-400","-2040000"} } },
	{ "select +i, +j, +(+i * +10 * -20), +(+j * +100 * -200) from t", new String [][] {
		{null,null,null,null},
		{"0","100","0","-2000000"},
		{"1","101","-200","-2020000"},
		{"-2","-102","400","2040000"} } },
	// test null/null, constant/null, null/constant
	{ "select i, j, i / j, 10 / j, j / 10 from t", new String [][] {
		{null,null,null,null,null},
		{"0","100","0","0","10"},
		{"1","101","0","0","10"},
		{"-2","-102","0","0","-10"} } },
	// test for divide by 0
	{ "select j / i from t", "22012" },      // Math div by zero error during result set scan
	{ "select (j - 1) / (i + 4), 20 / 5 / 4, 20 / 4 / 5 from t", new String [][] {
		{null, "1","1"},
		{"24","1","1"},
		{"20","1","1"},
		{"-51","1","1"} } },
	{ "select j, j / (0 - j), (0 - j) / j, (0 - j) / (0 - j) from t", new String [][] {
		{null,null,null,null},
		{"100","-1","-1","1"},
		{"101","-1","-1","1"},
		{"-102","-1","-1","1"} } },
	{ "select i, i + 10, i - (10 - 20), i - 10, i - (20 - 10) from t", new String [][] {
		{null,null,null,null,null},
		{"0","10","10","-10","-10"},
		{"1","11","11","-9","-9"},
		{"-2","8","8","-12","-12"} } },
	{ "select 'The next 2 columns should agree', 2 + 3 * 4 + 5, 2 + (3 * 4) + 5 from t", new String [][] {
		{"The next 2 columns should agree","19","19"},
		{"The next 2 columns should agree","19","19"},
		{"The next 2 columns should agree","19","19"},
		{"The next 2 columns should agree","19","19"} } },
	{ "select 'The next column should be 45', (2 + 3) * (4 + 5) from t", new String [][] {
		{"The next column should be 45","45"},
		{"The next column should be 45","45"},
		{"The next column should be 45","45"},
		{"The next column should be 45","45"} } },
	{ "delete from t", null },
	{ "insert into t values (null, null)", null },
	{ "insert into t values (0, 100)", null },
	{ "insert into t values (1, 101)", null },
	{ "select i + 2147483647 from t", "22003" },
	{ "select i - 2147483647 - 1, 'This query should work' from t", new String [][] {
		{null, "This query should work"},
		{"-2147483648","This query should work"},
		{"-2147483647","This query should work"} } },
	{ "select i - 2147483647 - 2, 'This query should fail' from t", "22003" },
	{ "select j * 2147483647 from t", "22003" },
	{ "select j * -2147483647 from t", "22003" },
	{ "insert into t values (-2147483648, 0)", null },
	{ "select -i from t", "22003" },
	// test the arithmetic operators on a type we know they don't work on
	{ "create table s (x char(10), y char(10))partition by column(x)", null },
	{ "select x + y from s", "42Y95" },
	{ "select x - y from s", "42Y95" },
	{ "select x * y from s", "42Y95" },
	{ "select x / y from s", "42Y95" },
	{ "select -x from s", "42X37" },
	{ "create table smallint_t (i smallint, j smallint)partition by column(i)", null },
	{ "create table smallint_s (i smallint, j smallint)partition by column(i)", null },
	{ "insert into smallint_t values (null, null)", null },
	{ "insert into smallint_t values (0, 100)", null },
	{ "insert into smallint_t values (1, 101)", null },
	{ "insert into smallint_t values (-2, -102)", null },
	{ "select i + j from smallint_t", new String [][] { {null},{"100"},{"102"},{"-104"} } },
	{ "select i, j, i + i + j, j + j + i from smallint_t", new String [][] {
		{null,null,null,null},
		{"0","100","100","200"},
		{"1","101","103","203"},
		{"-2","-102","-106","-206"} } },
	{ "select i - j, j - i from smallint_t", new String [][] {
		{null,null},{"-100","100"},{"-100","100"},{"100","-100"} } },
	{ "select i, i - j - j, j - j - i, j, j - i - i, i - i - j from smallint_t", new String [][] {
		{null,null,null,null,null,null},
		{"0","-200","0","100","100","-100"},
		{"1","-201","-1","101","99","-101"},
		{"-2","202","2","-102","-98","102"} } },
	{ "select i, j, i * j, j * i from smallint_t", new String [][] {
		{null,null,null,null},
		{"0","100","0","0"},
		{"1","101","101","101"},
		{"-2","-102","204","204"} } },
	{ "select i, j, i * i * (i - j), j * i * (i - j) from smallint_t", new String [][] {
		{null,null,null,null},
		{"0","100","0","0"},
		{"1","101","-100","-10100"},
		{"-2","-102","400","20400"} } },
	{ "select -i, -j, -(i * i * (i - j)), -(j * i * (i - j)) from smallint_t", new String [][] {
		{null,null,null,null},
		{"0","-100","0","0"},
		{"-1","-101","100","10100"},
		{"2","102","-400","-20400"} } },
	{ "select j / i from smallint_t", "22012" },
	{ "insert into smallint_s values (1, 32767)", null },
	{ "select i + j from smallint_s", "22003" },
	{ "select i - j - j from smallint_s", "22003" },
	{ "select j + j from smallint_s", "22003" },
	{ "select j * j from smallint_s", "22003" },
	{ "insert into smallint_s values (-32768, 0)", null },
	{ "select -i from smallint_s", "22003" },
	// test mixed types: int and smallint
	{ "create table smallint_r (y smallint)partition by column(y)", null },
	{ "insert into smallint_r values (2)", null },
	{ "select 65535 + y from smallint_r", new String [][] { {"65537"} } },
	// FIXME
	// This throws datatype range exception 22003 (even though previous statement works!)
	// All these fail with Y on the LHS of the math operator
	//{ "select y + 65535 from smallint_r", new String [][] { {"65537"} } },
	{ "select 65535 - y from smallint_r", new String [][] { {"65533"} } },
	//{ "select y - 65535 from smallint_r", new String [][] { {"-65533"} } },
	{ "select 65535 * y from smallint_r", new String [][] { {"131070"} } },
	//{ "select y * 65535 from smallint_r", new String [][] { {"131070"} } },
	{ "select 65535 / y from smallint_r", new String [][] { {"32767"} } },
	//{ "select y / 65535 from smallint_r", new String [][] { {"0"} } },  // This shouldn't fail at all
	// do the same thing with bigints
	{ "create table bigint_t (i bigint, j bigint) partition by column(i)", null },
	{ "create table bigint_s (i bigint, j bigint) partition by column(i)", null },
	{ "insert into bigint_t values (null, null)", null },
	{ "insert into bigint_t values (0, 100)", null },
	{ "insert into bigint_t values (1, 101)", null },
	{ "insert into bigint_t values (-2, -102)", null },
	{ "select i + j from bigint_t", new String [][] { {null},{"100"},{"102"},{"-104"} } },
	{ "select i, j, i + i + j, j + j + i from bigint_t", new String [][] {
		{null,null,null,null},
		{"0","100","100","200"},
		{"1","101","103","203"},
		{"-2","-102","-106","-206"} } },
	{ "select i - j, j - i from bigint_t", new String [][] {
		{null,null}, {"-100","100"}, {"-100","100"}, {"100","-100"} } },
	{ "select i, i - j - j, j - j - i, j, j - i - i, i - i - j from bigint_t", new String [][] {
		{null,null,null,null,null,null},
		{"0","-200","0","100","100","-100"},
		{"1","-201","-1","101","99","-101"},
		{"-2","202","2","-102","-98","102"} } },
	{ "select i, j, i * j, j * i from bigint_t", new String [][] {
		{null,null,null,null},
		{"0","100","0","0"},
		{"1","101","101","101"},
		{"-2","-102","204","204"} } },
	{ "select i, j, i * i * (i - j), j * i * (i - j) from bigint_t", new String [][] {
		{null,null,null,null},
		{"0","100","0","0"},
		{"1","101","-100","-10100"},
		{"-2","-102","400","20400"} } },
	{ "select -i, -j, -(i * i * (i - j)), -(j * i * (i - j)) from bigint_t", new String [][] {
		{null,null,null,null},
		{"0","-100","0","0"},
		{"-1","-101","100","10100"},
		{"2","102","-400","-20400"} } },
	{ "select j / i from bigint_t", "22012" },
	{ "insert into bigint_s values (1, 9223372036854775807)", null },
	{ "select i + j from bigint_s", "22003" },
	{ "select i - j - j from bigint_s", "22003" },
	{ "select j + j from bigint_s", "22003" },
	{ "select j * j from bigint_s", "22003" },
	{ "select 2 * (9223372036854775807 / 2 + 1) from bigint_s", "22003" },
	{ "select -2 * (9223372036854775807 / 2 + 2) from bigint_s", "22003" },
	{ "select 2 * (-9223372036854775808 / 2 - 1) from bigint_s", "22003" },
	{ "select -2 * (-9223372036854775808 / 2 - 1) from bigint_s", "22003" },
	{ "insert into bigint_s values (-9223372036854775808, 0)", null },
	{ "select -i from bigint_s", "22003" },
	{ "select -j from bigint_s", new String [][] { {"-9223372036854775807"},{"0"} } },
	{ "select i / 2 * 2 + 1 from bigint_s", new String [][] { {"1"},{"-9223372036854775807"} } },
	{ "select j / 2 * 2 from bigint_s", new String [][] { {"9223372036854775806"}, {"0"} } },
	// test mixed types: int and bigint
	{ "create table bigint_r (y bigint) partition by column(y)", null },
	{ "insert into bigint_r values (2)", null },
	{ "select 2147483647 + y from bigint_r", new String [][] { {"2147483649"} } },
	{ "select y + 2147483647 from bigint_r", new String [][] { {"2147483649"} } },
	{ "select 2147483647 - y from bigint_r", new String [][] { {"2147483645"} } },
	{ "select y - 2147483647 from bigint_r", new String [][] { {"-2147483645"} } },
	{ "select 2147483647 * y from bigint_r", new String [][] { {"4294967294"} } },
	{ "select y * 2147483647 from bigint_r", new String [][] { {"4294967294"} } },
	{ "select 2147483647 / y from bigint_r", new String [][] { {"1073741823"} } },
	{ "select y / 2147483647 from bigint_r", new String [][] { {"0"} } },
	// test precedence and associativity
	{ "create table r (x int) partition by column(x)", null },
	{ "insert into r values (1)", null },
	{ "select 2 + 3 * 4 from r", new String [][] { {"14"} } },
	{ "select (2 + 3) * 4 from r", new String [][] { {"20"} } },
	{ "select 3 * 4 + 2 from r", new String [][] { {"14"} } },
	{ "select 3 * (4 + 2) from r", new String [][] { {"18"} } },
	{ "select 2 - 3 * 4 from r", new String [][] { {"-10"} } },
	{ "select (2 - 3) * 4 from r", new String [][] { {"-4"} } },
	{ "select 3 * 4 - 2 from r", new String [][] { {"10"} } },
	{ "select 3 * (4 - 2) from r", new String [][] { {"6"} } },
	{ "select 4 + 3 / 2 from r", new String [][] { {"5"} } },
	{ "select (4 + 3) / 2 from r", new String [][] { {"3"} } },
	{ "select 3 / 2 + 4 from r", new String [][] { {"5"} } },
	{ "select 3 / (2 + 4) from r", new String [][] { {"0"} } },
	{ "select 4 - 3 / 2 from r", new String [][] { {"3"} } },
	{ "select (4 - 3) / 2 from r", new String [][] { {"0"} } },
	{ "select 1 + 2147483647 - 2 from r", "22003" },
	{ "select 1 + (2147483647 - 2) from r", new String [][] { {"2147483646"} } },
	{ "select 4 * 3 / 2 from r", new String [][] { {"6"} } },
	{ "select 4 * (3 / 2) from r", new String [][] { {"4"} } },
	// Test associativity of unary - versus the binary operators
	{ "select -(1 + 2) from r", new String [][] { {"-3"} } },
	{ "select -(1 - 2) from r", new String [][] { {"1"} } },
	// FIXME
	// This also gets 22003 overflow error
	//{ "select -1073741824 * 2 from r", new String [][] { {"-2147483648"} } },
	{ "select -(1073741824 * 2) from r", "22003" },
	// This should not get an overflow
	// FIXME - it does!
	//{ "select -2147483648 / 2 from r", new String [][] { {"-1073741824"} } },
	{ "create table u (c1 int, c2 char(10)) partition by column(c1)", null },
	{ "insert into u (c2) values 'asdf'", null },
	{ "insert into u (c1) values null", null },
	{ "insert into u (c1) values 1", null },
	{ "insert into u (c1) values null", null },
	{ "insert into u (c1) values 2", null },
	{ "select c1 + c1 from u", new String [][] { {null},{null},{"2"},{null},{"4"} } },
	{ "select c1 / c1 from u", new String [][] { {null},{null},{"1"},{null},{"1"} } },
	{ "select c1 + c2 from u", "22018" },
	{ "drop table t", null },
	{ "drop table s", null },
	{ "drop table r", null },
	{ "drop table u", null },
	{ "drop table smallint_t", null },
	{ "drop table smallint_s", null },
	{ "drop table smallint_r", null },
	{ "drop table bigint_t", null },
	{ "drop table bigint_s", null },
	{ "drop table bigint_r", null }
    };

    Connection conn = TestUtil.getConnection();
    Statement stmt = conn.createStatement();
    // Go through the array, execute each string[0], check sqlstate [1]
    // This will fail on the first one that succeeds where it shouldn't
    // or throws unknown exception
    JDBC.SQLUnitTestHelper(stmt,Script_ArithmeticUTPartitioning);

  }
}
