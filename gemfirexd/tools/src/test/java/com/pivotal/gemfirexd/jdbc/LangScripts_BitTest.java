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

import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.sql.catalog.DistributionDescriptor;

import junit.framework.TestSuite;
import junit.textui.TestRunner;

public class LangScripts_BitTest extends JdbcTestBase {

  public static void main(String[] args) {
    TestRunner.run(new TestSuite(LangScripts_BitTest.class));
  }
  
  public LangScripts_BitTest(String name) {
    super(name); 
  }

  // This test is the as-is LangScript conversion, without any partitioning clauses
  public void testLangScript_BitNoPartitioning() throws Exception
  {
    // This is a JUnit conversion of the Derby Lang bit.sql script
    // without any GemFireXD extensions
	  
    // Catch exceptions from illegal syntax
    // Tests still not fixed marked FIXME
	  
    // Array of SQL text to execute and sqlstates to expect
    // The first object is a String, the second is either 
    // 1) null - this means query returns no rows and throws no exceptions
    // 2) a string - this means query returns no rows and throws expected SQLSTATE
    // 3) a String array - this means query returns rows which must match (unordered) given resultset
    //       - for an empty result set, an uninitialized size [0][0] array is used
    Object[][] Script_BitUT = {
      { "values(X'aAff')", new String[][]{ {"aaff"} } },
      { "values(cast (x'ee' as char(2) for bit data))", new String[][]{ {"ee20"} } },  // blankpad bit data
      { "values x'aAff' || (cast (x'ee' as char(2) for bit data))", new String[][]{ {"aaffee20"} } },  // blankpad bit data
      // Test bit operations outside of tables
      { "create table tab1 (c1 char(25))", null },  
      { "insert into tab1 values 'search condition is true'", null },  
      { "select * from tab1 where ((X'1010' || X'0011' || X'0100') = X'101000110100')", new String[][]{ {"search condition is true"} } },  
      { "select * from tab1 where (X'1100' > X'0011')", new String[][]{ {"search condition is true"} } },
      { "drop table tab1", null },
      { "values(X'gg')", "42X01" },
      { "values(X'z')", "42X01" },
      { "values(X'zz')", "42X01" },
      { "values(X'9')", "42606" },
      { "values({fn length(X'ab')} * 8)", "42846" },
      { "values({fn length(X'11')} * 8)", "42846" },
      { "create table t1 (b1 char for bit data, b2 char(2) for bit data, b3 varchar(2) for bit data, b4 LONG VARCHAR FOR BIT DATA, b5 LONG VARCHAR FOR BIT DATA, b6 LONG VARCHAR FOR BIT DATA)", null },
      { "drop table t1", null },
      { "create table t1 (b1 char for bit data, b2 char(1) for bit data not null, b3 varchar(1) for bit data not null, b4 LONG VARCHAR FOR BIT DATA not null, b5 LONG VARCHAR FOR BIT DATA not null, b6 LONG VARCHAR FOR BIT DATA not null)", null },
      { "drop table t1", null },
      { "create table t (i int, s smallint, c char(10), v varchar(50), d double precision, r real, b char (2) for bit data, bv varchar(8) for bit data, lbv LONG VARCHAR FOR BIT DATA)", null },
      // nullability and select of null
      { "insert into t values (null, null, null, null, null, null, null, null, null)", null },
      { "insert into t (i) values (null)", null },
      { "select b, bv, lbv from t", new String[][]{ {null,null,null},{null,null,null} } },
      { "insert into t values (0, 100, 'hello', 'everyone is here', 200.0e0, 200.0e0, X'12af', X'0000111100001111', X'abc123')", null},
      { "insert into t values (-1, -100, 'goodbye', 'everyone is there', -200.0e0, -200.0e0, X'0000', X'', X'10101010')", null },
      // Truncation test
      { "insert into t (b, bv) values (X'ffffffff', X'ffffffff')", "22001" },
      { "select b, bv, lbv from t", new String[][]{ {null,null,null},{null,null,null},{"12af","0000111100001111","abc123"},{"0000","","10101010"} } },
      { "insert into t (b, bv) values (X'01', X'01')", null },
      { "insert into t (b, bv) values (X'', X'')", null },
      { "select b, bv from t", new String[][]{ {null,null},{null,null},{"12af","0000111100001111"},{"0000",""},{"0120","01"},{"2020",""} } },
      { "drop table t", null},
      // Simple comparisons
      { "create table nulltab (b char(1) for bit data)", null },
      { "insert into nulltab values (null)", null },
      { "select 1 from nulltab where X'0001' > X'0000'", new String[][]{ {"1"} } },
      { "select 1 from nulltab where X'0100' > X'0001'", new String[][]{ {"1"} } },
      { "select 1 from nulltab where X'ff00' > X'00ff'", new String[][]{ {"1"} } },
      { "select 1 from nulltab where X'0100' > X'0100'", new String[0][0] },
      { "select 1 from nulltab where X'0100' > b", new String[0][0] },
      { "select 1 from nulltab where X'0001' >= X'0000'", new String[][]{ {"1"} } },
      { "select 1 from nulltab where X'0100' >= X'0001'", new String[][]{ {"1"} } },
      { "select 1 from nulltab where X'ff00' >= X'00ff'", new String[][]{ {"1"} } },
      { "select 1 from nulltab where X'0100' >= b", new String[0][0] },
      { "select 1 from nulltab where X'0001' < X'0000'", new String[0][0] },
      { "select 1 from nulltab where X'0100' < X'0001'", new String[0][0] },
      { "select 1 from nulltab where X'ff00' < X'00ff'", new String[0][0] },
      { "select 1 from nulltab where X'0100' < b", new String[0][0] },
      { "select 1 from nulltab where X'0001' <= X'0000'", new String[0][0] },
      { "select 1 from nulltab where X'0100' <= X'0001'", new String[0][0] },
      { "select 1 from nulltab where X'ff00' <= X'00ff'", new String[0][0] },
      { "select 1 from nulltab where X'0100' <= b", new String[0][0] },
      { "drop table nulltab", null},
      // Select comparisons - use relational instead of equality for bit data but TODO for GemFireXD try equality as well
      { "create table t (b10 char(20) for bit data, vb10 varchar(20) for bit data, b16 char(2) for bit data, vb16 varchar(2) for bit data, lbv LONG VARCHAR FOR BIT DATA, c20 char(20), cv20 varchar(20))", null},
      { "insert into t values (null, null, null, null, null, 'null', 'null columns')", null },
      { "insert into t values (X'',  X'',  X'',  X'', X'', '0', 'zero length column')", null },
      { "insert into t values (X'0000000001', X'0000000001', X'01', X'01', X'0000000001', '1', '1')", null },
      { "insert into t values (X'0000000011', X'0000000011', X'03', X'03', X'03', '3', '3')", null },
      { "insert into t values (X'1111111111', X'1111111111', X'ff', X'ff', X'1111111111', 'ff', 'ff')", null },
      { "insert into t values (X'11', X'11', X'aa', X'aa', X'aa', 'aa', 'aa')", null },
      { "select {fn length(cast(b10 as char(10)))} from t where b10 is not null", "42846" },
      { "select {fn length(cast(vb10 as char(10)))} from t where vb10 is not null", "42846" },
      { "select {fn length(cast(lbv as char(10)))} from t where vb10 is not null", "42846" },
      // All result set comparisons are against trimmed strings
      { "select b10, c20, cv20 from t order by b10 asc", new String[][]{ 
                 {"0000000001202020202020202020202020202020","1","1"},
                 {"0000000011202020202020202020202020202020","3","3"}, 
                 {"1111111111202020202020202020202020202020","ff","ff"}, 
                 {"1120202020202020202020202020202020202020","aa","aa"}, 
                 {"2020202020202020202020202020202020202020","0","zero length column"}, 
                 {null,"null","null columns"} } },  
      { "select b10, c20, cv20 from t order by b10 desc", new String[][]{ 
                 {null,"null","null columns"},  
                 {"2020202020202020202020202020202020202020","0","zero length column"}, 
                 {"1120202020202020202020202020202020202020","aa","aa"}, 
                 {"1111111111202020202020202020202020202020","ff","ff"}, 
                 {"0000000011202020202020202020202020202020","3","3"}, 
                 {"0000000001202020202020202020202020202020","1","1"} } },
      { "select vb10, c20, cv20 from t order by vb10", new String[][]{
                 {"","0","zero length column"},
                 {"0000000001","1","1"},
                 {"0000000011","3","3"},
                 {"11","aa","aa"},
                 {"1111111111","ff","ff"},
                 {null,"null","null columns"} } },        
      { "select b16, c20, cv20 from t order by b16", new String[][]{
                 {"0120","1","1"},
                 {"0320","3","3"},
                 {"2020","0","zero length column"},
                 {"aa20","aa","aa"},
                 {"ff20","ff","ff"},
                 {null,"null","null columns"} } },
      { "select vb16, c20, cv20 from t order by vb16", new String[][]{
                 {"","0","zero length column"},
                 {"01","1","1"},
                 {"03","3","3"},
                 {"aa","aa","aa"},
                 {"ff","ff","ff"},
                 {null,"null","null columns"} } },
      { "select vb16, c20, cv20, lbv from t order by lbv","X0X67"},
      { "select b10 from t where b10 > X'0000000010'", new String[][]{
                 {"2020202020202020202020202020202020202020"},
                 {"0000000011202020202020202020202020202020"},
                 {"1111111111202020202020202020202020202020"},
                 {"1120202020202020202020202020202020202020"} } },
      { "select b10 from t where b10 < X'0000000010'", new String[][]{ {"0000000001202020202020202020202020202020"} } },
      { "select b10 from t where b10 <= X'0000000011'", new String[][]{ 
                 {"0000000001202020202020202020202020202020"},{"0000000011202020202020202020202020202020"} } },
      { "select b10 from t where b10 >= X'0000000011'", new String[][]{ 
                 {"2020202020202020202020202020202020202020"},{"0000000011202020202020202020202020202020"},
                 {"1111111111202020202020202020202020202020"},{"1120202020202020202020202020202020202020"} } },
      { "select b10 from t where b10 <> X'0000000011'", new String[][]{ 
                 {"2020202020202020202020202020202020202020"},{"0000000001202020202020202020202020202020"},
                 {"1111111111202020202020202020202020202020"},{"1120202020202020202020202020202020202020"} } },
      { "select vb10 from t where vb10 > X'0000000010'", new String[][]{
                 {"0000000011"},{"1111111111"},{"11"} } },                                      
      { "select vb10 from t where vb10 < X'0000000010'", new String[][]{
                 {""},{"0000000001"} } },                         
      { "select vb10 from t where vb10 <= X'0000000011'", new String[][]{
                 {""},{"0000000001"},{"0000000011"} } },                             
      { "select vb10 from t where vb10 >= X'0000000011'", new String[][]{
                 {"0000000011"},{"1111111111"},{"11"} } },                                      
      { "select vb10 from t where vb10 <> X'0000000011'", new String[][]{
                 {""},{"0000000001"},{"1111111111"},{"11"} } },                             
      { "select b16 from t where b16 > X'0000000010'", new String[][]{
                 {"2020"},{"0120"},{"0320"},{"ff20"},{"aa20"} } },
      { "select b16 from t where b16 < X'0000000010'", new String[0][0] },   // empty result set
      { "select b16 from t where b16 <= X'0000000011'", new String[0][0] },   // empty result set
      { "select b16 from t where b16 >= X'0000000011'", new String[][]{
                 {"2020"},{"0120"},{"0320"},{"ff20"},{"aa20"} } },
      { "select b16 from t where b16 <> X'0000000011'", new String[][]{
                 {"2020"},{"0120"},{"0320"},{"ff20"},{"aa20"} } },
      { "select vb16 from t where vb16 > X'0000000010'", new String[][]{
                 {"01"},{"03"},{"ff"},{"aa"} } },
      { "select vb16 from t where vb16 <= X'0000000011'", new String[][]{ {""} } },
      { "select vb16 from t where vb16 >= X'0000000011'", new String[][]{
                 {"01"},{"03"},{"ff"},{"aa"} } },
      { "select vb16 from t where vb16 <> X'0000000011'", new String[][]{
                 {""},{"01"},{"03"},{"ff"},{"aa"} } },
      { "select lbv from t where lbv > X'0000000010'", "42818" },
      { "select lbv from t where lbv < X'0000000010'", "42818" },
      { "select lbv from t where lbv <= X'0000000011'", "42818" },
      { "select lbv from t where lbv >= X'0000000011'", "42818" },
      { "select lbv from t where lbv <> X'0000000011'", "42818" },
      { "select b10, vb10||X'11' from t where vb10||X'11' > b10", new String[0][0] },
      { "select b10, X'11'||vb10 from t where X'11'||vb10 > b10", new String[][] {
             {"0000000001202020202020202020202020202020","110000000001"},                              
             {"0000000011202020202020202020202020202020","110000000011"} } },                              
      { "select b16, vb16||X'11' from t where vb16||X'11' > b16", new String[0][0] },
      { "select b10 || vb10 from t", new String[][] {
             {null},{"2020202020202020202020202020202020202020"},{"00000000012020202020202020202020202020200000000001"},
             {"00000000112020202020202020202020202020200000000011"},{"11111111112020202020202020202020202020201111111111"},
             {"112020202020202020202020202020202020202011"} } }, 
      { "select lbv || b10 from t", new String[][] {
             {null},{"2020202020202020202020202020202020202020"},{"00000000010000000001202020202020202020202020202020"}, 
             {"030000000011202020202020202020202020202020"},{"11111111111111111111202020202020202020202020202020"},
             {"aa1120202020202020202020202020202020202020"} } }, 
      { "select b10 || lbv from t", new String[][] {
             {null},{"2020202020202020202020202020202020202020"},{"00000000012020202020202020202020202020200000000001"},
             {"000000001120202020202020202020202020202003"},{"11111111112020202020202020202020202020201111111111"},
             {"1120202020202020202020202020202020202020aa"} } }, 
      { "select lbv || vb10 from t", new String[][] {
             {null},{""},{"00000000010000000001"},{"030000000011"},{"11111111111111111111"},{"aa11"} } },
      { "select vb10 || lbv from t", new String[][] {
             {null},{""},{"00000000010000000001"},{"000000001103"},{"11111111111111111111"},{"11aa"} } },   
      { "select t1.b10 from t t1, t t2 where t1.b10 > t2.b10", new String[][]{
             {"2020202020202020202020202020202020202020"},{"2020202020202020202020202020202020202020"},
             {"2020202020202020202020202020202020202020"},{"2020202020202020202020202020202020202020"},
             {"0000000011202020202020202020202020202020"},{"1111111111202020202020202020202020202020"},
             {"1111111111202020202020202020202020202020"},{"1120202020202020202020202020202020202020"},
             {"1120202020202020202020202020202020202020"},{"1120202020202020202020202020202020202020"} } },
      // Functions
      { "select {fn length(b10)} from t", "42846" },
      { "select {fn length(vb10)} from t", "42846" },
      { "select {fn length(lbv)} from t", "42846" },
      { "select {fn length(c20)} from t", new String[][] { {"4"},{"1"},{"1"},{"1"},{"2"},{"2"} } },
      { "select {fn length(cv20)} from t", new String[][] { {"12"},{"18"},{"1"},{"1"},{"2"},{"2"} } },
      { "drop table t", null },
      // Test normalization
      { "create table t1 (c1 char(2) for bit data)", null},
      { "insert into t1 values (X'0001')", null },
      { "insert into t1 values (X'0010')", null },
      { "insert into t1 values (X'0011')", null },
      { "select * from t1", new String[][]{ {"0001"},{"0010"},{"0011"} } },
      // Now insert something that needs to be expanded
      { "insert into t1 values (X'11')", null},
      { "select * from t1", new String[][]{ {"0001"},{"0010"},{"0011"},{"1120"} } },
      // Insert select, expand 1 byte
      { "create table t2 (c1 char(3) for bit data)", null},
      { "insert into t2 select c1 from t1", null},
      { "select * from t2", new String[][]{ {"000120"},{"001020"},{"001120"},{"112020"} } },
      { "drop table t2", null },
      // Insert select, expand many bytes
      { "create table t2 (c1 char(20) for bit data)", null },
      { "insert into t2 select c1 from t1", null },
      { "select * from t2", new String[][]{
            {"0001202020202020202020202020202020202020"},{"0010202020202020202020202020202020202020"},
            {"0011202020202020202020202020202020202020"},{"1120202020202020202020202020202020202020"} } },
      { "drop table t2", null },
      { "drop table t1", null },
      // Extra truncation tests
      { "create table t1 (b1 char(1) for bit data)", null },
      { "insert into t1 values (X'11')", null },
      { "insert into t1 values (X'10')", null },
      { "insert into t1 values (X'11')", null },
      { "insert into t1 values (X'1000')", "22001" },
      { "insert into t1 values (X'100000')", "22001" },
      { "insert into t1 values (X'10000000')", "22001" },
      { "insert into t1 values (X'1000000000')", "22001" },
      { "insert into t1 values (X'100001')", "22001" },
      { "insert into t1 values (X'0001')", "22001" },
      { "insert into t1 values (X'8001')", "22001" },
      { "insert into t1 values (X'8000')", "22001" },
      { "drop table t1", null },
      { "create table t1 (b9 char(2) for bit data)", null },
      { "insert into t1 values (X'1111')", null },
      { "insert into t1 values (X'111100')", "22001" },
      { "insert into t1 values (X'11110000')", "22001" },
      { "insert into t1 values (X'1111000000')", "22001" },
      { "insert into t1 values (X'1111111100000000')", "22001" },
      { "insert into t1 values (X'1111111111')", "22001" },
      { "insert into t1 values (X'11111111100001')", "22001" },
      { "insert into t1 values (X'0001')", null },
      { "insert into t1 values (X'8001')", null },
      { "insert into t1 values (X'8000')", null },
      { "drop table t1", null },
      { "create table t1 (b3 char(2) for bit data, b7 char(4) for bit data, b8 char (5) for bit data, b15 char(8) for bit data, b16 char(9) for bit data)", null },
      { "insert into t1 values ( X'1111', X'11111111', X'1111111111', X'1111111111111111', X'111111111111111111')", null },
      { "insert into t1 values ( X'1110', X'11111110', X'11111111', X'1111111111111110', X'1111111111111111')", null },
      { "insert into t1 values ( null, null, X'111111111110', null, null)", "22001" },
      { "insert into t1 values ( null, X'1111111100', null, null, null)", "22001" },
      { "insert into t1 values ( null, X'1111111111', null, null, null)", "22001" },
      { "insert into t1 values ( null, null, null, X'111111111111111100', null)", "22001" },
      { "insert into t1 values ( null, null, null, X'111111111111111111', null)", "22001" },
      { "insert into t1 values ( null, null, null, null, X'11111111111111111110')", "22001" },
      // autocommit off here in original sql script - needed?
      // bug 5160 - incorrect typing of VALUES table constructor on an insert;
      { "create table iv (id int, vc varchar(12))", null },
      { "insert into iv values (1, 'abc'), (2, 'defghijk'), (3, 'lmnopqrstcc')", null },
      { "insert into iv values (4, null), (5, 'null ok?'), (6, '2blanks  ')", null },
      { "insert into iv values (7, 'dddd'), (8, '0123456789123'), (9, 'too long')", "22001" },
      { "select id, vc, {fn length(vc)} AS LEN from iv order by 1", new String[][] {
            {"1","abc","3"}, {"2","defghijk","8"}, {"3", "lmnopqrstcc","11"},
            {"4",null,null},{"5","null ok?","8"},{"6","2blanks","7"} } },
      { "insert into iv select * from (values (10, 'pad'), (11, 'pad me'), (12, 'anakin jedi')) as t(i, c)", null}, 
      { "select id, vc, {fn length(vc)} AS LEN from iv order by 1", new String[][] {
            {"1","abc","3"}, {"2","defghijk","8"}, {"3", "lmnopqrstcc","11"},
            {"4",null,null},{"5","null ok?","8"},{"6","2blanks","7"},
            {"10","pad","3"}, {"11","pad me","6"},{"12","anakin jedi","11"} } },   // I see what you did there
      // check values outside of table constructors retain their CHARness
      { "select c, {fn length(c)} AS LEN from (values (1, 'abc'), (2, 'defghijk'), (3, 'lmnopqrstcc')) as t(i, c)", new String[][] {
            {"abc","3"}, {"defghijk","8"}, {"lmnopqrstcc","11"} } },
      { "drop table iv", null},
      { "create table bv (id int, vb varchar(16) for bit data)", null},
      { "insert into bv values (1, X'1a'), (2, X'cafebabe'), (3, null)", null},
      { "select id, vb, {fn length(vb)} AS LEN from bv order by 1", "42846"},
      { "drop table bv", null},
      { "create table dv (id int, vc varchar(12))", null},
      { "insert into dv values (1, 1.2), (2, 34.5639), (3, null)", "42X61"},  // not-union-compatible error??
      { "insert into dv values (1, '1.2'), (2, '34.5639'), (3, null)", null},
      { "select id, vc from dv order by 1", new String[][] { {"1","1.2"},{"2","34.5639"},{"3",null} } },
      { "drop table dv", null},
      // bug 5306 -- incorrect padding of VALUES table constructor on an insert
      { "create table bitTable (id int, bv LONG VARCHAR FOR BIT DATA)", null},
      { "insert into bitTable values (1, X'031'), (2, X'032'), (3, X'')", "42606"},
      { "insert into bitTable values (4, null), (5, X'033'), (6, X'2020')", "42606"},
      { "select id, bv, {fn length(bv)} as LEN from bitTable order by 1", "42846"},
      { "insert into bitTable select * from (values (10, 'pad'), (11, 'pad me'), (12, 'anakin jedi')) as t(i, c)", "42821"}, 
      { "select id, bv, {fn length(bv)} AS LEN from bitTable order by 1", "42846" },
      { "drop table bitTable", null},
      { "create table charTable (id int, cv long varchar)", null},
      { "insert into charTable values (1, x'0101'), (2, x'00101100101001'), (3, x'')", "42821"},
      { "insert into charTable values (4, null), (5, x'1010101111'), (6, x'1000')", "42X61"},
      { "select id, cv, {fn length(cv)} as LEN from charTable order by 1", new String[0][0] },  //empty results
      { "insert into charTable select * from (values (10, x'001010'), (11, x'01011010101111'), (12, x'0101010101000010100101110101')) as t(i, c)", "42821"},
      { "select id, cv, {fn length(cv)} as LEN from charTable order by 1", new String[0][0] },  //empty results
      { "drop table charTable", null},
      // union tests
      { "create table pt5 (b5 char(2) for bit data)", null},
      { "create table pt10 (b10 char (4) for bit data)", null},
      { "insert into pt10 values (x'01000110')", null},
      { "insert into pt5 values (x'1010')", null},
      { "select {fn length(CM)} from (select b5 from pt5 union all select b10 from pt10) as t(CM)", "42846"},
      { "drop table pt5", null },
      { "drop table pt10", null },
      { "create table t5612 (c1 char(10), c2 varchar(10), c3 long  varchar)", null},
      { "insert into t5612 values (X'00680069', X'00680069', X'00680069')", "42821"},
      { "select * from t5612", new String[0][0]},   // empty results
      { "values cast(X'00680069' as char(30)), cast(X'00680069' as varchar(30)), cast(X'00680069' as long varchar)", "42846"}
      // FIXME : GemFireXD does not support CASCADE DELETE which this test depends upon - reenable later
/*      { "create table npetest1 (col1 varchar(36) for bit data not null, constraint pknpe1 primary key (col1))", null},
      { "create table npetest2 (col2 varchar(36) for bit data, constraint fknpe1 foreign key (col2) references npetest1(col1) on delete cascade)", null},  // GemFireXD does not support cascade delete - will throw 0A000
      { "insert into npetest1 (col1) values (X'0000000001')", null},
      { "insert into npetest1 (col1) values (X'0000000002')", null},
      { "insert into npetest1 (col1) values (X'0000000003')", null},
      { "insert into npetest2 (col2) values (X'0000000001')", null},
      { "insert into npetest2 (col2) values (NULL)", null},
      { "insert into npetest2 (col2) values (X'0000000002')", null},
      { "select col1 from npetest1 where col1 not in (select col2 from npetest2)", new String[0][0] },
      { "select col1 from npetest1 where col1 not in (select col2 from npetest2 where col2 is not null)", 
                       new String[][] { {"0000000003"} } },
      { "drop table npetest2", null},
      { "drop table npetest1", null} */
    };

    // Do not use partitioning as default, use replicate
    // (Some results are expected to be different with partitioning)
    skipDefaultPartitioned = true;

    Connection conn = TestUtil.getConnection();
    conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
    conn.setAutoCommit(true);
    Statement stmt = conn.createStatement();
    // Go through the array, execute each string[0], check sqlstate [1]
    // This will fail on the first one that succeeds where it shouldn't
    // or throws unknown exception
    JDBC.SQLUnitTestHelper(stmt,Script_BitUT);
    conn.commit();
  }
  
  // This test is the script enhanced with partitioning
  public void testLangScript_BitWithPartitioning() throws Exception
  {
    // This form of the Bit.sql script is enhanced with partitioning clauses
    Object[][] Script_BitUTPartitioning = {
      { "values(X'aAff')", new String[][]{ {"aaff"} } },
      { "values(cast (x'ee' as char(2) for bit data))", new String[][]{ {"ee20"} } },  // blankpad bit data
      { "values x'aAff' || (cast (x'ee' as char(2) for bit data))", new String[][]{ {"aaffee20"} } },  // blankpad bit data
      // Test bit operations outside of tables
      { "create table tab1 (c1 char(25)) partition by column(c1)", null },  
      { "insert into tab1 values 'search condition is true'", null },  
      { "select * from tab1 where ((X'1010' || X'0011' || X'0100') = X'101000110100')", new String[][]{ {"search condition is true"} } },  
      { "select * from tab1 where (X'1100' > X'0011')", new String[][]{ {"search condition is true"} } },
      { "drop table tab1", null },
      { "values(X'gg')", "42X01" },
      { "values(X'z')", "42X01" },
      { "values(X'zz')", "42X01" },
      { "values(X'9')", "42606" },
      { "values({fn length(X'ab')} * 8)", "42846" },
      { "values({fn length(X'11')} * 8)", "42846" },
      { "create table t1 (b1 char for bit data, b2 char(2) for bit data, b3 varchar(2) for bit data, b4 LONG VARCHAR FOR BIT DATA, b5 LONG VARCHAR FOR BIT DATA, b6 LONG VARCHAR FOR BIT DATA) partition by column(b1)", null },
      { "drop table t1", null },
      { "create table t1 (b1 char for bit data, b2 char(1) for bit data not null, b3 varchar(1) for bit data not null, b4 LONG VARCHAR FOR BIT DATA not null, b5 LONG VARCHAR FOR BIT DATA not null, b6 LONG VARCHAR FOR BIT DATA not null) partition by column(b1)", null },
      { "drop table t1", null },
      { "create table t (i int, s smallint, c char(10), v varchar(50), d double precision, r real, b char (2) for bit data, bv varchar(8) for bit data, lbv LONG VARCHAR FOR BIT DATA) partition by column(i)", null },
      // nullability and select of null
      { "insert into t values (null, null, null, null, null, null, null, null, null)", null },
      { "insert into t (i) values (null)", null },
      { "select b, bv, lbv from t", new String[][]{ {null,null,null},{null,null,null} } },
      { "insert into t values (0, 100, 'hello', 'everyone is here', 200.0e0, 200.0e0, X'12af', X'0000111100001111', X'abc123')", null},
      { "insert into t values (-1, -100, 'goodbye', 'everyone is there', -200.0e0, -200.0e0, X'0000', X'', X'10101010')", null },
      // Truncation test
      { "insert into t (b, bv) values (X'ffffffff', X'ffffffff')", "22001" },
      { "select b, bv, lbv from t", new String[][]{ {null,null,null},{null,null,null},{"12af","0000111100001111","abc123"},{"0000","","10101010"} } },
      { "insert into t (b, bv) values (X'01', X'01')", null },
      { "insert into t (b, bv) values (X'', X'')", null },
      { "select b, bv from t", new String[][]{ {null,null},{null,null},{"12af","0000111100001111"},{"0000",""},{"0120","01"},{"2020",""} } },
      { "drop table t", null},
      // Simple comparisons
      { "create table nulltab (b char(1) for bit data) partition by column(b)", null },
      { "insert into nulltab values (null)", null },
      { "select 1 from nulltab where X'0001' > X'0000'", new String[][]{ {"1"} } },
      { "select 1 from nulltab where X'0100' > X'0001'", new String[][]{ {"1"} } },
      { "select 1 from nulltab where X'ff00' > X'00ff'", new String[][]{ {"1"} } },
      { "select 1 from nulltab where X'0100' > X'0100'", new String[0][0] },
      { "select 1 from nulltab where X'0100' > b", new String[0][0] },
      { "select 1 from nulltab where X'0001' >= X'0000'", new String[][]{ {"1"} } },
      { "select 1 from nulltab where X'0100' >= X'0001'", new String[][]{ {"1"} } },
      { "select 1 from nulltab where X'ff00' >= X'00ff'", new String[][]{ {"1"} } },
      { "select 1 from nulltab where X'0100' >= b", new String[0][0] },
      { "select 1 from nulltab where X'0001' < X'0000'", new String[0][0] },
      { "select 1 from nulltab where X'0100' < X'0001'", new String[0][0] },
      { "select 1 from nulltab where X'ff00' < X'00ff'", new String[0][0] },
      { "select 1 from nulltab where X'0100' < b", new String[0][0] },
      { "select 1 from nulltab where X'0001' <= X'0000'", new String[0][0] },
      { "select 1 from nulltab where X'0100' <= X'0001'", new String[0][0] },
      { "select 1 from nulltab where X'ff00' <= X'00ff'", new String[0][0] },
      { "select 1 from nulltab where X'0100' <= b", new String[0][0] },
      { "drop table nulltab", null},
      // Select comparisons - use relational instead of equality for bit data but TODO for GemFireXD try equality as well
      { "create table t (b10 char(20) for bit data, vb10 varchar(20) for bit data, b16 char(2) for bit data, vb16 varchar(2) for bit data, lbv LONG VARCHAR FOR BIT DATA, c20 char(20), cv20 varchar(20)) partition by column(b10)", null},
      { "insert into t values (null, null, null, null, null, 'null', 'null columns')", null },
      { "insert into t values (X'',  X'',  X'',  X'', X'', '0', 'zero length column')", null },
      { "insert into t values (X'0000000001', X'0000000001', X'01', X'01', X'0000000001', '1', '1')", null },
      { "insert into t values (X'0000000011', X'0000000011', X'03', X'03', X'03', '3', '3')", null },
      { "insert into t values (X'1111111111', X'1111111111', X'ff', X'ff', X'1111111111', 'ff', 'ff')", null },
      { "insert into t values (X'11', X'11', X'aa', X'aa', X'aa', 'aa', 'aa')", null },
      { "select {fn length(cast(b10 as char(10)))} from t where b10 is not null", "42846" },
      { "select {fn length(cast(vb10 as char(10)))} from t where vb10 is not null", "42846" },
      { "select {fn length(cast(lbv as char(10)))} from t where vb10 is not null", "42846" },
      // All result set comparisons are against trimmed strings
      { "select b10, c20, cv20 from t order by b10 asc", new String[][]{ 
                 {"0000000001202020202020202020202020202020","1","1"},
                 {"0000000011202020202020202020202020202020","3","3"}, 
                 {"1111111111202020202020202020202020202020","ff","ff"}, 
                 {"1120202020202020202020202020202020202020","aa","aa"}, 
                 {"2020202020202020202020202020202020202020","0","zero length column"}, 
                 {null,"null","null columns"} } },  
      { "select b10, c20, cv20 from t order by b10 desc", new String[][]{ 
                 {null,"null","null columns"},  
                 {"2020202020202020202020202020202020202020","0","zero length column"}, 
                 {"1120202020202020202020202020202020202020","aa","aa"}, 
                 {"1111111111202020202020202020202020202020","ff","ff"}, 
                 {"0000000011202020202020202020202020202020","3","3"}, 
                 {"0000000001202020202020202020202020202020","1","1"} } },
      { "select vb10, c20, cv20 from t order by vb10", new String[][]{
                 {"","0","zero length column"},
                 {"0000000001","1","1"},
                 {"0000000011","3","3"},
                 {"11","aa","aa"},
                 {"1111111111","ff","ff"},
                 {null,"null","null columns"} } },        
      { "select b16, c20, cv20 from t order by b16", new String[][]{
                 {"0120","1","1"},
                 {"0320","3","3"},
                 {"2020","0","zero length column"},
                 {"aa20","aa","aa"},
                 {"ff20","ff","ff"},
                 {null,"null","null columns"} } },
      { "select vb16, c20, cv20 from t order by vb16", new String[][]{
                 {"","0","zero length column"},
                 {"01","1","1"},
                 {"03","3","3"},
                 {"aa","aa","aa"},
                 {"ff","ff","ff"},
                 {null,"null","null columns"} } },
      { "select vb16, c20, cv20, lbv from t order by lbv","X0X67"},
      { "select b10 from t where b10 > X'0000000010'", new String[][]{
                 {"2020202020202020202020202020202020202020"},
                 {"0000000011202020202020202020202020202020"},
                 {"1111111111202020202020202020202020202020"},
                 {"1120202020202020202020202020202020202020"} } },
      { "select b10 from t where b10 < X'0000000010'", new String[][]{ {"0000000001202020202020202020202020202020"} } },
      { "select b10 from t where b10 <= X'0000000011'", new String[][]{ 
                 {"0000000001202020202020202020202020202020"},{"0000000011202020202020202020202020202020"} } },
      { "select b10 from t where b10 >= X'0000000011'", new String[][]{ 
                 {"2020202020202020202020202020202020202020"},{"0000000011202020202020202020202020202020"},
                 {"1111111111202020202020202020202020202020"},{"1120202020202020202020202020202020202020"} } },
      { "select b10 from t where b10 <> X'0000000011'", new String[][]{ 
                 {"2020202020202020202020202020202020202020"},{"0000000001202020202020202020202020202020"},
                 {"1111111111202020202020202020202020202020"},{"1120202020202020202020202020202020202020"} } },
      { "select vb10 from t where vb10 > X'0000000010'", new String[][]{
                 {"0000000011"},{"1111111111"},{"11"} } },                                      
      { "select vb10 from t where vb10 < X'0000000010'", new String[][]{
                 {""},{"0000000001"} } },                         
      { "select vb10 from t where vb10 <= X'0000000011'", new String[][]{
                 {""},{"0000000001"},{"0000000011"} } },                             
      { "select vb10 from t where vb10 >= X'0000000011'", new String[][]{
                 {"0000000011"},{"1111111111"},{"11"} } },                                      
      { "select vb10 from t where vb10 <> X'0000000011'", new String[][]{
                 {""},{"0000000001"},{"1111111111"},{"11"} } },                             
      { "select b16 from t where b16 > X'0000000010'", new String[][]{
                 {"2020"},{"0120"},{"0320"},{"ff20"},{"aa20"} } },
      { "select b16 from t where b16 < X'0000000010'", new String[0][0] },   // empty result set
      { "select b16 from t where b16 <= X'0000000011'", new String[0][0] },   // empty result set
      { "select b16 from t where b16 >= X'0000000011'", new String[][]{
                 {"2020"},{"0120"},{"0320"},{"ff20"},{"aa20"} } },
      { "select b16 from t where b16 <> X'0000000011'", new String[][]{
                 {"2020"},{"0120"},{"0320"},{"ff20"},{"aa20"} } },
      { "select vb16 from t where vb16 > X'0000000010'", new String[][]{
                 {"01"},{"03"},{"ff"},{"aa"} } },
      { "select vb16 from t where vb16 <= X'0000000011'", new String[][]{ {""} } },
      { "select vb16 from t where vb16 >= X'0000000011'", new String[][]{
                 {"01"},{"03"},{"ff"},{"aa"} } },
      { "select vb16 from t where vb16 <> X'0000000011'", new String[][]{
                 {""},{"01"},{"03"},{"ff"},{"aa"} } },
      { "select lbv from t where lbv > X'0000000010'", "42818" },
      { "select lbv from t where lbv < X'0000000010'", "42818" },
      { "select lbv from t where lbv <= X'0000000011'", "42818" },
      { "select lbv from t where lbv >= X'0000000011'", "42818" },
      { "select lbv from t where lbv <> X'0000000011'", "42818" },
      { "select b10, vb10||X'11' from t where vb10||X'11' > b10", new String[0][0] },
      { "select b10, X'11'||vb10 from t where X'11'||vb10 > b10", new String[][] {
             {"0000000001202020202020202020202020202020","110000000001"},                              
             {"0000000011202020202020202020202020202020","110000000011"} } },                              
      { "select b16, vb16||X'11' from t where vb16||X'11' > b16", new String[0][0] },
      { "select b10 || vb10 from t", new String[][] {
             {null},{"2020202020202020202020202020202020202020"},{"00000000012020202020202020202020202020200000000001"},
             {"00000000112020202020202020202020202020200000000011"},{"11111111112020202020202020202020202020201111111111"},
             {"112020202020202020202020202020202020202011"} } }, 
      { "select lbv || b10 from t", new String[][] {
             {null},{"2020202020202020202020202020202020202020"},{"00000000010000000001202020202020202020202020202020"}, 
             {"030000000011202020202020202020202020202020"},{"11111111111111111111202020202020202020202020202020"},
             {"aa1120202020202020202020202020202020202020"} } }, 
      { "select b10 || lbv from t", new String[][] {
             {null},{"2020202020202020202020202020202020202020"},{"00000000012020202020202020202020202020200000000001"},
             {"000000001120202020202020202020202020202003"},{"11111111112020202020202020202020202020201111111111"},
             {"1120202020202020202020202020202020202020aa"} } }, 
      { "select lbv || vb10 from t", new String[][] {
             {null},{""},{"00000000010000000001"},{"030000000011"},{"11111111111111111111"},{"aa11"} } },
      { "select vb10 || lbv from t", new String[][] {
             {null},{""},{"00000000010000000001"},{"000000001103"},{"11111111111111111111"},{"11aa"} } },   
      { "select t1.b10 from t t1, t t2 where t1.b10 > t2.b10", new String[][]{
             {"2020202020202020202020202020202020202020"},{"2020202020202020202020202020202020202020"},
             {"2020202020202020202020202020202020202020"},{"2020202020202020202020202020202020202020"},
             {"0000000011202020202020202020202020202020"},{"1111111111202020202020202020202020202020"},
             {"1111111111202020202020202020202020202020"},{"1120202020202020202020202020202020202020"},
             {"1120202020202020202020202020202020202020"},{"1120202020202020202020202020202020202020"} } },
      // Functions
      { "select {fn length(b10)} from t", "42846" },
      { "select {fn length(vb10)} from t", "42846" },
      { "select {fn length(lbv)} from t", "42846" },
      { "select {fn length(c20)} from t", new String[][] { {"4"},{"1"},{"1"},{"1"},{"2"},{"2"} } },
      { "select {fn length(cv20)} from t", new String[][] { {"12"},{"18"},{"1"},{"1"},{"2"},{"2"} } },
      { "drop table t", null },
      // Test normalization
      { "create table t1 (c1 char(2) for bit data) partition by column(c1)", null},
      { "insert into t1 values (X'0001')", null },
      { "insert into t1 values (X'0010')", null },
      { "insert into t1 values (X'0011')", null },
      { "select * from t1", new String[][]{ {"0001"},{"0010"},{"0011"} } },
      // Now insert something that needs to be expanded
      { "insert into t1 values (X'11')", null},
      { "select * from t1", new String[][]{ {"0001"},{"0010"},{"0011"},{"1120"} } },
      // Insert select, expand 1 byte
      { "create table t2 (c1 char(3) for bit data) partition by column(c1)", null},
      { "insert into t2 select c1 from t1", null},
      { "select * from t2", new String[][]{ {"000120"},{"001020"},{"001120"},{"112020"} } },
      { "drop table t2", null },
      // Insert select, expand many bytes
      { "create table t2 (c1 char(20) for bit data) partition by column(c1)", null },
      { "insert into t2 select c1 from t1", null },
      { "select * from t2", new String[][]{
            {"0001202020202020202020202020202020202020"},{"0010202020202020202020202020202020202020"},
            {"0011202020202020202020202020202020202020"},{"1120202020202020202020202020202020202020"} } },
      { "drop table t2", null },
      { "drop table t1", null },
      // Extra truncation tests
      { "create table t1 (b1 char(1) for bit data) partition by column(b1)", null },
      { "insert into t1 values (X'11')", null },
      { "insert into t1 values (X'10')", null },
      { "insert into t1 values (X'11')", null },
      { "insert into t1 values (X'1000')", "22001" },
      { "insert into t1 values (X'100000')", "22001" },
      { "insert into t1 values (X'10000000')", "22001" },
      { "insert into t1 values (X'1000000000')", "22001" },
      { "insert into t1 values (X'100001')", "22001" },
      { "insert into t1 values (X'0001')", "22001" },
      { "insert into t1 values (X'8001')", "22001" },
      { "insert into t1 values (X'8000')", "22001" },
      { "drop table t1", null },
      { "create table t1 (b9 char(2) for bit data) partition by column(b9)", null },
      { "insert into t1 values (X'1111')", null },
      { "insert into t1 values (X'111100')", "22001" },
      { "insert into t1 values (X'11110000')", "22001" },
      { "insert into t1 values (X'1111000000')", "22001" },
      { "insert into t1 values (X'1111111100000000')", "22001" },
      { "insert into t1 values (X'1111111111')", "22001" },
      { "insert into t1 values (X'11111111100001')", "22001" },
      { "insert into t1 values (X'0001')", null },
      { "insert into t1 values (X'8001')", null },
      { "insert into t1 values (X'8000')", null },
      { "drop table t1", null },
      { "create table t1 (b3 char(2) for bit data, b7 char(4) for bit data, b8 char (5) for bit data, b15 char(8) for bit data, b16 char(9) for bit data) partition by column(b3)", null },
      { "insert into t1 values ( X'1111', X'11111111', X'1111111111', X'1111111111111111', X'111111111111111111')", null },
      { "insert into t1 values ( X'1110', X'11111110', X'11111111', X'1111111111111110', X'1111111111111111')", null },
      { "insert into t1 values ( null, null, X'111111111110', null, null)", "22001" },
      { "insert into t1 values ( null, X'1111111100', null, null, null)", "22001" },
      { "insert into t1 values ( null, X'1111111111', null, null, null)", "22001" },
      { "insert into t1 values ( null, null, null, X'111111111111111100', null)", "22001" },
      { "insert into t1 values ( null, null, null, X'111111111111111111', null)", "22001" },
      { "insert into t1 values ( null, null, null, null, X'11111111111111111110')", "22001" },
      // autocommit off here in original sql script - needed?
      // bug 5160 - incorrect typing of VALUES table constructor on an insert;
      { "create table iv (id int, vc varchar(12)) partition by column(id)", null },
      { "insert into iv values (1, 'abc'), (2, 'defghijk'), (3, 'lmnopqrstcc')", null },
      { "insert into iv values (4, null), (5, 'null ok?'), (6, '2blanks  ')", null },
      { "insert into iv values (7, 'dddd'), (8, '0123456789123'), (9, 'too long')", "22001" },
      { "select id, vc, {fn length(vc)} AS LEN from iv order by 1", new String[][] {
            {"1","abc","3"}, {"2","defghijk","8"}, {"3", "lmnopqrstcc","11"},
            {"4",null,null},{"5","null ok?","8"},{"6","2blanks","7"} } },
      { "insert into iv select * from (values (10, 'pad'), (11, 'pad me'), (12, 'anakin jedi')) as t(i, c)", null}, 
      { "select id, vc, {fn length(vc)} AS LEN from iv order by 1", new String[][] {
            {"1","abc","3"}, {"2","defghijk","8"}, {"3", "lmnopqrstcc","11"},
            {"4",null,null},{"5","null ok?","8"},{"6","2blanks","7"},
            {"10","pad","3"}, {"11","pad me","6"},{"12","anakin jedi","11"} } },   // I see what you did there
      // check values outside of table constructors retain their CHARness
      { "select c, {fn length(c)} AS LEN from (values (1, 'abc'), (2, 'defghijk'), (3, 'lmnopqrstcc')) as t(i, c)", new String[][] {
            {"abc","3"}, {"defghijk","8"}, {"lmnopqrstcc","11"} } },
      { "drop table iv", null},
      { "create table bv (id int, vb varchar(16) for bit data) partition by column(id)", null},
      { "insert into bv values (1, X'1a'), (2, X'cafebabe'), (3, null)", null},
      { "select id, vb, {fn length(vb)} AS LEN from bv order by 1", "42846"},
      { "drop table bv", null},
      { "create table dv (id int, vc varchar(12)) partition by column(id)", null},
      { "insert into dv values (1, 1.2), (2, 34.5639), (3, null)", "42X61"},  // not-union-compatible error??
      { "insert into dv values (1, '1.2'), (2, '34.5639'), (3, null)", null},
      { "select id, vc from dv order by 1", new String[][] { {"1","1.2"},{"2","34.5639"},{"3",null} } },
      { "drop table dv", null},
      // bug 5306 -- incorrect padding of VALUES table constructor on an insert
      { "create table bitTable (id int, bv LONG VARCHAR FOR BIT DATA) partition by column(id)", null},
      { "insert into bitTable values (1, X'031'), (2, X'032'), (3, X'')", "42606"},
      { "insert into bitTable values (4, null), (5, X'033'), (6, X'2020')", "42606"},
      { "select id, bv, {fn length(bv)} as LEN from bitTable order by 1", "42846"},
      { "insert into bitTable select * from (values (10, 'pad'), (11, 'pad me'), (12, 'anakin jedi')) as t(i, c)", "42821"}, 
      { "select id, bv, {fn length(bv)} AS LEN from bitTable order by 1", "42846" },
      { "drop table bitTable", null},
      { "create table charTable (id int, cv long varchar) partition by column(id)", null},
      { "insert into charTable values (1, x'0101'), (2, x'00101100101001'), (3, x'')", "42821"},
      { "insert into charTable values (4, null), (5, x'1010101111'), (6, x'1000')", "42X61"},
      { "select id, cv, {fn length(cv)} as LEN from charTable order by 1", new String[0][0] },  //empty results
      { "insert into charTable select * from (values (10, x'001010'), (11, x'01011010101111'), (12, x'0101010101000010100101110101')) as t(i, c)", "42821"},
      { "select id, cv, {fn length(cv)} as LEN from charTable order by 1", new String[0][0] },  //empty results
      { "drop table charTable", null},
      // union tests
      { "create table pt5 (b5 char(2) for bit data) partition by column(b5)", null},
      { "create table pt10 (b10 char (4) for bit data) partition by column(b10)", null},
      { "insert into pt10 values (x'01000110')", null},
      { "insert into pt5 values (x'1010')", null},
      { "select {fn length(CM)} from (select b5 from pt5 union all select b10 from pt10) as t(CM)", "42846"},
      { "drop table pt5", null },
      { "drop table pt10", null },
      { "create table t5612 (c1 char(10), c2 varchar(10), c3 long  varchar) partition by column(c1)", null},
      { "insert into t5612 values (X'00680069', X'00680069', X'00680069')", "42821"},
      { "select * from t5612", new String[0][0]},   // empty results
      { "values cast(X'00680069' as char(30)), cast(X'00680069' as varchar(30)), cast(X'00680069' as long varchar)", "42846"}
      // FIXME : GemFireXD does not support CASCADE DELETE which this test depends upon - reenable later
/*      { "create table npetest1 (col1 varchar(36) for bit data not null, constraint pknpe1 primary key (col1)) partition by primary key", null},
      { "create table npetest2 (col2 varchar(36) for bit data, constraint fknpe1 foreign key (col2) references npetest1(col1) on delete cascade) partition by column(col2) colocate with (npetest1)", null},  // GemFireXD does not support cascade delete - will throw 0A000
      { "insert into npetest1 (col1) values (X'0000000001')", null},
      { "insert into npetest1 (col1) values (X'0000000002')", null},
      { "insert into npetest1 (col1) values (X'0000000003')", null},
      { "insert into npetest2 (col2) values (X'0000000001')", null},
      { "insert into npetest2 (col2) values (NULL)", null},
      { "insert into npetest2 (col2) values (X'0000000002')", null},
      { "select col1 from npetest1 where col1 not in (select col2 from npetest2)", new String[0][0] },
      { "select col1 from npetest1 where col1 not in (select col2 from npetest2 where col2 is not null)", 
                       new String[][] { {"0000000003"} } },
      { "drop table npetest2", null},
      { "drop table npetest1", null}  */
    };

    Connection conn = TestUtil.getConnection();
    conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
    conn.setAutoCommit(true);
    Statement stmt = conn.createStatement();
    // Go through the array, execute each string[0], check sqlstate [1]
    // This will fail on the first one that succeeds where it shouldn't
    // or throws unknown exception
    JDBC.SQLUnitTestHelper(stmt,Script_BitUTPartitioning);
    conn.commit();
  }
}
