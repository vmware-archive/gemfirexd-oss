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

public class LangScripts_StringTypesDUnit extends DistributedSQLTestBase {

	public LangScripts_StringTypesDUnit(String name) {
		super(name);
		// TODO Auto-generated constructor stub
	}
	  // This test is the as-is LangScript conversion, without any partitioning clauses
	  public void testLangScript_StringTypesTestNoPartitioning() throws Exception
	  {
	    // This is a JUnit conversion of the Derby Lang stringtypes.sql script
	    // without any GemFireXD extensions
		  
	    // Catch exceptions from illegal syntax
	    // Tests still not fixed marked FIXME
		  
	    // Array of SQL text to execute and sqlstates to expect
	    // The first object is a String, the second is either 
	    // 1) null - this means query returns no rows and throws no exceptions
	    // 2) a string - this means query returns no rows and throws expected SQLSTATE
	    // 3) a String array - this means query returns rows which must match (unordered) given resultset
	    //       - for an empty result set, an uninitialized size [0][0] array is used
	    Object[][] Script_StringTypesUT = {
		// create a table with null and non-null char columns of different lengths
		{ "create table ct (c1 char(1), c2 char(5) not null, c3 char(30) default null)", null },
		// first, try values that fill each column with non-blanks
		{ "insert into ct values ('1', '11111', '111111111111111111111111111111')", null },
		// now try some values that are shorter than the columns
		{ "insert into ct values ('', '22', '222')", null },
		// now try some values that are longer than the columns, where the excess characters are blanks
		{ "insert into ct values ('3         ', '33333      ', '333333333333333333333333333333          ')", null },
		// now try some values that are longer than the columns, where the excess characters are non-blanks.  These should get errors
		// FIXME
		// This succeeds - should fail with truncation error - not reproducible in regular GFXD command line
		//{ "insert into ct values ('44', '4', '4')", "22001" },
		//{ "insert into ct values ('5', '555555', '5')", "22001" },
		//{ "insert into ct values ('6', '66666', '6666666666666666666666666666666')", "22001" },
		// now try inserting some nulls, first in columns that accept them
		{ "insert into ct values (null, '77777', null)", null },
		// now try inserting nulls into columns that don't accept them
		{ "insert into ct values ('8', null, '8')", "23502" },
		// now check the rows that made it into the table successfully
		{ "select * from ct", new String[][] {
			{"1","11111","111111111111111111111111111111"},
			{"","22","222"},
			{"3","33333","333333333333333333333333333333"},
			{null,"77777",null} } },
		//  now try the char_length function on the columns
		{ "select {fn length(c1)}, {fn length(c2)}, {fn length(c3)} from ct", new String[][] {
			{"1","5","30"},
			{"0","2","3"},
			{"1","5","30"},
			{null,"5",null} } },       
		//  now create a table with varchar columns
		{ "create table vt (c1 varchar(1), c2 varchar(5) not null, c3 varchar(30) default null)", null },
		{ "insert into vt values ('1', '11111', '111111111111111111111111111111')", null },
		{ "insert into vt values ('', '22', '222')", null },
		{ "insert into vt values ('3         ', '33333      ', '333333333333333333333333333333          ')", null },
		{ "insert into vt values ('44', '4', '4')", "22001" },
		{ "insert into vt values ('5', '555555', '5')", "22001" },
		{ "insert into vt values ('6', '66666', '6666666666666666666666666666666')", "22001" },
		{ "insert into vt values (null, '77777', null)", null },
		{ "insert into vt values ('8', null, '8')", "23502" },
		{ "select * from vt", new String[][] {
			{"1","11111","111111111111111111111111111111"},
			{"","22","222"},
			{"3","33333","333333333333333333333333333333"},
			{null,"77777",null} } },
		{ "select {fn length(c1)}, {fn length(c2)}, {fn length(c3)} from vt", new String[][] {
			{"1","5","30"},
			{"0","2","3"},
			{"1","5","30"},
			{null,"5",null} } },       
		// now create a table with long varchar columns
		{ "create table lvt (c1 long varchar, c2 long varchar not null, c3 long varchar default null)", null },
		{ "insert into lvt values ('1', '11', '111')", null },
		{ "insert into lvt values ('2 ', '22  ', '222   ')", null },
		{ "insert into lvt values ('3333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333','333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333','33333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333')", null },
		{ "insert into lvt values (null, '4444', null)", null },
		{ "insert into lvt values ('5', null, '55555')", "23502" },
		// FIXME - do not select the data as it is too large
		{ "select {fn length(c1)}, {fn length(c2)}, {fn length(c3)} from lvt", new String[][] {
			{"1","2","3"},
			{"1","2","3"},
			{"247","432","821"},
			{null,"4",null} } },       
		{ "insert into ct select * from vt", null},
		{ "select * from ct", new String[][] {
			{"1","11111","111111111111111111111111111111"},
			{"","22","222"},
			{"3","33333","333333333333333333333333333333"},
			{null,"77777",null},
			{"1","11111","111111111111111111111111111111"},
			{"","22","222"},
			{"3","33333","333333333333333333333333333333"},
			{null,"77777",null} } },
		{ "insert into vt select * from ct", null },
		{ "select * from vt", new String[][] {
			{"1","11111","111111111111111111111111111111"},
			{"","22","222"},
			{"3","33333","333333333333333333333333333333"},
			{null,"77777",null},
			{"1","11111","111111111111111111111111111111"},
			{"","22","222"},
			{"3","33333","333333333333333333333333333333"},
			{null,"77777",null},
			{"1","11111","111111111111111111111111111111"},
			{"","22","222"},
			{"3","33333","333333333333333333333333333333"},
			{null,"77777",null} } },
		{ "insert into ct select c3, c2, c1 from vt where c3 is not null", "22001" },
		{ "select * from ct", new String[][] {
			{"1","11111","111111111111111111111111111111"},
			{"","22","222"},
			{"3","33333","333333333333333333333333333333"},
			{null,"77777",null},
			{"1","11111","111111111111111111111111111111"},
			{"","22","222"},
			{"3","33333","333333333333333333333333333333"},
			{null,"77777",null} } },
		{ "insert into vt select c3, c2, c1 from ct where c3 is not null", "22001" },
		{ "select * from vt", new String[][] {
			{"1","11111","111111111111111111111111111111"},
			{"","22","222"},
			{"3","33333","333333333333333333333333333333"},
			{null,"77777",null},
			{"1","11111","111111111111111111111111111111"},
			{"","22","222"},
			{"3","33333","333333333333333333333333333333"},
			{null,"77777",null},
			{"1","11111","111111111111111111111111111111"},
			{"","22","222"},
			{"3","33333","333333333333333333333333333333"},
			{null,"77777",null} } },
		// insert-select from char columns into long varchar columns
		{ "insert into lvt select * from ct", null },
		// FIXME - don't select from lvt as values too big - maybe just check rowcount
		{ "select count(*) from lvt", new String[][] { {"12" } } },
		// insert-select from varchar columns into long varchar columns
		{ "insert into lvt select * from vt", null },
		// FIXME - don't select from lvt as values too big - maybe just check rowcount
		{ "select count(*) from lvt", new String[][] { {"24"} } },
		//  insert-select from long varchar columns into char columns with trunc. errors
		{ "insert into ct select * from lvt", "22001" },
		//FIXME
		// In multi-server scenario, more rows might have been inserted even though insert failed
		//{ "select * from ct", new String[][] {
		//	{"1","11111","111111111111111111111111111111"},
		//	{"","22","222"},
		//	{"3","33333","333333333333333333333333333333"},
		//	{null,"77777",null},
		//	{"1","11111","111111111111111111111111111111"},
		//	{"","22","222"},
		//	{"3","33333","333333333333333333333333333333"},
		//	{null,"77777",null} } },
		//  insert-select from long varchar columns into char columns without trunc errs
		// FIXME
		// This throws an assertion - skip the insert and all other selects from CT and VT below
		//{ "insert into ct select * from lvt where cast(substr(c1,1,30) as varchar(30)) = '1' or cast(substr(c1,1,30) as varchar(30)) = '2'", null },
		//{ "select * from ct", new String[][] {
		//	{"1","11111","111111111111111111111111111111"},
		//	{"","22","222"},
		//	{"3","33333","333333333333333333333333333333"},
		//	{null,"77777",null},
		//	{"1","11111","111111111111111111111111111111"},
		//	{"","22","222"},
		//	{"3","33333","333333333333333333333333333333"},
		//	{null,"77777",null},
		//	{"1","11","111"},
		//	{"2","22","222"},
		//	{"1","11111","111111111111111111111111111111"},
		//	{"1","11111","111111111111111111111111111111"},
		//	{"1","11111","111111111111111111111111111111"},
		//	{"1","11111","111111111111111111111111111111"} } },
		// insert-select from long varchar columns into varchar columns with trunc. errs
		{ "insert into vt select * from lvt", "22001" },
		// insert-select from long varchar columns into varchar cols without trunc errs
		//{ "insert into vt select * from lvt where cast(substr(c1,1,30) as varchar(30)) = '1' or cast(substr(c1,1,30) as varchar(30)) = '2'", null },
		//{ "select * from vt", new String[][] {
		//	{"1","11111","111111111111111111111111111111"},
		//	{"","22","222"},
		//	{"3","33333","333333333333333333333333333333"},
		//	{null,"77777",null},
		//	{"1","11111","111111111111111111111111111111"},
		//	{"","22","222"},
		//	{"3","33333","333333333333333333333333333333"},
		//	{null,"77777",null},
		//	{"1","11111","111111111111111111111111111111"},
		//	{"","22","222"},
		//	{"3","33333","333333333333333333333333333333"},
		//	{null,"77777",null},
		//	{"1","11","111"},
		//	{"2","22","222"},
		//	{"1","11111","111111111111111111111111111111"},
		//	{"1","11111","111111111111111111111111111111"},
		//	{"1","11111","111111111111111111111111111111"},
		//	{"1","11111","111111111111111111111111111111"},
		//	{"1","11111","111111111111111111111111111111"} } },
		// Now try insert-select with type conversion where column lengths don't match
		// but there are no truncation errors.  Need new tables for this.
		{ "create table ct2 (c1 char(5), c2 char(10))", null },
		{ "insert into ct2 values ('111', '111')", null },
		{ "create table vt2 (c1 varchar(5), c2 varchar(10))", null },
		{ "insert into vt2 values ('222', '222')", null },
		{ "create table lvt2 (c1 long varchar, c2 long varchar)", null },
		{ "insert into lvt2 values ('333', '333')", null },
		{ "insert into ct2 select * from vt2", null },
		{ "insert into ct2 select * from lvt2", null },
		{ "select * from ct2", new String [][] {
			{"111","111"},
			{"222","222"},      
			{"333","333"} } },       
		{ "insert into vt2 select * from ct2", null },
		{ "insert into vt2 select * from lvt2", null },
		{ "select * from vt2", new String [][] {
			{"111","111"},
			{"222","222"},      
			{"222","222"},      
			{"333","333"},      
			{"333","333"} } },       
		{ "insert into lvt2 select * from ct2", null },
		{ "insert into lvt2 select * from vt2", null },
		{ "select * from lvt2", new String [][] {
			{"333","333"},                                                                                                                             		{"111","111"},                                                                                                                             
			{"222","222"},                                                                                                                             
			{"333","333"},
			{"222","222"},                                                                                                                             
			{"111","111"},                                                                                                                             
			{"222","222"},                                                                                                                             
			{"333","333"},
			{"333","333"} } },       
		//  Now try string constants that contain the ' character
		{ "delete from vt", null },
		// FIXME
		// This throws an assertion, not reproducible from command line with same SQL text
		//{ "insert into vt values (''', '12''34', '123''456''''''789')", null },
		//{ "select * from vt", new String[][] {
		//	{"'","12'34","123'456'''789"} } },
		//  Try creating a column with an illegal length
		{ "create table badtab (x char(10.2))", "42X44" },
		{ "create table badtab2 (x varchar(0))", "42X44" },
		{ "create table badtab3 (x long varchar(3))", "42X01" },
		{ "create table badtab4 (x char(300))", "42611" },
		//  JDBC escape syntax for string functions
		{ "create table trash(c1 char(10))", null },
		{ "insert into trash values 'asdf', 'asdfasdf'", null },
		{ "select {fn length(c1)}, length(c1) from trash", new String [][] {
			{"4","10"},
			{"8","10"} } },
		{ "drop table trash", null },
		//FIXME
		// This scenario incorrectly returns a match on this row with trailing characters
		{ "create table z1 (col1 CHAR(5))", null },
		{ "insert into z1 values ('TEST1')", null },
		{ "select col1 from z1 where col1='TEST1'", new String[][] { {"TEST1"} } },
		//{ "select col1 from z1 where col1='TEST1XXXX'", new String[0][0] },  // empty result set
		{ "drop table z1", null}
		// New scenario - primary key over CHAR/CHAR for bit data columns shows incorrect results - test the fix
	/*
		{ "create table char1 (col1 char(5) primary key)", null },
		{ "insert into char1 values ('ABC')", null },
		{ "select col1 from char1 where col1='ABC'", new String[][] { {"ABC"} } },
		{ "select col1 from char1 where col1='ABC '",new String[][] { {"ABC"} } },
		{ "select col1 from char1 where col1='ABC  '", new String[][] { {"ABC"} } },
		{ "select col1 from char1 where col1!='ABC'", new String[0][0] },
		{ "drop table char1", null },
		{ "create table charfbd1 (col1 char(5) for bit data primary key)", null },
		{ "insert into charfbd1 values (x'515253')", null },
		{ "select col1 from charfbd1 where col1=x'515253'", new String[][] { {"5152532020"} } },   // extra blanks as bitdata
		{ "select col1 from charfbd1 where col1=x'51525320'", new String[][] { {"5152532020"} } }, 
		{ "select col1 from charfbd1 where col1!=x'515253'", new String[0][0] },
		{ "drop table charfbd1", null }
	*/
	    };

	    // Start 1 client and 3 servers, use default partitioning
	    startVMs(1, 3);


	    Connection conn = TestUtil.getConnection();
	    Statement stmt = conn.createStatement();
	    // Go through the array, execute each string[0], check sqlstate [1]
	    // This will fail on the first one that succeeds where it shouldn't
	    // or throws unknown exception
	    JDBC.SQLUnitTestHelper(stmt,Script_StringTypesUT);
	  }

}
