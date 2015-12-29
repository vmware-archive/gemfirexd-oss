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

public class LangScripts_FunctionsDUnit extends DistributedSQLTestBase {

	public LangScripts_FunctionsDUnit(String name) {
		super(name);
		// TODO Auto-generated constructor stub
	}

	  // This test is the as-is LangScript conversion, without any partitioning clauses
	  public void testLangScript_FunctionsTestNoPartitioning() throws Exception
	  {
	    // This is a JUnit conversion of the Derby Lang functions.sql script
	    // without any GemFireXD extensions
		  
	    // Catch exceptions from illegal syntax
	    // Tests still not fixed marked FIXME
		  
	    // Array of SQL text to execute and sqlstates to expect
	    // The first object is a String, the second is either 
	    // 1) null - this means query returns no rows and throws no exceptions
	    // 2) a string - this means query returns no rows and throws expected SQLSTATE
	    // 3) a String array - this means query returns rows which must match (unordered) given resultset
	    //       - for an empty result set, an uninitialized size [0][0] array is used
	    Object[][] Script_FunctionsUT = {
		// Test various functions
		{ "create table alltypes(id int not null primary key,smallIntCol smallint,intCol int,bigIntCol bigint,floatCol float,float1Col float(1),float26Col float(26),realCol real,doubleCol double,decimalCol decimal,decimal10Col decimal(10),decimal11Col decimal(11),numeric10d2Col numeric(10,2),charCol char,char32Col char(32),charForBitCol char(16) for bit data,varcharCol varchar(64),varcharForBitCol varchar(64) for bit data,longVarcharCol long varchar,blobCol blob(10k),clobCol clob(10k),dateCol date,timeCol time,timestampCol timestamp)", null },
		{ "insert into allTypes(id) values(1),(2)", null },
		{ "update allTypes set smallIntCol = 2 where id = 1", null },
		{ "update allTypes set intCol = 2 where id = 1", null },
		{ "update allTypes set bigIntCol = 3 where id = 1", null },
		{ "update allTypes set floatCol = 4.1 where id = 1", null },
		{ "update allTypes set float1Col = 5 where id = 1", null },
		{ "update allTypes set float26Col = 6.1234567890123456 where id = 1", null },
		{ "update allTypes set realCol = 7.2 where id = 1", null },
		{ "update allTypes set doubleCol = 8.2 where id = 1", null },
		{ "update allTypes set decimalCol = 9 where id = 1", null },
		{ "update allTypes set decimal10Col = 1234 where id = 1", null },
		{ "update allTypes set decimal11Col = 1234 where id = 1", null },
		{ "update allTypes set numeric10d2Col = 11.12 where id = 1", null },
		{ "update allTypes set charCol = 'a' where id = 1", null },
		{ "update allTypes set char32Col = 'abc' where id = 1", null },
		{ "update allTypes set charForBitCol = X'ABCD' where id = 1", null },
		{ "update allTypes set varcharCol = 'abcde' where id = 1", null },
		{ "update allTypes set varcharForBitCol = X'ABCDEF' where id = 1", null },
		{ "update allTypes set longVarcharCol = 'abcdefg' where id = 1", null },
		{ "update allTypes set blobCol = cast( X'0031' as blob(10k)) where id = 1", null },
		{ "update allTypes set clobCol = 'clob data' where id = 1", null },
		{ "update allTypes set dateCol = date( '2004-3-13') where id = 1", null },
		{ "update allTypes set timeCol = time( '16:07:21') where id = 1", null },
		{ "update allTypes set timestampCol = timestamp( '2004-3-14 17:08:22.123456') where id = 1", null },
		{ "select id, length(smallIntCol) from allTypes order by id", new String[][] { {"1","2"},{"2",null} } },
		{ "select id, length(intCol) from allTypes order by id", new String[][] { {"1","4"},{"2",null} } },
		{ "select id, length(bigIntCol) from allTypes order by id", new String[][] { {"1","8"},{"2",null} } },
		{ "select id, length(floatCol) from allTypes order by id", new String[][] { {"1","8"},{"2",null} } },
		{ "select id, length(float1Col) from allTypes order by id", new String[][] { {"1","4"},{"2",null} } },
		{ "select id, length(float26Col) from allTypes order by id", new String[][] { {"1","8"},{"2",null} } },
		{ "select id, length(realCol) from allTypes order by id", new String[][] { {"1","4"},{"2",null} } },
		{ "select id, length(doubleCol) from allTypes order by id", new String[][] { {"1","8"},{"2",null} } },
		{ "select id, length(decimalCol) from allTypes order by id", new String[][] { {"1","20"},{"2",null} } },
		{ "select id, length(decimal10Col) from allTypes order by id", new String[][] { {"1","6"},{"2",null} } },
		{ "select id, length(decimal11Col) from allTypes order by id", new String[][] { {"1","6"},{"2",null} } },
		{ "select id, length(numeric10d2Col) from allTypes order by id", new String[][] { {"1","6"},{"2",null} } },
		{ "select id, length(charCol) from allTypes order by id", new String[][] { {"1","1"},{"2",null} } },
		{ "select id, length(char32Col) from allTypes order by id", new String[][] { {"1","32"},{"2",null} } },
		{ "select id, length(charForBitCol) from allTypes order by id", new String[][] { {"1","16"},{"2",null} } },
		{ "select id, length(varcharCol) from allTypes order by id", new String[][] { {"1","5"},{"2",null} } },
		{ "select id, length(varcharForBitCol) from allTypes order by id", new String[][] { {"1","3"},{"2",null} } },
		{ "select id, length(longVarcharCol) from allTypes order by id", new String[][] { {"1","7"},{"2",null} } },
		{ "select id, length(blobCol) from allTypes order by id", new String[][] { {"1","2"},{"2",null} } },
		{ "select id, length(clobCol) from allTypes order by id", new String[][] { {"1","9"},{"2",null} } },
		{ "select id, length(dateCol) from allTypes order by id", new String[][] { {"1","4"},{"2",null} } },
		{ "select id, length(timeCol) from allTypes order by id", new String[][] { {"1","3"},{"2",null} } },
		{ "select id, length(timestampCol) from allTypes order by id", new String[][] { {"1","10"},{"2",null} } },
		{ "values( length( 1), length( 720176), length( 12345678901))", new String[][] { {"4","4","8"} } },
		{ "values( length( 2.2E-1))", new String [][] { {"8"} } },
		{ "values( length( 1.), length( 12.3), length( 123.4), length( 123.45))", new String[][] { {"1","2","3","3"} } },
		{ "values( length( '1'), length( '12'))", new String[][] { {"1","2"} } },
		{ "values( length( X'00'), length( X'FF'), length( X'FFFF'))", new String[][] { {"1","1","2"} } },
		{ "values( length( date('0001-1-1')), length( time('0:00:00')), length( timestamp( '0001-1-1 0:00:00')))", new String[][] { {"4","3","10"} } },
		{ "select id from allTypes where length(smallIntCol) > 5 order by id", new String[0][0] },
		{ "select id from allTypes where length(intCol) > 5 order by id", new String[0][0] },
		{ "select id from allTypes where length(bigIntCol) > 5 order by id", new String[][] { {"1"} } },
		{ "select id from allTypes where length(floatCol) > 5 order by id", new String[][] { {"1"} } },
		{ "select id from allTypes where length(float1Col) > 5 order by id", new String[0][0] },
		{ "select id from allTypes where length(float26Col) > 5 order by id", new String[][] { {"1"} } },
		{ "select id from allTypes where length(realCol) > 5 order by id", new String[0][0] },
		{ "select id from allTypes where length(doubleCol) > 5 order by id", new String[][] { {"1"} } },
		{ "select id from allTypes where length(decimalCol) > 5 order by id", new String[][] { {"1"} } },
		{ "select id from allTypes where length(decimal10Col) > 5 order by id", new String[][] { {"1"} } },
		{ "select id from allTypes where length(decimal11Col) > 5 order by id", new String[][] { {"1"} } },
		{ "select id from allTypes where length(numeric10d2Col) > 5 order by id", new String[][] { {"1"} } },
		{ "select id from allTypes where length(charCol) > 5 order by id", new String[0][0] },
		{ "select id from allTypes where length(char32Col) > 5 order by id", new String[][] { {"1"} } },
		{ "select id from allTypes where length(charForBitCol) > 5 order by id", new String[][] { {"1"} } },
		{ "select id from allTypes where length(varcharCol) > 5 order by id", new String[0][0] },
		{ "select id from allTypes where length(varcharForBitCol) > 5 order by id", new String[0][0] },
		{ "select id from allTypes where length(longVarcharCol) > 5 order by id", new String[][] { {"1"} } },
		{ "select id from allTypes where length(blobCol) > 5 order by id", new String[0][0] },
		{ "select id from allTypes where length(clobCol) > 5 order by id", new String[][] { {"1"} } },
		{ "select id from allTypes where length(dateCol) > 5 order by id", new String[0][0] },
		{ "select id from allTypes where length(timeCol) > 5 order by id", new String[0][0] },
		{ "select id from allTypes where length(timestampCol) > 5 order by id", new String[][] { {"1"} } },
		{ "select id, length( charCol || 'abc') from allTypes order by id", new String[][] { {"1","4"},{"2",null} } },
		{ "values {FN LENGTH('xxxx                    ')}", new String[][] { {"4"} } },
		{ "values {FN LENGTH(' xxxx                    ')}", new String[][] { {"5"} } },
		{ "values {FN LENGTH('  xxxx                    ')}", new String[][] { {"6"} } },
		{ "values {FN LENGTH('   xxxx                    ')}", new String[][] { {"7"} } }
	    };

	    // Start 1 client and 3 servers, use default partitioning
	    startVMs(1, 3);

	    Connection conn = TestUtil.getConnection();
	    Statement stmt = conn.createStatement();
	    // Go through the array, execute each string[0], check sqlstate [1]
	    // This will fail on the first one that succeeds where it shouldn't
	    // or throws unknown exception
	    JDBC.SQLUnitTestHelper(stmt,Script_FunctionsUT);
	  }
	  

}
