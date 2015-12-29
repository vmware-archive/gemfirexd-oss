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

public class LangScripts_SelectDUnit extends DistributedSQLTestBase {

	public LangScripts_SelectDUnit(String name) {
		super(name);
		// TODO Auto-generated constructor stub
	}
	  // This test is the as-is LangScript conversion, without any partitioning clauses
	  public void testLangScript_SelectTestNoPartitioning() throws Exception
	  {
	    // This is a JUnit conversion of the Derby Lang Select.sql script
	    // without any GemFireXD extensions
		  
	    // Catch exceptions from illegal syntax
	    // Tests still not fixed marked FIXME
		  
	    // Array of SQL text to execute and sqlstates to expect
	    // The first object is a String, the second is either 
	    // 1) null - this means query returns no rows and throws no exceptions
	    // 2) a string - this means query returns no rows and throws expected SQLSTATE
	    // 3) a String array - this means query returns rows which must match (unordered) given resultset
	    //       - for an empty result set, an uninitialized size [0][0] array is used
	    Object[][] Script_SelectUT = {
		{ "create table t(i int, s smallint)", null },
		{ "insert into t (i,s) values (1956,475)", null },
		{ "select i from t", new String[][] { {"1956"} } },
		{ "select i,s from t", new String[][] { {"1956","475"} } },
		{ "select s,i from t", new String[][] { {"475","1956"} } },
		{ "select i,i,s,s,i,i from t", new String[][] {
			{"1956","1956","475","475","1956","1956"} } },
		{ "select 10 from t", new String[][] { {"10"} } },
		{ "select t.i from t", new String[][] { {"1956"} } },
		{ "select b.i from t b", new String[][] { {"1956"} } },
		{ "select *, 10, i from t", "42X01" },
		{ "select b.* from t b", new String[][] { {"1956","475"} } },
		{ "select t.* from t", new String[][] { {"1956","475"} } },
		{ "(select * from t)", new String[][] { {"1956","475"} } },    //?!
		{ "select * from t where i", "42X19" },
		{ "select asdf.* from t", "42X10" },
		{ "drop table t", null },
		// FIXME
		// This large collection of tables should be partitioned, but the join makes colocation very difficult
		// Use replicated for now
		{ "CREATE SCHEMA CONTENT", null },
		{ "CREATE TABLE CONTENT.CONTENT (ID INTEGER NOT NULL, CREATOR VARCHAR(128) NOT NULL, CREATION_DATE DATE NOT NULL, URL VARCHAR(256) NOT NULL, TITLE VARCHAR(128) NOT NULL, DESCRIPTION VARCHAR(512) NOT NULL, HEIGHT INTEGER NOT NULL, WIDTH INTEGER NOT NULL) REPLICATE", null },
		{ "ALTER TABLE CONTENT.CONTENT ADD CONSTRAINT CONTENT_ID PRIMARY KEY (ID)", null },
		{ "CREATE TABLE CONTENT.STYLE (ID INTEGER NOT NULL,DESCRIPTION VARCHAR(128) NOT NULL) REPLICATE", null },
		{ "ALTER TABLE CONTENT.STYLE ADD CONSTRAINT STYLE_ID PRIMARY KEY (ID)", null },
		{ "CREATE TABLE CONTENT.CONTENT_STYLE  (CONTENT_ID INTEGER NOT NULL, STYLE_ID INTEGER NOT NULL) REPLICATE", null },
		{ "ALTER TABLE CONTENT.CONTENT_STYLE ADD CONSTRAINT CONTENTSTYLEID PRIMARY KEY (CONTENT_ID, STYLE_ID)", null },
		{ "CREATE TABLE CONTENT.KEYGEN (KEYVAL INTEGER NOT NULL, KEYNAME VARCHAR(256) NOT NULL) REPLICATE", null },
		{ "ALTER TABLE CONTENT.KEYGEN  ADD CONSTRAINT PK_KEYGEN PRIMARY KEY (KEYNAME)", null },
		{ "CREATE TABLE CONTENT.RATING  (ID INTEGER NOT NULL,RATING DOUBLE PRECISION NOT NULL,ENTRIES DOUBLE PRECISION NOT NULL) REPLICATE", null },
		{ "ALTER TABLE CONTENT.RATING ADD CONSTRAINT PK_RATING PRIMARY KEY (ID)", null },
		{ "INSERT INTO CONTENT.STYLE VALUES (1, 'BIRD')", null },
		{ "INSERT INTO CONTENT.STYLE VALUES (2, 'CAR')", null },
		{ "INSERT INTO CONTENT.STYLE VALUES (3, 'BUILDING')", null },
		{ "INSERT INTO CONTENT.STYLE VALUES (4, 'PERSON')", null },
		{ "INSERT INTO CONTENT.CONTENT values(1, 'djd', CURRENT DATE, 'http://url.1', 'title1', 'desc1', 100, 100)", null },
		{ "INSERT INTO CONTENT.CONTENT values(2, 'djd', CURRENT DATE, 'http://url.2', 'title2', 'desc2', 100, 100)", null },
		{ "INSERT INTO CONTENT.CONTENT values(3, 'djd', CURRENT DATE, 'http://url.3', 'title3', 'desc3', 100, 100)", null },
		{ "INSERT INTO CONTENT.CONTENT values(4, 'djd', CURRENT DATE, 'http://url.4', 'title4', 'desc4', 100, 100)", null },
		{ "INSERT INTO CONTENT.CONTENT values(5, 'djd', CURRENT DATE, 'http://url.5', 'title5', 'desc5', 100, 100)", null },
		{ "INSERT INTO CONTENT.CONTENT_STYLE VALUES(1,1)", null },
		{ "INSERT INTO CONTENT.CONTENT_STYLE VALUES(1,2)", null },
		{ "INSERT INTO CONTENT.CONTENT_STYLE VALUES(2,1)", null },
		{ "INSERT INTO CONTENT.CONTENT_STYLE VALUES(2,4)", null },
		{ "INSERT INTO CONTENT.CONTENT_STYLE VALUES(3,3)", null },
		{ "INSERT INTO CONTENT.CONTENT_STYLE VALUES(3,4)", null },
		{ "INSERT INTO CONTENT.CONTENT_STYLE VALUES(3,1)", null },
		{ "INSERT INTO CONTENT.CONTENT_STYLE VALUES(4,4)", null },
		{ "INSERT INTO CONTENT.CONTENT_STYLE VALUES(5,1)", null },
		{ "INSERT INTO CONTENT.RATING VALUES(1, 4.5, 1)", null },
		{ "INSERT INTO CONTENT.RATING VALUES(2, 4.0, 1)", null },
		{ "INSERT INTO CONTENT.RATING VALUES(3, 3.9, 1)", null },
		{ "INSERT INTO CONTENT.RATING VALUES(4, 4.1, 1)", null },
		{ "INSERT INTO CONTENT.RATING VALUES(5, 4.0, 1)", null },
		{ "select S.DESCRIPTION, FAV.MAXRATE, C.TITLE, C.URL FROM CONTENT.RATING R, CONTENT.CONTENT C, CONTENT.STYLE S, CONTENT.CONTENT_STYLE CS, (select S.ID, max(rating) from CONTENT.RATING R, CONTENT.CONTENT C, CONTENT.STYLE S, CONTENT.CONTENT_STYLE CS group by S.ID) AS FAV(FID,MAXRATE) where R.ID = C.ID AND C.ID = CS.CONTENT_ID AND CS.STYLE_ID = FAV.FID AND FAV.FID = S.ID AND FAV.MAXRATE = R.RATING", new String[][] {
			{"BIRD","4.5","title1","http://url.1"},{"CAR","4.5","title1","http://url.1"} } },
		{ "drop table content.rating", null },
		{ "drop table content.content_style", null },
		{ "drop table content.content", null },
		{ "drop table content.style", null },
		{ "drop table content.keygen", null },
		{ "drop schema content restrict", null }
	   };

	    // Start 1 client and 3 servers, use default partitioning
	    startVMs(1, 3);

	    Connection conn = TestUtil.getConnection();
	    Statement stmt = conn.createStatement();
	    // Go through the array, execute each string[0], check sqlstate [1]
	    // This will fail on the first one that succeeds where it shouldn't
	    // or throws unknown exception
	    JDBC.SQLUnitTestHelper(stmt,Script_SelectUT);
	  }

}
