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

public class LangScripts_NullsTest extends JdbcTestBase {

  public static void main(String[] args) {
    TestRunner.run(new TestSuite(LangScripts_NullsTest.class));
  }
  
  public LangScripts_NullsTest(String name) {
    super(name); 
  }

  // This test is the as-is LangScript conversion, without any partitioning clauses
  public void testLangScript_NullsTestNoPartitioning() throws Exception
  {
    // This is a JUnit conversion of the Derby Lang nulls.sql script
    // without any GemFireXD extensions
	  
    // Catch exceptions from illegal syntax
    // Tests still not fixed marked FIXME
	  
    // Array of SQL text to execute and sqlstates to expect
    // The first object is a String, the second is either 
    // 1) null - this means query returns no rows and throws no exceptions
    // 2) a string - this means query returns no rows and throws expected SQLSTATE
    // 3) a String array - this means query returns rows which must match (unordered) given resultset
    //       - for an empty result set, an uninitialized size [0][0] array is used
    Object[][] Script_NullsUT = {
      // this test shows the current supported null value functionality
      // First, trying to define null and not null for a column
      { "create table a(a1 int null not null)", "42X83" },
      { "create table a(a1 int not null null)", "42X83" },
      { "create table a(a1 int not null , a2 int not null)", null },
      // alter table adding explicitly nullable column and primary key column constraint on it fails
      { "alter table a add column a3 int null constraint ap1 primary key", "42831" },
      // alter table table level primary key constraint on nullable column doesn't give an error
      { "alter table a add constraint ap1 primary key(a1,a2)", null },
      { "drop table a", null },
      // create table with not null column and unique key should work
      { "create table a (a int not null constraint auniq unique )", null },
      { "insert into a values (1)", null },
      { "insert into a values (1)", "23505" },
      { "drop table a", null },
      // alter nullability on a unique column should fail
      { "create table a ( a int not null unique)", null},
      { "alter table a modify a null", "42X01" },   // GFXD doesn't support modify keyword
      { "drop table a", null},
      // try adding a primary key where there is null data, this should error
      // FIXME - GFXD does not allow ALTER on a table with data, some of these tests need to be redone
      //         after full ALTER TABLE support is added
      { "create table a (a1 int not null, a2 int)", null },
      { "insert into a values(1, NULL)", null },
      { "alter table a add constraint ap1 primary key(a1, a2)", "42831" },
      { "drop table a", null },
      { "create table a (a1 int, a2 int, a3 int)", null },
      { "alter table a add constraint ap1 primary key(a1, a2, a3)", "42831" },
      { "drop table a", null },
      { "create table a (a1 int not null, a2 int, a3 int)", null },
      { "insert into a values(1,1,1)", null },
      { "alter table a add constraint ap1 primary key(a1, a2, a3)", "42831" }, // GFXD does not allow ALTER on table w/data - will be different error than 42831
      { "insert into a values(1, NULL, 1)", null },
      { "drop table a", null },
      { "create table a (a1 int not null, a2 int default null, a3 int default null)", null },
      { "insert into a values(1,NULL,1)", null },
      { "alter table a add constraint ap1 primary key(a1, a2, a3)", "42831" },
      // defining primarykey column constraint for explicitly nullable column gives error
      { "create table a1(ac1 int null primary key)", "42831" },
      // defining primarykey table constraint on explicitly nullable columns give error
      { "create table a1(ac1 int null, ac2 int not null, primary key(ac1,ac2))", "42831" },
      { "create table a2(ac1 int null null)", "42Y49" },   
      // say not null, null and no null for a column. This is to make sure the flags stay proper for a column
      { "create table a3(ac1 int not null null not null)", "42X83" },
      // first statement says null and second one says not null. This is to make sure
      // the flag for the first one doesn't affect the second one ... seriously? They thought this could happen??
      { "create table a3(ac1 int default null)", null },
      { "create table a4(ac1 int not null)", null },
      // one column says null and second one says not null
      { "create table a5(ac1 int default null, ac2 int not null)", null },
      // statement1 says null, 2nd says nothing but says primary key
      { "create table a6(ac1 int default null)", null },
      { "create table a7(ac1 int not null primary key)", null },
      { "create table t (i int, i_d int default null, i_n int not null, s smallint, s_d smallint default null, s_n smallint not null)", null },
      //  insert non-nulls into null and non-null columns, etc etc.
      { "insert into t (i, i_d, i_n, s, s_d, s_n) values (1, 1, 1, 1, 1, 1)", null},
      { "insert into t values (null, null, 2, null, null, 2)", null},
      { "insert into t (i, i_n, s, s_d, s_n) values (3, 3, 3, 3, 3)", null },
      { "insert into t (i, i_d, i_n, s, s_n) values (4, 4, 4, 4, 4)", null },
      { "insert into t (i, i_n, s, s_n) values (5, 5, 5, 5)", null },
      { "insert into t (i, i_d, s, s_d) values (6, 6, 6, 6)", "23502" },
      { "insert into t (i_d, i_n, s_d, s_n) values (7, 7, 7, 7)", null },
      { "insert into t values (8, 8, null, 8, 8, 8)", "23502" },
      { "insert into t values (9, 9, 9, 9, 9, null)", "23502" },
      { "select * from t", new String[][] {
           {"1","1","1","1","1","1"},
           {null,null,"2",null,null,"2"},
           {"3",null,"3","3","3","3"},
           {"4","4","4","4",null,"4"},
           {"5",null,"5","5",null,"5"},
           {null,"7","7",null,"7","7"} } },
      // create a table with a non-null column with a default value of null and verify that nulls are not allowed
      { "create table s (x int default null not null, y int)", null },
      { "insert into s (y) values(1)", "23502" },
      { "select * from s", new String[0][0] },       // empty result set
      //  is null/is not null on an integer type
      { "create table u (c1 integer)", null },
      { "insert into u values null", null },
      { "insert into u values 1", null },
      { "insert into u values null", null },
      { "insert into u values 2", null },
      { "select * from u where c1 is null", new String[][] { {null},{null} } },
      { "select * from u where c1 is not null", new String[][] { {"1"},{"2"} } },
      // TODO - there are prepared statements in the SQL file here -- handle manually
      /* prepare p1 as 'select * from u where cast (? as varchar(1)) is null';
         execute p1 using 'values (''a'')';   -- empty results
         prepare p2 as 'select * from u where cast (? as varchar(1)) is not null';
         execute p2 using 'values (''a'')';  -- results 4 rows, (null),(1),(null),(2)  */
      { "select count(*) from u where c1 is null", new String[][] { {"2"} } },
      { "insert into u select * from (values null) as X", null },
      { "select count(*) from u where c1 is null", new String[][] { {"3"} } },
      // cleanup
      { "drop table t", null},
      { "drop table s", null},
      { "drop table u", null},
      { "drop table a", null},
      { "drop table a3", null},
      { "drop table a4", null},
      { "drop table a5", null},
      { "drop table a6", null},
      { "drop table a7", null}
    };

    // Do not use partitioning as default, use replicate
    // (Some results are expected to be different with partitioning)
    skipDefaultPartitioned = true;

    Connection conn = TestUtil.getConnection();
    Statement stmt = conn.createStatement();
    // Go through the array, execute each string[0], check sqlstate [1]
    // This will fail on the first one that succeeds where it shouldn't
    // or throws unknown exception
    JDBC.SQLUnitTestHelper(stmt,Script_NullsUT);
  }
  
  // This test is the script enhanced with partitioning
  public void testLangScript_NullsWithPartitioning() throws Exception
  {
    // This form of the nulls.sql test has partitioning clauses
    Object[][] Script_NullsUTPartitioning = {
      // this test shows the current supported null value functionality
      // First, trying to define null and not null for a column
      { "create table a(a1 int null not null) partition by column(a1)", "42X83" },
      { "create table a(a1 int not null null) partition by column(a1)", "42X83" },
      { "create table a(a1 int not null , a2 int not null) partition by column(a1)", null },
      // alter table adding explicitly nullable column and primary key column constraint on it fails
      { "alter table a add column a3 int null constraint ap1 primary key", "42831" },
      // alter table table level primary key constraint on nullable column doesn't give an error
      { "alter table a add constraint ap1 primary key(a1,a2)", null },
      { "drop table a", null },
      // create table with not null column and unique key should work
      { "create table a (a int not null constraint auniq unique ) partition by column(a)", null },
      { "insert into a values (1)", null },
      { "insert into a values (1)", "23505" },
      { "drop table a", null },
      // alter nullability on a unique column should fail
      { "create table a ( a int not null unique) partition by column(a)", null},
      { "alter table a modify a null", "42X01" },   // GFXD doesn't support modify keyword
      { "drop table a", null},
      // try adding a primary key where there is null data, this should error
      // FIXME - GFXD does not allow ALTER on a table with data, some of these tests need to be redone
      //         after full ALTER TABLE support is added
      { "create table a (a1 int not null, a2 int) partition by column(a1)", null },
      { "insert into a values(1, NULL)", null },
      { "alter table a add constraint ap1 primary key(a1, a2)", "42831" },
      { "drop table a", null },
      { "create table a (a1 int, a2 int, a3 int) partition by column(a1)", null },
      { "alter table a add constraint ap1 primary key(a1, a2, a3)", "42831" },
      { "drop table a", null },
      { "create table a (a1 int not null, a2 int, a3 int) partition by column(a1)", null },
      { "insert into a values(1,1,1)", null },
      { "alter table a add constraint ap1 primary key(a1, a2, a3)", "42831" }, // GFXD does not allow ALTER on table w/data - will be different error than 42831
      { "insert into a values(1, NULL, 1)", null },
      { "drop table a", null },
      { "create table a (a1 int not null, a2 int default null, a3 int default null) partition by column(a1)", null },
      { "insert into a values(1,NULL,1)", null },
      { "alter table a add constraint ap1 primary key(a1, a2, a3)", "42831" },
      // defining primarykey column constraint for explicitly nullable column gives error
      { "create table a1(ac1 int null primary key) partition by column(ac1)", "42831" },
      // defining primarykey table constraint on explicitly nullable columns give error
      { "create table a1(ac1 int null, ac2 int not null, primary key(ac1,ac2)) partition by column(ac1)", "42831" },
      { "create table a2(ac1 int null null) partition by column(ac1)", "42Y49" },   
      // say not null, null and no null for a column. This is to make sure the flags stay proper for a column
      { "create table a3(ac1 int not null null not null) partition by column(ac1)", "42X83" },
      // first statement says null and second one says not null. This is to make sure
      // the flag for the first one doesn't affect the second one ... seriously? They thought this could happen??
      { "create table a3(ac1 int default null) partition by column(ac1)", null },
      { "create table a4(ac1 int not null) partition by column(ac1)", null },
      // one column says null and second one says not null
      { "create table a5(ac1 int default null, ac2 int not null) partition by column(ac1)", null },
      // statement1 says null, 2nd says nothing but says primary key
      { "create table a6(ac1 int default null) partition by column(ac1)", null },
      { "create table a7(ac1 int not null primary key) partition by primary key", null },
      { "create table t (i int, i_d int default null, i_n int not null, s smallint, s_d smallint default null, s_n smallint not null) partition by column(i)", null },
      //  insert non-nulls into null and non-null columns, etc etc.
      { "insert into t (i, i_d, i_n, s, s_d, s_n) values (1, 1, 1, 1, 1, 1)", null},
      { "insert into t values (null, null, 2, null, null, 2)", null},
      { "insert into t (i, i_n, s, s_d, s_n) values (3, 3, 3, 3, 3)", null },
      { "insert into t (i, i_d, i_n, s, s_n) values (4, 4, 4, 4, 4)", null },
      { "insert into t (i, i_n, s, s_n) values (5, 5, 5, 5)", null },
      { "insert into t (i, i_d, s, s_d) values (6, 6, 6, 6)", "23502" },
      { "insert into t (i_d, i_n, s_d, s_n) values (7, 7, 7, 7)", null },
      { "insert into t values (8, 8, null, 8, 8, 8)", "23502" },
      { "insert into t values (9, 9, 9, 9, 9, null)", "23502" },
      { "select * from t", new String[][] {
           {"1","1","1","1","1","1"},
           {null,null,"2",null,null,"2"},
           {"3",null,"3","3","3","3"},
           {"4","4","4","4",null,"4"},
           {"5",null,"5","5",null,"5"},
           {null,"7","7",null,"7","7"} } },
      // create a table with a non-null column with a default value of null and verify that nulls are not allowed
      { "create table s (x int default null not null, y int) partition by column(x)", null },
      { "insert into s (y) values(1)", "23502" },
      { "select * from s", new String[0][0] },       // empty result set
      //  is null/is not null on an integer type
      { "create table u (c1 integer) partition by column(c1)", null },
      { "insert into u values null", null },
      { "insert into u values 1", null },
      { "insert into u values null", null },
      { "insert into u values 2", null },
      { "select * from u where c1 is null", new String[][] { {null},{null} } },
      { "select * from u where c1 is not null", new String[][] { {"1"},{"2"} } },
      // TODO - there are prepared statements in the SQL file here -- handle manually
      /* prepare p1 as 'select * from u where cast (? as varchar(1)) is null';
         execute p1 using 'values (''a'')';   -- empty results
         prepare p2 as 'select * from u where cast (? as varchar(1)) is not null';
         execute p2 using 'values (''a'')';  -- results 4 rows, (null),(1),(null),(2)  */
      { "select count(*) from u where c1 is null", new String[][] { {"2"} } },
      { "insert into u select * from (values null) as X", null },
      { "select count(*) from u where c1 is null", new String[][] { {"3"} } },
      // cleanup (some tables do not exist)
      { "drop table t", null},
      { "drop table s", null},
      { "drop table u", null},
      { "drop table a", null},
      { "drop table a3", null},
      { "drop table a4", null},
      { "drop table a5", null},
      { "drop table a6", null},
      { "drop table a7", null}
    };

    Connection conn = TestUtil.getConnection();
    Statement stmt = conn.createStatement();
    // Go through the array, execute each string[0], check sqlstate [1]
    // This will fail on the first one that succeeds where it shouldn't
    // or throws unknown exception
    JDBC.SQLUnitTestHelper(stmt,Script_NullsUTPartitioning);

  }
}
