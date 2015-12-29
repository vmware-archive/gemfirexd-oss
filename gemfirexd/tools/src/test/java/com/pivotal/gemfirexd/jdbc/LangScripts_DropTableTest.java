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

public class LangScripts_DropTableTest extends JdbcTestBase {

  public static void main(String[] args) {
    TestRunner.run(new TestSuite(LangScripts_DropTableTest.class));
  }
  
  public LangScripts_DropTableTest(String name) {
    super(name); 
  }

  // This test is the as-is LangScript conversion, without any partitioning clauses
  public void testLangScript_DropTableTestNoPartitioning() throws Exception
  {
    // This is a JUnit conversion of the Derby Lang dropTable.sql script
    // without any GemFireXD extensions
	  
    // Catch exceptions from illegal syntax
    // Tests still not fixed marked FIXME
	  
    // Array of SQL text to execute and sqlstates to expect
    // The first object is a String, the second is either 
    // 1) null - this means query returns no rows and throws no exceptions
    // 2) a string - this means query returns no rows and throws expected SQLSTATE
    // 3) a String array - this means query returns rows which must match (unordered) given resultset
    //       - for an empty result set, an uninitialized size [0][0] array is used
    Object[][] Script_DropTableUT = {
	{ "create table t1 ( a int)", null },
	{ "drop table t1", null },
	{ "select * from t1", "42X05" },
	{ "create table t1 (a int)", null },
	{ "drop table t1", null },
	{ "select * from t1", "42X05" },
	{ "create table t1 (a int not null unique)", null },
	{ "drop table t1", null },
	{ "select * from t1", "42X05" },
	{ "create table t1 (a int not null unique)", null },
	{ "drop table t1", null },
	{ "select * from t1", "42X05" },
	{ "create table t1 ( a int not null primary key)", null },
	{ "drop table t1", null },
	{ "select * from t1", "42X05" },
	{ "create table t1 ( a int not null primary key)", null },
	{ "drop table t1", null },
	{ "select * from t1", "42X05" },
	{ "create table t1 ( a int check(a > 0))", null },
	{ "drop table t1", null },
	{ "select * from t1", "42X05" },
	{ "create table t1 ( a int check(a > 0))", null },
	{ "drop table t1", null },
	{ "select * from t1", "42X05" },
	{ "create table t1 ( a int)", null },
	{ "create index t1index on t1(a)", null },
	{ "drop table t1", null },
	{ "select * from t1", "42X05" },
	{ "create table t1 ( a int)", null },
	{ "create index t1index on t1(a)", null },
	{ "drop table t1", null },
	{ "select * from t1", "42X05" },
	{ "create table t1(a int not null primary key)", null },
	{ "create table t2(a int constraint reft1a references t1(a))", null },
	// this should fail with a dependent constraint error
	{ "drop table t1", "X0Y25" },
	{ "drop table t1", "X0Y25" },
	{ "alter table t2 drop constraint reft1a", null },
	{ "drop table t1", null },
	{ "select * from t1", "42X05" },
	{ "insert into t2 values(1)", null },
	{ "drop table t2", null },
	{ "create table t1(a int, b int)", null },
	{ "create table t2(c int, d int)", null },
	{ "create view vt1a as select a from t1", null },
	{ "create view vt1b as select b from t1", null },
	{ "create view vt1t2 as select * from t1, t2", null },
	{ "create view vvt1a as select * from vt1a", null },
	{ "create view vvvt1a as select * from vvt1a", null },
	// this should fail with view being a dependent object
	{ "drop table t1", "X0Y23" },
	{ "drop table t1", "X0Y23" },
	{ "drop view vvvt1a", null },
	{ "drop view vvt1a", null },
	{ "drop view vt1t2", null },
	{ "drop view vt1b", null },
	{ "drop view vt1a", null },
	{ "drop table t1", null },
	{ "select * from vt1a", "42X05" },
	{ "select * from vt1b", "42X05" },
	{ "select * from vt1t2", "42X05" },
	{ "select * from vvt1a", "42X05" },
	{ "select * from vvvt1a", "42X05" },
	{ "drop table t2", null },
	// TODO : prepare-based test commented out
/*	{ "create table t1(a int)", null },
	{ "prepare t1stmt as 'select * from t1'", null },
	{ "drop table t1", null },
	{ "execute t1stmt", "42X05" },
	{ "remove t1stmt", null },
*/
	{ "create table t1(a int)", null },
	{ "create table t2(a int)", null },
	{ "create trigger t1trig after insert on t1 for each row insert into t2 values(1)", null },
	{ "drop table t1", null },
	{ "select * from t1", "42X05" },
	{ "create table t1(a int)", null },
	{ "create trigger t1trig after insert on t1 for each row insert into t2 values(1)", null },
	{ "drop table t1", null },
	{ "select * from t1", "42X05" },
	{ "drop table t2", null },
	{ "create table t1(a int)", null },
	{ "create table t2(a int)", null },
	{ "create trigger t2trig after insert on t2 for each row insert into t1 values(1)", null },
	{ "drop table t1", null },
	{ "insert into t2 values(1)", "42X05" },
	{ "drop table t2", null },
	{ "create table t1(a int)", null },
	{ "create table t2(a int)", null },
	{ "create trigger t2trig after insert on t2 for each row insert into t1 values(1)", null },
	{ "drop table t1", null },
	{ "insert into t2 values(1)", "42X05" },
	{ "drop table t2", null },
	{ "create table t1(a int)", null },
	{ "create view vt1 as select * from t1", null },
	{ "create view vvt1 as select * from vt1", null },
	{ "drop view vt1", "X0Y23" },
	{ "create table t2(a int not null primary key)", null },
	{ "create table reft2(a int constraint ref1 references t2)", null },
	{ "select count(*) from sys.sysconglomerates c, sys.systables t where t.tableid = c.tableid and t.tablename = 'REFT2'", new String[][] { {"2"} } },
	{ "alter table reft2 drop constraint ref1", null },
	{ "drop table t2", null },
	{ "select count(*) from sys.sysconglomerates c, sys.systables t where t.tableid = c.tableid and t.tablename = 'REFT2'", new String[][] { {"1"} } }
    };

    // Do not use partitioning as default, use replicate
    // (Some results are expected to be different with partitioning)
    skipDefaultPartitioned = true;

    Connection conn = TestUtil.getConnection();
    Statement stmt = conn.createStatement();
    // Go through the array, execute each string[0], check sqlstate [1]
    // This will fail on the first one that succeeds where it shouldn't
    // or throws unknown exception
    JDBC.SQLUnitTestHelper(stmt,Script_DropTableUT);
  }
  
  // This test is the script enhanced with partitioning
  public void testLangScript_DropTableWithPartitioning() throws Exception
  {
    // This form of the dropTable.sql test has partitioning enabled
    // But have not added PARTITION clauses to each create table statement as there are the whole test
    Object[][] Script_DropTableUTPartitioning = {
	{ "create table t1 ( a int)", null },
	{ "drop table t1", null },
	{ "select * from t1", "42X05" },
	{ "create table t1 (a int)", null },
	{ "drop table t1", null },
	{ "select * from t1", "42X05" },
	{ "create table t1 (a int not null unique)", null },
	{ "drop table t1", null },
	{ "select * from t1", "42X05" },
	{ "create table t1 (a int not null unique)", null },
	{ "drop table t1", null },
	{ "select * from t1", "42X05" },
	{ "create table t1 ( a int not null primary key)", null },
	{ "drop table t1", null },
	{ "select * from t1", "42X05" },
	{ "create table t1 ( a int not null primary key)", null },
	{ "drop table t1", null },
	{ "select * from t1", "42X05" },
	{ "create table t1 ( a int check(a > 0))", null },
	{ "drop table t1", null },
	{ "select * from t1", "42X05" },
	{ "create table t1 ( a int check(a > 0))", null },
	{ "drop table t1", null },
	{ "select * from t1", "42X05" },
	{ "create table t1 ( a int)", null },
	{ "create index t1index on t1(a)", null },
	{ "drop table t1", null },
	{ "select * from t1", "42X05" },
	{ "create table t1 ( a int)", null },
	{ "create index t1index on t1(a)", null },
	{ "drop table t1", null },
	{ "select * from t1", "42X05" },
	{ "create table t1(a int not null primary key)", null },
	{ "create table t2(a int constraint reft1a references t1(a))", null },
	// this should fail with a dependent constraint error
	{ "drop table t1", "X0Y98" }, 	// GemFireXD throws colocation error here, as expected
	{ "drop table t1", "X0Y98" },
	{ "alter table t2 drop constraint reft1a", null },
        // [sumedh] default partitioning/colocation does not change now
        // on changing constraints so it throws colocation error now
	{ "drop table t1", "X0Y98" },
	{ "insert into t2 values(1)", null },
	{ "drop table t2", null },
        { "drop table t1", null },
        { "select * from t1", "42X05" },
	{ "create table t1(a int, b int)", null },
	{ "create table t2(c int, d int)", null },
	{ "create view vt1a as select a from t1", null },
	{ "create view vt1b as select b from t1", null },
	{ "create view vt1t2 as select * from t1, t2", null },
	{ "create view vvt1a as select * from vt1a", null },
	{ "create view vvvt1a as select * from vvt1a", null },
	// this should fail with view being a dependent object
	{ "drop table t1", "X0Y23" },
	{ "drop table t1", "X0Y23" },
	{ "drop view vvvt1a", null },
	{ "drop view vvt1a", null },
	{ "drop view vt1t2", null },
	{ "drop view vt1b", null },
	{ "drop view vt1a", null },
	{ "drop table t1", null },
	{ "select * from vt1a", "42X05" },
	{ "select * from vt1b", "42X05" },
	{ "select * from vt1t2", "42X05" },
	{ "select * from vvt1a", "42X05" },
	{ "select * from vvvt1a", "42X05" },
	{ "drop table t2", null },
	// TODO : prepare-based test commented out
/*	{ "create table t1(a int)", null },
	{ "prepare t1stmt as 'select * from t1'", null },
	{ "drop table t1", null },
	{ "execute t1stmt", "42X05" },
	{ "remove t1stmt", null },
*/
	{ "create table t1(a int)", null },
	{ "create table t2(a int)", null },
	{ "create trigger t1trig after insert on t1 for each row insert into t2 values(1)", null },
	{ "drop table t1", null },
	{ "select * from t1", "42X05" },
	{ "create table t1(a int)", null },
	{ "create trigger t1trig after insert on t1 for each row insert into t2 values(1)", null },
	{ "drop table t1", null },
	{ "select * from t1", "42X05" },
	{ "drop table t2", null },
	{ "create table t1(a int)", null },
	{ "create table t2(a int)", null },
	{ "create trigger t2trig after insert on t2 for each row insert into t1 values(1)", null },
	{ "drop table t1", null },
	{ "insert into t2 values(1)", "42X05" },
	{ "drop table t2", null },
	{ "create table t1(a int)", null },
	{ "create table t2(a int)", null },
	{ "create trigger t2trig after insert on t2 for each row insert into t1 values(1)", null },
	{ "drop table t1", null },
	{ "insert into t2 values(1)", "42X05" },
	{ "drop table t2", null },
	{ "create table t1(a int)", null },
	{ "create view vt1 as select * from t1", null },
	{ "create view vvt1 as select * from vt1", null },
	{ "drop view vt1", "X0Y23" },
	{ "create table t2(a int not null primary key)", null },
	{ "create table reft2(a int constraint ref1 references t2)", null },
	{ "select count(*) from sys.sysconglomerates c, sys.systables t where t.tableid = c.tableid and t.tablename = 'REFT2'", new String[][] { {"2"} } },
	{ "alter table reft2 drop constraint ref1", null },
        // [sumedh] default partitioning/colocation does not change now
        // on changing constraints so it throws colocation error now
	{ "drop table t2", "X0Y98" },
	{ "select count(*) from sys.sysconglomerates c, sys.systables t where t.tableid = c.tableid and t.tablename = 'REFT2'", new String[][] { {"1"} } }
    };

    Connection conn = TestUtil.getConnection();
    Statement stmt = conn.createStatement();
    // Go through the array, execute each string[0], check sqlstate [1]
    // This will fail on the first one that succeeds where it shouldn't
    // or throws unknown exception
    JDBC.SQLUnitTestHelper(stmt,Script_DropTableUTPartitioning);

  }
}
