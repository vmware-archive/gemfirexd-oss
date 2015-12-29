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

public class LangScripts_SchemasTest extends JdbcTestBase {

  public static void main(String[] args) {
    TestRunner.run(new TestSuite(LangScripts_SchemasTest.class));
  }
  
  public LangScripts_SchemasTest(String name) {
    super(name); 
  }

  // This test is the as-is LangScript conversion, without any partitioning clauses
  public void testLangScript_SchemasTestNoPartitioning() throws Exception
  {
    // This is a JUnit conversion of the Derby Lang Schemas.sql script
    // without any GemFireXD extensions
	  
    // Catch exceptions from illegal syntax
    // Tests still not fixed marked FIXME
	  
    // Array of SQL text to execute and sqlstates to expect
    // The first object is a String, the second is either 
    // 1) null - this means query returns no rows and throws no exceptions
    // 2) a string - this means query returns no rows and throws expected SQLSTATE
    // 3) a String array - this means query returns rows which must match (unordered) given resultset
    //       - for an empty result set, an uninitialized size [0][0] array is used
    Object[][] Script_SchemasUT = {
	{ "create table myschem.t(c int)", null },
	{ "insert into t values (1)", "42X05" },
	{ "insert into blah.t values (2)", "42Y07" },
	{ "insert into blah.blah.t values (3)", "42X01" },
	{ "insert into blah.blah.blah.t values (3)", "42X01" },
	{ "create table mycat.myschem.s(c int)", "42X01" },
	{ "create table myworld.mycat.myschem.s(c int)", "42X01" },
	{ " create table myschem.s(c int)", null },
	{ "insert into s values (1)", "42X05" },
	{ "insert into honk.s values (2)", "42Y07" },
	{ "insert into honk.blat.s values (3)", "42X01" },
	{ "insert into loud.honk.blat.s values (4)", "42X01" },
	{ "drop table xyzzy.t", "42Y07" },
	{ "drop table goodness.gosh.s", "42X01" },
	{ "drop table gosh.s", "42Y07" },
	{ "create table mytab (i int)", null },
	{ "create table APP.mytab2 (i int)", null },
	{ "insert into mytab values 1,2,3", null },
     { "insert into APP.mytab2 values 1,2,3", null },
	{ "select i, mytab.i from mytab", new String[][] { {"1","1"},{"2","2"},{"3","3"} } },
     { "select APP.mytab2.i from APP.mytab2", new String[][] { {"1"},{"2"},{"3"} } },
     { "select APP.mytab2.i from mytab2", new String[][] { {"1"},{"2"},{"3"} } },
     { "select mytab2.i from APP.mytab2", new String[][] { {"1"},{"2"},{"3"} } },
     { "select m.i from APP.mytab2 m", new String[][] { {"1"},{"2"},{"3"} } },
     { "select nocatalogs.APP.mytab.i from mytab2", "42X04" },
     { "drop table mytab", null },
     { "drop table APP.mytab2", null },
	{ "create schema app", "X0Y68" },
     { "create schema sys", "42939" },
     { "drop schema does_not_exist RESTRICT", "42Y07" },
     { "create schema app", "X0Y68" },
     { "create schema APP", "X0Y68" },
     { "create schema SYS", "42939" },
     { "create schema sysibm", "42939" },
     { "create schema syscat", "42939" },
     { "create schema sysfun", "42939" },
     { "create schema sysproc", "42939" },
     { "create schema sysstat", "42939" },
     { "create schema syscs_diag", "42939" },
     { "create schema syscs_util", "42939" },
     { "create schema nullid", "X0Y68" },
     { "create schema sqlj", "X0Y68" },
     { "create table syscat.foo1 (a int)", "42X62" },
     { "create table sysfun.foo2 (a int)", "42X62" },
     { "create table sysproc.foo3 (a int)", "42X62" },
     { "create table sysstat.foo4 (a int)", "42X62" },
     { "create table syscs_diag.foo6 (a int)", "42X62" },
     { "create table nullid.foo7 (a int)", "42X62" },
     { "create table sysibm.foo8 (a int)", "42X62" },
     { "create table sqlj.foo8 (a int)", "42X62" },
     { "create table syscs_util.foo9 (a int)", "42X62" },
     { "create table SYSCAT.foo1 (a int)", "42X62" },
     { "create table SYSFUN.foo2 (a int)", "42X62" },
     { "create table SYSPROC.foo3 (a int)", "42X62" },
     { "create table SYSSTAT.foo4 (a int)", "42X62" },
     { "create table SYSCS_DIAG.foo6 (a int)", "42X62" },
     { "create table SYSIBM.foo8 (a int)", "42X62" },
     { "create table SQLJ.foo8 (a int)", "42X62" },
     { "create table SYSCS_UTIL.foo9 (a int)", "42X62" },
     { "drop schema app RESTRICT", null },   // GemFireXD allows dropping default APP schema, Derby did not!
     { "drop schema APP RESTRICT", "42Y07" },
     { "drop schema sys RESTRICT", "42Y67" },
     { "drop schema SYS RESTRICT", "42Y67" },
     { "drop schema sysibm RESTRICT", "42Y67" },
     { "drop schema syscat RESTRICT", "42Y67" },
     { "drop schema sysfun RESTRICT", "42Y67" },
     { "drop schema sysproc RESTRICT", "42Y67" },
     { "drop schema sysstat RESTRICT", "42Y67" },
     { "drop schema syscs_diag RESTRICT", "42Y67" },
     { "drop schema syscs_util RESTRICT", "42Y67" },
     { "drop schema nullid RESTRICT", "42Y67" },
     { "drop schema sqlj RESTRICT", "42Y67" },
     { "create schema app", null },
     { "set schema app", null },
     { "create table test (a int)", null },
     { "set schema syscat", null },
     { "create table foo1 (a int)", "42X62" },
     { "create view foo1 as select * from app.test", "42X62" },
     { "set schema sysfun", null },
     { "create table foo1 (a int)", "42X62" },
     { "create view foo1 as select * from app.test", "42X62" },
     { "set schema sysproc", null },
     { "create table foo1 (a int)", "42X62" },
     { "create view foo1 as select * from app.test", "42X62" },
     { "set schema sysstat", null },
     { "create table foo1 (a int)", "42X62" },
     { "create view foo1 as select * from app.test", "42X62" },
     { "set schema sysstat", null },
     { "create table foo1 (a int)", "42X62" },
     { "create view foo1 as select * from app.test", "42X62" },
     { "set schema syscs_diag", null },
     { "create table foo1 (a int)", "42X62" },
     { "create view foo1 as select * from app.test", "42X62" },
     { "set schema syscs_util", null },
     { "create table foo1 (a int)", "42X62" },
     { "create view foo1 as select * from app.test", "42X62" },
     { "set schema nullid", null },
     { "create table foo1 (a int)", "42X62" },
     { "create view foo1 as select * from app.test", "42X62" },
     { "set schema sysibm", null },
     { "create table foo1 (a int)", "42X62" },
     { "create view foo1 as select * from app.test", "42X62" },
     { "set schema sqlj", null },
     { "create table foo1 (a int)", "42X62" },
     { "create view foo1 as select * from app.test", "42X62"},
     { "set schema SYSCAT", null },
     { "create table foo1 (a int)", "42X62" },
     { "create view foo1 as select * from app.test", "42X62" },
     { "set schema SYSFUN", null },
     { "create table foo1 (a int)", "42X62" },
     { "create view foo1 as select * from app.test", "42X62" },
     { "set schema SYSPROC", null },
     { "create table foo1 (a int)", "42X62" },
     { "create view foo1 as select * from app.test", "42X62" },
     { "set schema SYSSTAT", null },
     { "create table foo1 (a int)", "42X62" },
     { "create view foo1 as select * from app.test", "42X62" },
     { "set schema SYSSTAT", null },
     { "create table foo1 (a int)", "42X62" },
     { "create view foo1 as select * from app.test", "42X62" },
     { "set schema SYSCS_DIAG", null },
     { "create table foo1 (a int)", "42X62" },
     { "create view foo1 as select * from app.test", "42X62" },
     { "set schema SYSCS_UTIL", null },
     { "create table foo1 (a int)", "42X62" },
     { "create view foo1 as select * from app.test", "42X62" },
     { "set schema NULLID", null },
     { "create table foo1 (a int)", "42X62" },
     { "create view foo1 as select * from app.test", "42X62" },
     { "set schema SYSIBM", null },
     { "create table foo1 (a int)", "42X62" },
     { "create view foo1 as select * from app.test", "42X62" },
     { "set schema SQLJ", null },
     { "create table foo1 (a int)", "42X62" },
     { "create view foo1 as select * from app.test", "42X62" },
     { "set schema app", null },
     { "create table t1 (c1 int)", null },
     { "create trigger sysblah.trig1 after update of c1 on t1 for each row insert into t1 values 1", "42X62" },
     { "create procedure sysblah.dummy() language java external name 'NotReallyThere.NoMethod' parameter style java", "42X62" },
     { "drop table t1", null },
     { "create schema test", null },
     { "create schema test", "X0Y68" },
     { "select schemaname, authorizationid from sys.sysschemas 	where CAST(schemaname AS VARCHAR(128)) = 'TEST'", new String[][] {
		{"TEST","APP"} } },                                                                                                   
     { "set schema test", null },
     { "create table sampletab (c1 int constraint st_c1 check (c1 > 1), c2 char(20))", null },
     { "insert into sampletab values (1,'in schema: TEST')", "23513" },
     { "insert into sampletab values (2,'in schema: TEST')", null },
     { "select schemaname, tablename, descriptor from sys.sysschemas s, sys.sysconglomerates c , sys.systables t where CAST(t.tablename AS VARCHAR(128)) = 'SAMPLETAB' and s.schemaid = c.schemaid and c.tableid = t.tableid", new String[][] {
		{"TEST","SAMPLETAB",null} } },
     { "create index ixsampletab on sampletab(c1)", null },
     { "create index ix2sampletab on test.sampletab(c1)", null },
     { "create view vsampletab as select * from sampletab", null },
     { "create view v2sampletab as select * from test.sampletab", null },
     { "set schema APP", null },
     { "create table sampletab (c1 int constraint st_c1 check(c1 > 1), c2 char(20))", null },
     { "insert into sampletab values (2,'in schema: APP')", null },
     { "select schemaname, tablename, descriptor as descr from sys.sysschemas s, sys.sysconglomerates c , sys.systables t where CAST(t.tablename AS VARCHAR(128)) = 'SAMPLETAB' and s.schemaid = c.schemaid and c.tableid = t.tableid order by schemaname, tablename", new String[][] {
		{"APP","SAMPLETAB",null},{"TEST","SAMPLETAB","LOCALSORTEDMAP (1)"},{"TEST","SAMPLETAB",null} } },   // GEMFIREXD uses sortedmap, not btree indexes
     { "select * from sampletab", new String[][] { {"2","in schema: APP"} } },
     { "select * from test.sampletab", new String[][] { {"2","in schema: TEST"} } },
     { "drop schema test RESTRICT", "X0Y54" },
     { "drop view test.vsampletab", null },
     { "drop view test.v2sampletab", null },
     { "drop index test.ixsampletab", null },
     { "drop index test.ix2sampletab", "42X65" },
     { "drop table sampletab", null },
	{ "drop table test.sampletab", null },
     { "drop schema test RESTRICT", null },
	{ "create schema x", null },
     { "set schema x", null },
     { "create view vx as select * from sys.sysschemas", null },
     { "drop schema x RESTRICT", "X0Y54"},
     { "drop view x.vx", null },
     { "create table x (x int)", null },
     { "drop schema x restrict", "X0Y54" },
     { "drop table x.x", null },
     { "drop schema x cascade", "42X01" },
     { "set schema app", null },
     { "drop schema x restrict", null },
     { "create schema test", null },
     { "set schema test", null },
     { "create table s (i int, s smallint, c char(30), vc char(30))", null },
     { "create table t (i int, s smallint, c char(30), vc char(30))", null },
     { "create table tt (ii int, ss smallint, cc char(30), vcvc char(30))", null },
     { "create table ttt (iii int, sss smallint, ccc char(30), vcvcvc char(30))", null },
     { "insert into s values (null, null, null, null)", null },
     { "insert into s values (0, 0, '0', '0')", null },
     { "insert into s values (1, 1, '1', '1')", null },
     { "insert into t values (null, null, null, null)", null },
     { "insert into t values (0, 0, '0', '0')", null },
     { "insert into t values (1, 1, '1', '1')", null },
     { "insert into t values (1, 1, '1', '1')", null },
     { "insert into tt values (null, null, null, null)", null },
     { "insert into tt values (0, 0, '0', '0')", null },
     { "insert into tt values (1, 1, '1', '1')", null },
     { "insert into tt values (1, 1, '1', '1')", null },
     { "insert into tt values (2, 2, '2', '2')", null },
     { "insert into ttt values (null, null, null, null)", null },
     { "insert into ttt values (11, 11, '11', '11')", null },
     { "insert into ttt values (11, 11, '11', '11')", null },
     { "insert into ttt values (22, 22, '22', '22')", null },
     { "set schema app", null },
     { "insert into test.t values (2, 2, '2', '2')", null },
     { "update test.t set s = 2 where i = 2", null },
     { "update test.t set s = 2 where test.t.i = 2", null },
     { "delete from test.t where i = 1", null },
     { "select * from test.t", new String[][] {
		{null,null,null,null},{"0","0","0","0"},{"2","2","2","2"} } },
     { "insert into test.t values (1, 1, '1', '1')", null },
     { "insert into test.t values (1, 1, '1', '1')", null },
     { "select * from test.t t1", new String[][] {
		{null,null,null,null},{"0","0","0","0"},{"2","2","2","2"},{"1","1","1","1"},{"1","1","1","1"} } },
     { "select * from test.s where exists (select test.s.* from test.t)", new String[][] {
		{null,null,null,null},{"0","0","0","0"},{"1","1","1","1"} } },
     { "DECLARE GLOBAL TEMPORARY TABLE SESSION.ISCT(c21 int) on commit delete rows not logged", null },
     { "select count(*) from SYS.SYSSCHEMAS WHERE CAST(SCHEMANAME AS VARCHAR(128)) = 'SESSION'", new String[][] { {"0"} } },
     { "drop table SESSION.ISCT", null },
     { "create schema SYSDJD", "42939" },
     { "drop schema SYSDJD restrict", "42Y07" }
   };

    // Do not use partitioning as default, use replicate
    // (Some results are expected to be different with partitioning)
    skipDefaultPartitioned = true;

    Connection conn = TestUtil.getConnection();
    Statement stmt = conn.createStatement();
    // Go through the array, execute each string[0], check sqlstate [1]
    // This will fail on the first one that succeeds where it shouldn't
    // or throws unknown exception
    JDBC.SQLUnitTestHelper(stmt,Script_SchemasUT);
  }
  
  // This test is the script enhanced with partitioning
  public void testLangScript_SchemasWithPartitioning() throws Exception
  {
    // This form of the Schemas.sql test has partitioning activated
    // TODO : Create specialized partitioning for these tables for Schemas
    //  Currently, default partitioning is active
    Object[][] Script_SchemasUTPartitioning = {
	{ "create table myschem.t(c int)", null },
	{ "insert into t values (1)", "42X05" },
	{ "insert into blah.t values (2)", "42Y07" },
	{ "insert into blah.blah.t values (3)", "42X01" },
	{ "insert into blah.blah.blah.t values (3)", "42X01" },
	{ "create table mycat.myschem.s(c int)", "42X01" },
	{ "create table myworld.mycat.myschem.s(c int)", "42X01" },
	{ " create table myschem.s(c int)", null },
	{ "insert into s values (1)", "42X05" },
	{ "insert into honk.s values (2)", "42Y07" },
	{ "insert into honk.blat.s values (3)", "42X01" },
	{ "insert into loud.honk.blat.s values (4)", "42X01" },
	{ "drop table xyzzy.t", "42Y07" },
	{ "drop table goodness.gosh.s", "42X01" },
	{ "drop table gosh.s", "42Y07" },
	{ "create table mytab (i int)", null },
	{ "create table APP.mytab2 (i int)", null },
	{ "insert into mytab values 1,2,3", null },
     { "insert into APP.mytab2 values 1,2,3", null },
	{ "select i, mytab.i from mytab", new String[][] { {"1","1"},{"2","2"},{"3","3"} } },
     { "select APP.mytab2.i from APP.mytab2", new String[][] { {"1"},{"2"},{"3"} } },
     { "select APP.mytab2.i from mytab2", new String[][] { {"1"},{"2"},{"3"} } },
     { "select mytab2.i from APP.mytab2", new String[][] { {"1"},{"2"},{"3"} } },
     { "select m.i from APP.mytab2 m", new String[][] { {"1"},{"2"},{"3"} } },
     { "select nocatalogs.APP.mytab.i from mytab2", "42X04" },
     { "drop table mytab", null },
     { "drop table APP.mytab2", null },
	{ "create schema app", "X0Y68" },
     { "create schema sys", "42939" },
     { "drop schema does_not_exist RESTRICT", "42Y07" },
     { "create schema app", "X0Y68" },
     { "create schema APP", "X0Y68" },
     { "create schema SYS", "42939" },
     { "create schema sysibm", "42939" },
     { "create schema syscat", "42939" },
     { "create schema sysfun", "42939" },
     { "create schema sysproc", "42939" },
     { "create schema sysstat", "42939" },
     { "create schema syscs_diag", "42939" },
     { "create schema syscs_util", "42939" },
     { "create schema nullid", "X0Y68" },
     { "create schema sqlj", "X0Y68" },
     { "create table syscat.foo1 (a int)", "42X62" },
     { "create table sysfun.foo2 (a int)", "42X62" },
     { "create table sysproc.foo3 (a int)", "42X62" },
     { "create table sysstat.foo4 (a int)", "42X62" },
     { "create table syscs_diag.foo6 (a int)", "42X62" },
     { "create table nullid.foo7 (a int)", "42X62" },
     { "create table sysibm.foo8 (a int)", "42X62" },
     { "create table sqlj.foo8 (a int)", "42X62" },
     { "create table syscs_util.foo9 (a int)", "42X62" },
     { "create table SYSCAT.foo1 (a int)", "42X62" },
     { "create table SYSFUN.foo2 (a int)", "42X62" },
     { "create table SYSPROC.foo3 (a int)", "42X62" },
     { "create table SYSSTAT.foo4 (a int)", "42X62" },
     { "create table SYSCS_DIAG.foo6 (a int)", "42X62" },
     { "create table SYSIBM.foo8 (a int)", "42X62" },
     { "create table SQLJ.foo8 (a int)", "42X62" },
     { "create table SYSCS_UTIL.foo9 (a int)", "42X62" },
     { "drop schema app RESTRICT", null },   // GemFireXD allows dropping default APP schema, Derby did not!
     { "drop schema APP RESTRICT", "42Y07" },
     { "drop schema sys RESTRICT", "42Y67" },
     { "drop schema SYS RESTRICT", "42Y67" },
     { "drop schema sysibm RESTRICT", "42Y67" },
     { "drop schema syscat RESTRICT", "42Y67" },
     { "drop schema sysfun RESTRICT", "42Y67" },
     { "drop schema sysproc RESTRICT", "42Y67" },
     { "drop schema sysstat RESTRICT", "42Y67" },
     { "drop schema syscs_diag RESTRICT", "42Y67" },
     { "drop schema syscs_util RESTRICT", "42Y67" },
     { "drop schema nullid RESTRICT", "42Y67" },
     { "drop schema sqlj RESTRICT", "42Y67" },
     { "create schema app", null },
     { "set schema app", null },
     { "create table test (a int)", null },
     { "set schema syscat", null },
     { "create table foo1 (a int)", "42X62" },
     { "create view foo1 as select * from app.test", "42X62" },
     { "set schema sysfun", null },
     { "create table foo1 (a int)", "42X62" },
     { "create view foo1 as select * from app.test", "42X62" },
     { "set schema sysproc", null },
     { "create table foo1 (a int)", "42X62" },
     { "create view foo1 as select * from app.test", "42X62" },
     { "set schema sysstat", null },
     { "create table foo1 (a int)", "42X62" },
     { "create view foo1 as select * from app.test", "42X62" },
     { "set schema sysstat", null },
     { "create table foo1 (a int)", "42X62" },
     { "create view foo1 as select * from app.test", "42X62" },
     { "set schema syscs_diag", null },
     { "create table foo1 (a int)", "42X62" },
     { "create view foo1 as select * from app.test", "42X62" },
     { "set schema syscs_util", null },
     { "create table foo1 (a int)", "42X62" },
     { "create view foo1 as select * from app.test", "42X62" },
     { "set schema nullid", null },
     { "create table foo1 (a int)", "42X62" },
     { "create view foo1 as select * from app.test", "42X62" },
     { "set schema sysibm", null },
     { "create table foo1 (a int)", "42X62" },
     { "create view foo1 as select * from app.test", "42X62" },
     { "set schema sqlj", null },
     { "create table foo1 (a int)", "42X62" },
     { "create view foo1 as select * from app.test", "42X62"},
     { "set schema SYSCAT", null },
     { "create table foo1 (a int)", "42X62" },
     { "create view foo1 as select * from app.test", "42X62" },
     { "set schema SYSFUN", null },
     { "create table foo1 (a int)", "42X62" },
     { "create view foo1 as select * from app.test", "42X62" },
     { "set schema SYSPROC", null },
     { "create table foo1 (a int)", "42X62" },
     { "create view foo1 as select * from app.test", "42X62" },
     { "set schema SYSSTAT", null },
     { "create table foo1 (a int)", "42X62" },
     { "create view foo1 as select * from app.test", "42X62" },
     { "set schema SYSSTAT", null },
     { "create table foo1 (a int)", "42X62" },
     { "create view foo1 as select * from app.test", "42X62" },
     { "set schema SYSCS_DIAG", null },
     { "create table foo1 (a int)", "42X62" },
     { "create view foo1 as select * from app.test", "42X62" },
     { "set schema SYSCS_UTIL", null },
     { "create table foo1 (a int)", "42X62" },
     { "create view foo1 as select * from app.test", "42X62" },
     { "set schema NULLID", null },
     { "create table foo1 (a int)", "42X62" },
     { "create view foo1 as select * from app.test", "42X62" },
     { "set schema SYSIBM", null },
     { "create table foo1 (a int)", "42X62" },
     { "create view foo1 as select * from app.test", "42X62" },
     { "set schema SQLJ", null },
     { "create table foo1 (a int)", "42X62" },
     { "create view foo1 as select * from app.test", "42X62" },
     { "set schema app", null },
     { "create table t1 (c1 int)", null },
     { "create trigger sysblah.trig1 after update of c1 on t1 for each row insert into t1 values 1", "42X62" },
     { "create procedure sysblah.dummy() language java external name 'NotReallyThere.NoMethod' parameter style java", "42X62" },
     { "drop table t1", null },
     { "create schema test", null },
     { "create schema test", "X0Y68" },
     { "select schemaname, authorizationid from sys.sysschemas 	where CAST(schemaname AS VARCHAR(128)) = 'TEST'", new String[][] {
		{"TEST","APP"} } },                                                                                                   
     { "set schema test", null },
     { "create table sampletab (c1 int constraint st_c1 check (c1 > 1), c2 char(20))", null },
     { "insert into sampletab values (1,'in schema: TEST')", "23513" },
     { "insert into sampletab values (2,'in schema: TEST')", null },
     { "select schemaname, tablename, descriptor from sys.sysschemas s, sys.sysconglomerates c , sys.systables t where CAST(t.tablename AS VARCHAR(128)) = 'SAMPLETAB' and s.schemaid = c.schemaid and c.tableid = t.tableid", new String[][] {
		{"TEST","SAMPLETAB",null} } },
     { "create index ixsampletab on sampletab(c1)", null },
     { "create index ix2sampletab on test.sampletab(c1)", null },
     { "create view vsampletab as select * from sampletab", null },
     { "create view v2sampletab as select * from test.sampletab", null },
     { "set schema APP", null },
     { "create table sampletab (c1 int constraint st_c1 check(c1 > 1), c2 char(20))", null },
     { "insert into sampletab values (2,'in schema: APP')", null },
     { "select schemaname, tablename, descriptor as descr from sys.sysschemas s, sys.sysconglomerates c , sys.systables t where CAST(t.tablename AS VARCHAR(128)) = 'SAMPLETAB' and s.schemaid = c.schemaid and c.tableid = t.tableid order by schemaname, tablename", new String[][] {
		{"APP","SAMPLETAB",null},{"TEST","SAMPLETAB","LOCALSORTEDMAP (1)"},{"TEST","SAMPLETAB",null} } },   // GEMFIREXD uses sortedmap, not btree indexes
     { "select * from sampletab", new String[][] { {"2","in schema: APP"} } },
     { "select * from test.sampletab", new String[][] { {"2","in schema: TEST"} } },
     { "drop schema test RESTRICT", "X0Y54" },
     { "drop view test.vsampletab", null },
     { "drop view test.v2sampletab", null },
     { "drop index test.ixsampletab", null },
     { "drop index test.ix2sampletab", "42X65" },
     { "drop table sampletab", null },
	{ "drop table test.sampletab", null },
     { "drop schema test RESTRICT", null },
	{ "create schema x", null },
     { "set schema x", null },
     { "create view vx as select * from sys.sysschemas", null },
     { "drop schema x RESTRICT", "X0Y54"},
     { "drop view x.vx", null },
     { "create table x (x int)", null },
     { "drop schema x restrict", "X0Y54" },
     { "drop table x.x", null },
     { "drop schema x cascade", "42X01" },
     { "set schema app", null },
     { "drop schema x restrict", null },
     { "create schema test", null },
     { "set schema test", null },
     { "create table s (i int, s smallint, c char(30), vc char(30))", null },
     { "create table t (i int, s smallint, c char(30), vc char(30))", null },
     { "create table tt (ii int, ss smallint, cc char(30), vcvc char(30))", null },
     { "create table ttt (iii int, sss smallint, ccc char(30), vcvcvc char(30))", null },
     { "insert into s values (null, null, null, null)", null },
     { "insert into s values (0, 0, '0', '0')", null },
     { "insert into s values (1, 1, '1', '1')", null },
     { "insert into t values (null, null, null, null)", null },
     { "insert into t values (0, 0, '0', '0')", null },
     { "insert into t values (1, 1, '1', '1')", null },
     { "insert into t values (1, 1, '1', '1')", null },
     { "insert into tt values (null, null, null, null)", null },
     { "insert into tt values (0, 0, '0', '0')", null },
     { "insert into tt values (1, 1, '1', '1')", null },
     { "insert into tt values (1, 1, '1', '1')", null },
     { "insert into tt values (2, 2, '2', '2')", null },
     { "insert into ttt values (null, null, null, null)", null },
     { "insert into ttt values (11, 11, '11', '11')", null },
     { "insert into ttt values (11, 11, '11', '11')", null },
     { "insert into ttt values (22, 22, '22', '22')", null },
     { "set schema app", null },
     { "insert into test.t values (2, 2, '2', '2')", null },
     { "update test.t set s = 2 where i = 2", null },
     { "update test.t set s = 2 where test.t.i = 2", null },
     { "delete from test.t where i = 1", null },
     { "select * from test.t", new String[][] {
		{null,null,null,null},{"0","0","0","0"},{"2","2","2","2"} } },
     { "insert into test.t values (1, 1, '1', '1')", null },
     { "insert into test.t values (1, 1, '1', '1')", null },
     { "select * from test.t t1", new String[][] {
		{null,null,null,null},{"0","0","0","0"},{"2","2","2","2"},{"1","1","1","1"},{"1","1","1","1"} } },
     { "select * from test.s where exists (select test.s.* from test.t)", new String[][] {
		{null,null,null,null},{"0","0","0","0"},{"1","1","1","1"} } },
     { "DECLARE GLOBAL TEMPORARY TABLE SESSION.ISCT(c21 int) on commit delete rows not logged", null },
     { "select count(*) from SYS.SYSSCHEMAS WHERE CAST(SCHEMANAME AS VARCHAR(128)) = 'SESSION'", new String[][] { {"0"} } },
     { "drop table SESSION.ISCT", null },
     { "create schema SYSDJD", "42939" },
     { "drop schema SYSDJD restrict", "42Y07" }
    };

    Connection conn = TestUtil.getConnection();
    Statement stmt = conn.createStatement();
    // Go through the array, execute each string[0], check sqlstate [1]
    // This will fail on the first one that succeeds where it shouldn't
    // or throws unknown exception
    JDBC.SQLUnitTestHelper(stmt,Script_SchemasUTPartitioning);

  }
}
