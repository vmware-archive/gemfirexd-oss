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

public class LangScripts_NonReservedDUnit extends DistributedSQLTestBase {

	public LangScripts_NonReservedDUnit(String name) {
		super(name);
		// TODO Auto-generated constructor stub
	}
	  // This test is the as-is LangScript conversion, without any partitioning clauses
	  public void testLangScript_NonReservedTestNoPartitioning() throws Exception
	  {
	    // This is a JUnit conversion of the Derby Lang NonReserved.sql script
	    // without any GemFireXD extensions
	    // New nonreserved words can be added to this test
		  
	    // Catch exceptions from illegal syntax
	    // Tests still not fixed marked FIXME
		  
	    // Array of SQL text to execute and sqlstates to expect
	    // The first object is a String, the second is either 
	    // 1) null - this means query returns no rows and throws no exceptions
	    // 2) a string - this means query returns no rows and throws expected SQLSTATE
	    // 3) a String array - this means query returns rows which must match (unordered) given resultset
	    //       - for an empty result set, an uninitialized size [0][0] array is used
	    Object[][] Script_NonReservedUT = {
		// This tests that SQL92 formally reserved words are now unreserved
		// INTERVAL
		{ "create table interval(interval int)", null },
		{ "create index interval on interval(interval)", null },
		{ "drop table interval", null },
		// MODULE
		{ "create table module(module int)", null },
		{ "create index module on module(module)", null },
		{ "drop table module", null },
		// NAMES
		{ "create table names(names int)", null },
		{ "create index names on names(names)", null },
		{ "drop table names", null },
		// PRECISION
		{ "create table precision(precision int)", null },
		{ "create index precision on precision(precision)", null },
		{ "drop table precision", null },
		// POSITION
		{ "create table position(position int)", null },
		{ "create index position on position(position)", null },
		{ "drop table position", null },
		// SECTION
		{ "create table section(section int)", null },
		{ "create index section on section(section)", null },
		{ "drop table section", null },
		// VALUE
		{ "create table value(value int)", null },
		{ "create index value on value(value)", null },
		{ "drop table value", null },
		// DATE
		{ "create table date (date date)", null },
		{ "insert into date(date) values (date('2001-01-01'))", null },
		{ "select date from date", new String[][] { {"2001-01-01"} } },
		{ "select date date from date", new String[][] { {"2001-01-01"} } },
		{ "select date as date from date", new String[][] { {"2001-01-01"} } },
		{ "select date.date as date from date date", new String[][] { {"2001-01-01"} } },
		{ "select date.date as date from date as date", new String[][] { {"2001-01-01"} } },
		{ "delete from date where date = date('2001-01-01')", null },
		{ "create index date on date(date)", null },
		{ "drop table date", null },
		// TIME
		{ "create table time (time time)", null },
		{ "insert into time(time) values (time('01:01:01'))", null },
		{ "select time from time", new String[][] { {"01:01:01"} } },
		{ "select time time from time", new String[][] { {"01:01:01"} } },
		{ "select time as time from time", new String[][] { {"01:01:01"} } },
		{ "select time.time as time from time time", new String[][] { {"01:01:01"} } },
		{ "select time.time as time from time as time", new String[][] { {"01:01:01"} } },
		{ "delete from time where time = time('01:01:01')", null },
		{ "create index time on time(time)", null },
		{ "drop table time", null },
		// DOMAIN
		{ "create table DOMAIN (domain int)", null },
		{ "insert into domain values (1)", null },
		{ "select domain from domain where domain > 0", new String[][] { {"1"} } },
		{ "select domain from domain domain where domain > 0", new String[][] { {"1"} } },
		{ "select domain.domain from domain domain where domain.domain > 0", new String[][] { {"1"} } },
		{ "create index domain on domain(domain)", null },
		{ "drop table DOMAIN", null },
		// CATALOG
		{ "create table CATALOG (catalog int)", null },
		{ "insert into catalog values (1)", null },
		{ "select catalog from catalog where catalog > 0", new String[][] { {"1"} } },
		{ "select catalog from catalog catalog where catalog > 0", new String[][] { {"1"} } },
		{ "create index catalog on catalog(catalog)", null },
		{ "drop table CATALOG", null },
		// ACTION
		{ "create table ACTION (action int)", null },
		{ "insert into action values (1)", null },
		{ "select action from action where action > 0", new String[][] { {"1"} } },
		{ "select action from action action where action > 0", new String[][] { {"1"} } },
		{ "create index action on action(action)", null },
		{ "drop table ACTION", null },
		// DAY
		{ "create table DAY (day int)", null },
		{ "insert into day values (1)", null },
		{ "select day from day where day > 0", new String[][] { {"1"} } },
		{ "select day from day day where day > 0", new String[][] { {"1"} } },
		{ "create index day on day(day)", null },
		{ "drop table DAY", null },
		// MONTH
		{ "create table MONTH (month int)", null },
		{ "insert into month values (1)", null },
		{ "select month from month where month > 0", new String[][] { {"1"} } },
		{ "select month from month month where month > 0", new String[][] { {"1"} } },
		{ "select month.month from month month where month.month > 0", new String[][] { {"1"} } },
		{ "create index month on month(month)", null },
		{ "drop table MONTH", null },
		// USAGE
		{ "create table USAGE (usage int)", null },
		{ "insert into usage values (1)", null },
		{ "select usage from usage where usage > 0", new String[][] { {"1"} } },
		{ "select usage from usage usage where usage > 0", new String[][] { {"1"} } },
		{ "select usage.usage from usage usage where usage.usage > 0", new String[][] { {"1"} } },
		{ "create index usage on usage(usage)", null },
		{ "drop table USAGE", null },
		// LANGUAGE
		{ "create table LANGUAGE (language int)", null },
		{ "insert into language values (1)", null },
		{ "select language from language where language > 0", new String[][] { {"1"} } },
		{ "select language from language language where language > 0", new String[][] { {"1"} } },
		{ "select language.language from language language where language.language > 0", new String[][] { {"1"} } },
		{ "create index language on language(language)", null },
		{ "drop table LANGUAGE", null },
		// LOCKS
		{ "create table LOCKS (c11 int)", null },
		{ "drop table LOCKS", null },
		{ "create table t1 (LOCKS int)", null },
		{ "drop table t1", null },
		{ "create table LOCKS (locks int)", null },
		{ "insert into locks values (1)", null },
		{ "select locks from locks where locks > 0", new String[][] { {"1"} } },
		{ "select locks from locks locks where locks > 0", new String[][] { {"1"} } },
		{ "select locks.locks from locks locks where locks.locks > 0", new String[][] { {"1"} } },
		{ "create index locks on locks(locks)", null },
		{ "drop table LOCKS", null },
		// COUNT
		{ "create table count(i int)", null },
		{ "drop table count", null },
		{ "create table t1 (count int)", null },
		{ "drop table t1", null },
		{ "create table count(count int)", null },
		{ "insert into count values (1)", null },
		{ "select * from count", new String[][] { {"1"} } },
		{ "select count from count", new String[][] { {"1"} } },
		{ "select count from count where count=1", new String[][] { {"1"} } },
		{ "select count.count from count", new String[][] { {"1"} } },
		{ "create index count on count(count)", null },
		{ "drop table count", null },
		{ "create table t1(i int)", null },
		{ "insert into t1 values -1,2,-3,4,-5,6,-7,8,-9,0", null },
		{ "create function count(i int) returns int no sql external name 'java.lang.Math.abs' language java parameter style java", null },
		{ "select count(*) from t1", new String[][] { {"10"} } },
		{ "select count(i) from t1", new String[][] { {"10"} } },
		{ "select * from t1 where count(i)=i", "42903" },
		{ "drop table t1", null },
		// SECURITY
		{ "create table SECURITY (security int)", null },
		{ "insert into security values (1)", null },
		{ "select security from security where security > 0", new String[][] { {"1"} } },
		{ "select security from security security where security > 0", new String[][] { {"1"} } },
		{ "select security.security from security where security.security > 0", new String[][] { {"1"} } },
		{ "create index security on security(security)", null },
		{ "drop table SECURITY", null }
	   };

	    // Start 1 client and 3 servers, use default partitioning
	    startVMs(1, 3);


	    Connection conn = TestUtil.getConnection();
	    Statement stmt = conn.createStatement();
	    // Go through the array, execute each string[0], check sqlstate [1]
	    // This will fail on the first one that succeeds where it shouldn't
	    // or throws unknown exception
	    JDBC.SQLUnitTestHelper(stmt,Script_NonReservedUT);
	  }

}
