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

public class LangScripts_OuterJoinDUnit extends DistributedSQLTestBase {

	public LangScripts_OuterJoinDUnit(String name) {
		super(name);
		// TODO Auto-generated constructor stub
	}

	  public void testLangScript_OuterJoinTest() throws Exception
	  {
	    // This is a JUnit conversion of the Derby Lang OuterJoin.sql script
	    // without any GemFireXD extensions
		  
	    // Catch exceptions from illegal syntax
	    // Tests still not fixed marked FIXME
		  
	    // Array of SQL text to execute and sqlstates to expect
	    // The first object is a String, the second is either 
	    // 1) null - this means query returns no rows and throws no exceptions
	    // 2) a string - this means query returns no rows and throws expected SQLSTATE
	    // 3) a String array - this means query returns rows which must match (unordered) given resultset
	    //       - for an empty result set, an uninitialized size [0][0] array is used
	    Object[][] Script_OuterJoinUT = {
		// test outer joins (NO NATURAL JOIN)
	    // FIXME
	    // Try with all tables as REPLICATEd, too many join scenarios will throw colocation exceptions
		{ "create table t1(c1 int) REPLICATE", null },
		{ "create table t2(c1 int) REPLICATE", null },
		{ "create table t3(c1 int) REPLICATE", null },
		{ "create table tt1(c1 int, c2 int, c3 int) REPLICATE", null },
		{ "create table tt2(c1 int, c2 int, c3 int) REPLICATE", null },
		{ "create table tt3(c1 int, c2 int, c3 int) REPLICATE", null },
		{ "create table empty_table(c1 int)", null },
		{ "create table insert_test(c1 int, c2 int, c3 int)", null },
		{ "create table oj(oj int)", null },
		{ "insert into t1 values 1, 2, 2, 3, 4", null },
		{ "insert into t2 values 1, 3, 3, 5, 6", null },
		{ "insert into t3 values 2, 3, 5, 5, 7", null },
		{ "insert into tt1 select c1, c1, c1 from t1", null },
		{ "insert into tt2 select c1, c1, c1 from t2", null },
		{ "insert into tt3 select c1, c1, c1 from t3", null },
		{ "insert into oj(oj) values (1)", null },
		{ "select * from t1 outer join t2", "42X01" },
		{ "select * from t1 left outer join t2", "42X01" },
		{ "select * from t1 right outer join t2", "42X01" },
		{ "select t1.c1 from t1 left outer join t2 on t1.c1 = t2.c1", new String[][] { {"1"},{"2"},{"2"},{"3"},{"3"},{"4"} } },
		{ "select t2.c1 from t1 right outer join t2 on t1.c1 = t2.c1", new String[][] { {"1"},{"3"},{"3"},{"5"},{"6"} } },
		{ "select a.x from t1 a (x) left outer join t2 b (x) on a.x = b.x", new String[][] { {"1"},{"2"},{"2"},{"3"},{"3"},{"4"} } },
		{ "select b.* from (values 9) a left outer join t2 b on 1=1", new String[][] { {"1"},{"3"},{"3"},{"5"},{"6"} } },
		{ "select b.* from (values 9) a left outer join t2 b on 1=0", new String[][] { {null} } },
		{ "select b.* from (values 9) a right outer join t2 b on 1=0", new String[][] { {"1"},{"3"},{"3"},{"5"},{"6"} } },
		// FIXME
		// These throws NPE (bug 45941)
		//{ "select a.* from (values 9) a right outer join t2 b on 1=1", new String[][] { {"9"},{"9"},{"9"},{"9"},{"9"} } },
		//{ "select a.* from (values 9) a right outer join t2 b on 1=0", new String[][] { {null},{null},{null},{null},{null} } },
		{ "select a.* from ((values ('a', 'b')) a inner join (values ('c', 'd')) b on 1=1) left outer join (values ('e', 'f')) c on 1=1", new String[][] { {"a","b"} } },
		{ "select b.* from ((values ('a', 'b')) a inner join (values ('c', 'd')) b on 1=1) left outer join (values ('e', 'f')) c on 1=1", new String[][] { {"c","d"} } },
		{ "select c.* from ((values ('a', 'b')) a inner join (values ('c', 'd')) b on 1=1) left outer join (values ('e', 'f')) c on 1=1", new String[][] { {"e","f"} } },
		{ "select * from t1 left outer join {oj t2 left outer join t3 on t2.c1=t3.c1} on t1.c1=t3.c1", new String[][] {
			{"1",null,null},{"2",null,null},{"2",null,null},{"3","3","3"},{"3","3","3"},{"4",null,null} } },
		{ "select t1.c1 from t1 left outer join empty_table et on t1.c1 = et.c1", new String[][] { {"1"},{"2"},{"2"},{"3"},{"4"} } },
		{ "select t1.c1 from t1 right outer join empty_table et on t1.c1 = et.c1", new String[0][0] },
		{ "select t1.c1 from empty_table et right outer join t1 on et.c1 = t1.c1", new String[][] { {"1"},{"2"},{"2"},{"3"},{"4"} } },
		// this query may make no sense at all, but it's just trying to show that parser works with tableexpression oj
		{ "select * from t1, {oj t2 join t3 on t2.c1=t3.c1}", new String[][] {
			{"1","3","3"},{"1","3","3"},{"1","5","5"},{"1","5","5"},
			{"2","3","3"},{"2","3","3"},{"2","5","5"},{"2","5","5"},
			{"2","3","3"},{"2","3","3"},{"2","5","5"},{"2","5","5"},
			{"3","3","3"},{"3","3","3"},{"3","5","5"},{"3","5","5"},
			{"4","3","3"},{"4","3","3"},{"4","5","5"},{"4","5","5"} } },
		{ "select * from t1 left outer join t2 on t1.c1 = t2.c1 where t1.c1 = 1", new String[][] { {"1","1"} } },
		{ "select * from {oj t1 left outer join t2 on t1.c1 = t2.c1} where t1.c1 = 1", new String[][] { {"1","1"} } },
		{ "select * from t1 right outer join t2 on t1.c1 = 1 where t2.c1 = t1.c1", new String[][] { {"1","1"} } },
		{ "select * from {oj t1 right outer join t2 on t1.c1 = 1} where t2.c1 = t1.c1", new String[][] { {"1","1"} } },
		{ "select * from t1 a left outer join t2 b on a.c1 = b.c1 and a.c1 = (select c1 from t1 where a.c1 = t1.c1 and a.c1 = 1)", "42972"},
		{ " select * from {oj t1 a left outer join t2 b on a.c1 = b.c1 and a.c1 = (select c1 from t1 where a.c1 = t1.c1 and a.c1 = 1)}", "42972" },
		{ "select * from t1 a left outer join t2 b on a.c1 = b.c1 and a.c1 = (select c1 from t1 where a.c1 = t1.c1 and a.c1 <> 2)", "42972" },
		{ "select * from {oj t1 a left outer join t2 b on a.c1 = b.c1 and a.c1 = (select c1 from t1 where a.c1 = t1.c1 and a.c1 <> 2)}", "42972" },
		{ "select * from t1 a right outer join t2 b on a.c1 = b.c1 and a.c1 in (select c1 from t1 where a.c1 = t1.c1)", "42972" },
		{ "select * from t1 a where exists (select * from t1 left outer join t2 on t1.c1 = t2.c1)", new String[][] {
			{"1"},{"2"},{"2"},{"3"},{"4"} } },
		{ "select * from t1 a where exists (select * from {oj t1 left outer join t2 on t1.c1 = t2.c1})", new String[][] {
			{"1"},{"2"},{"2"},{"3"},{"4"} } },
		{ "select * from t1 a where exists (select * from t1 left outer join t2 on 1=0)", new String[][] {
			{"1"},{"2"},{"2"},{"3"},{"4"} } },
		{ "select * from t1 left outer join t2 on t1.c1 = t2.c1 left outer join t3 on t1.c1 = t3.c1", new String[][] {
			{"1","1",null},{"2",null,"2"},{"2",null,"2"},{"3","3","3"},{"3","3","3"},{"4",null,null} } },
		{ "select * from {oj t1 left outer join t2 on t1.c1 = t2.c1 left outer join t3 on t1.c1 =t3.c1}", new String[][] {
			{"1","1",null},{"2",null,"2"},{"2",null,"2"},{"3","3","3"},{"3","3","3"},{"4",null,null} } },
		{ "select * from t1 left outer join t2 on t1.c1 = t2.c1 left outer join t3 on t2.c1 = t3.c1", new String[][] {
			{"1","1",null},{"2",null,null},{"2",null,null},{"3","3","3"},{"3","3","3"},{"4",null,null} } },
		{ "select * from t3 right outer join t2 on t3.c1 = t2.c1 right outer join t1 on t1.c1 = t2.c1", new String[][] {
			{null,"1","1"},{null,null,"2"},{null,null,"2"},{"3","3","3"},{"3","3","3"},{null,null,"4"} } },
		{ "select * from (t1 left outer join t2 on t1.c1 = t2.c1) left outer join t3 on t1.c1 = t3.c1", new String[][] {
			{"1","1",null},{"2",null,"2"},{"2",null,"2"},{"3","3","3"},{"3","3","3"},{"4",null,null} } },
		{ "select * from t1 left outer join (t2 left outer join t3 on t2.c1 = t3.c1) on t1.c1 = t2.c1", new String[][] {
			{"1","1",null},{"2",null,null},{"2",null,null},{"3","3","3"},{"3","3","3"},{"4",null,null} } },
		{ "select * from t1 a right outer join t2 b on a.c1 = b.c1 left outer join t3 c on a.c1 = b.c1 and b.c1 = c.c1", new String[][] {
			{"1","1",null},{"3","3","3"},{"3","3","3"},{null,"5",null},{null,"6",null} } },
		{ "select * from (t1 a right outer join t2 b on a.c1 = b.c1) left outer join t3 c on a.c1 = b.c1 and b.c1 = c.c1", new String[][] {
			{"1","1",null},{"3","3","3"},{"3","3","3"},{null,"5",null},{null,"6",null} } },
		{ "select * from t1 a left outer join t2 b on a.c1 = b.c1 right outer join t3 c on c.c1 = a.c1 where a.c1 is not null", new String[][] {
			{"2",null,"2"},{"2",null,"2"},{"3","3","3"},{"3","3","3"} } },
		{ "select * from (t1 a left outer join t2 b on a.c1 = b.c1) right outer join t3 c on c.c1 = a.c1 where a.c1 is not null", new String[][] {
			{"2",null,"2"},{"2",null,"2"},{"3","3","3"},{"3","3","3"} } },
		{ "select * from t1 a left outer join (t2 b right outer join t3 c on c.c1 = b.c1) on a.c1 = c.c1 where c.c1=b.c1", new String[][] {
			{"3","3","3"},{"3","3","3"} } },
		{ "insert into insert_test select * from t1 a left outer join t2 b on a.c1 = b.c1 left outer join t3 c on a.c1 <> c.c1", null },
		{ "select count(*) from insert_test", new String[][] { {"26"} } },
		{ "update insert_test set c1 = (select 9 from t1 a left outer join t1 b on a.c1 = b.c1 where a.c1 = 1) where c1 = 1", null },
		{ "select * from insert_test", new String[][] {
			{"9","1","2"},{"9","1","3"},{"9","1","5"},{"9","1","5"},{"9","1","7"},
			{"2",null,"3"},{"2",null,"5"},{"2",null,"5"},{"2",null,"7"},{"2",null,"3"},
			{"2",null,"5"},{"2",null,"5"},{"2",null,"7"},{"3","3","2"}, {"3","3","5"},{"3","3","5"},
			{"3","3","7"},{"3","3","2"},{"3","3","5"},{"3","3","5"},{"3","3","7"},{"4",null,"2"},
			{"4",null,"3"},{"4",null,"5"},{"4",null,"5"},{"4",null,"7"} } },
		{ "delete from insert_test where c1 = (select 9 from t1 a left outer join t1 b on a.c1 = b.c1 where a.c1 = 1)", null },
		{ "select count(*) from insert_test", new String[][] { {"21"} } },
		{ "delete from insert_test", null },
		{ "insert into insert_test select * from (select * from t1 a left outer join t2 b on a.c1 = b.c1 left outer join t3 c on a.c1 <> c.c1) d (c1, c2, c3)", null },
		{ "select count(*) from insert_test", new String[][] { {"26"} } },
		{ "delete from insert_test", null },
		// FIXME
		// All these must be REPLICATEd because joins on boolean true or false is not allowed on partitioned tables
		{ "create table a (c1 int) REPLICATE", null },
		{ "create table b (c2 float) REPLICATE", null },
		{ "create table c (c3 char(30)) REPLICATE", null },
		{ "insert into a values 1", null },
		{ "insert into b values 3.3", null },
		{ "insert into c values 'asdf'", null },
		{ "select * from a left outer join b on 1=1 left outer join c on 1=1", new String[][] { {"1","3.3","asdf"} } },
		{ "select * from a left outer join b on 1=1 left outer join c on 1=0", new String[][] { {"1","3.3",null} } },
		{ "select * from a left outer join b on 1=0 left outer join c on 1=1", new String[][] { {"1",null,"asdf"} } },
		{ "select * from a left outer join b on 1=0 left outer join c on 1=0", new String[][] { {"1",null,null} } },
		{ "select * from c right outer join b on 1=1 right outer join a on 1=1", new String[][] { {"asdf","3.3","1"} } },
		{ "select * from c right outer join b on 1=1 right outer join a on 1=0", new String[][] { {null,null,"1"} } },
		{ "select * from c right outer join b on 1=0 right outer join a on 1=1", new String[][] { {null,"3.3","1"} } },
		{ "select * from c right outer join b on 1=0 right outer join a on 1=0", new String[][] { {null,null,"1"} } },
		{ "select tt1.c1, tt1.c2, tt1.c3, tt2.c2, tt2.c3 from tt1 left outer join tt2 on tt1.c1 = tt2.c1", new String[][] {
			{"1","1","1","1","1"},{"2","2","2",null,null},{"2","2","2",null,null},
			{"3","3","3","3","3"},{"3","3","3","3","3"},{"4","4","4",null,null} } },
		{ "select tt1.c1, tt1.c2, tt1.c3, tt2.c3 from tt1 left outer join tt2 on tt1.c1 = tt2.c1", new String[][] {
			{"1","1","1","1"},{"2","2","2",null},{"2","2","2",null},
			{"3","3","3","3"},{"3","3","3","3"},{"4","4","4",null} } },
		{ "select tt1.c1, tt1.c2, tt1.c3 from tt1 left outer join tt2 on tt1.c1 = tt2.c1", new String[][] {
			{"1","1","1"},{"2","2","2"},{"2","2","2"},{"3","3","3"},{"3","3","3"},{"4","4","4"} } },
		{ "select tt1.c2, tt1.c1, tt1.c3, tt2.c1, tt2.c3 from t1 left outer join tt1 on t1.c1 = tt1.c1 left outer join tt2 on tt1.c2 = tt2.c2", new String[][] {
			{"1","1","1","1","1"},{"2","2","2",null,null},{"2","2","2",null,null},{"2","2","2",null,null},{"2","2","2",null,null},
			{"3","3","3","3","3"},{"3","3","3","3","3"},{"4","4","4",null,null} } },
		// Partition and colocate on joined column C3
		{ "create table x (c1 int, c2 int, c3 int) partition by column(c3)", null },
		{ "create table y (c3 int, c4 int, c5 int) partition by column(c3) colocate with (x)", null },
		{ "insert into x values (1, 2, 3), (4, 5, 6)", null },
		{ "insert into y values (3, 4, 5), (666, 7, 8)", null },
		{ "select x.* from x join y on x.c3 = y.c3", new String[][] { {"1","2","3"} } },
		{ "select x.* from x left outer join y on x.c3 = y.c3", new String[][] { {"1","2","3"},{"4","5","6"} } },
		{ "select x.* from x right outer join y on x.c3 = y.c3", new String[][] { {"1","2","3"},{null,null,null} } },
		{ "select y.* from x join y on x.c3 = y.c3", new String[][] { {"3","4","5"} } },
		{ "select y.* from x left outer join y on x.c3 = y.c3", new String[][] { {"3","4","5"},{null,null,null} } },
		{ "select y.* from x right outer join y on x.c3 = y.c3", new String[][] { {"3","4","5"},{"666","7","8"} } },
		{ "select * from x join y on x.c3 = y.c3", new String[][] { {"1","2","3","3","4","5"} } },
		{ "select * from x left outer join y on x.c3 = y.c3", new String[][] {
			{"1","2","3","3","4","5"},{"4","5","6",null,null,null} } },
		{ "select * from x right outer join y on x.c3 = y.c3", new String[][] {
			{"1","2","3","3","4","5"},{null,null,null,"666","7","8"} } },
		{ "delete from tt1", null },
		{ "delete from tt2", null },
		{ "delete from tt3", null },
		{ "insert into tt1 values (1, 2, 3), (2, 3, 4), (3, 4, 5)", null },
		{ "insert into tt2 values (1, 2, 3), (2, 3, 4), (3, 4, 5)", null },
		{ "insert into tt3 values (1, 2, 3), (2, 3, 4), (3, 4, 5)", null },
		{ "select * from tt1 left outer join tt2 on tt1.c1 = tt2.c2 where tt1.c1 = 3", new String[][] {
			{"3","4","5","2","3","4"} } },
		{ "select * from tt1 left outer join tt2 on tt1.c1 = tt2.c2 where tt2.c2 = 3", new String[][] {
			{"3","4","5","2","3","4"} } },
		{ "select * from tt1 left outer join tt2 on tt1.c1 = tt2.c2 where tt2.c1 + 1= tt2.c2", new String[][] {
			{"2","3","4","1","2","3"},{"3","4","5","2","3","4"} } },
		{ "select * from tt1 left outer join tt2 on tt1.c1 = tt2.c2 where tt2.c1 + 1= 3", new String[][] {
			{"3","4","5","2","3","4"} } },
		{ "select * from tt2 right outer join tt1 on tt1.c1 = tt2.c2 where tt2.c1 + 1= 3", new String[][] {
			{"2","3","4","3","4","5"} } },
		{ "select * from tt1 left outer join tt2 on tt1.c1 = tt2.c2 left outer join tt3 on tt2.c2 = tt3.c3 where tt3.c3 = 3", new String[][] {
			{"3","4","5","2","3","4","1","2","3"} } },
		{ "select * from tt1 left outer join tt2 on tt1.c1 = tt2.c2 left outer join tt3 on tt2.c2 = tt3.c3 where tt2.c2 = 3", new String[][] {
			{"3","4","5","2","3","4","1","2","3"} } },
		{ "select * from tt1 left outer join tt2 on tt1.c1 = tt2.c2 where char(tt2.c2) is null", new String[][] {
			{"1","2","3",null,null,null} } },
		// FIXME
		// These tables cannot be colocated as INVENTORY is joined by its primary key and its other column, therefore no colocation column can be specified
		// Use replicated tables instead
		{ "CREATE TABLE inventory(itemno INT NOT NULL PRIMARY KEY, capacity INT) replicate", null },
		{ "INSERT INTO inventory VALUES (1, 4)", null },
		{ "INSERT INTO inventory VALUES (2, 2)", null },
		{ "INSERT INTO inventory VALUES (3, 2)", null },
		{ "CREATE TABLE timeslots (slotno INT NOT NULL PRIMARY KEY) partition by primary key", null },
		{ "INSERT INTO timeslots VALUES(1)", null },
		{ "INSERT INTO timeslots VALUES(2)", null },
		{ "create table reservations(slotno INT CONSTRAINT timeslots_fk REFERENCES timeslots, itemno INT CONSTRAINT inventory_fk REFERENCES inventory, name VARCHAR(100), resdate DATE) REPLICATE",null },
		{ "INSERT INTO reservations VALUES(1, 1, 'Joe', '2000-04-14')", null },
		{ "INSERT INTO reservations VALUES(1, 1, 'Fred', '2000-04-13')", null },
		{ "select name, resdate from reservations left outer join (inventory join timeslots on inventory.itemno = timeslots.slotno) on inventory.itemno = reservations.itemno and timeslots.slotno = reservations.slotno where resdate = '2000-04-14'", new String[][] { {"Joe","2000-04-14"} } },
		{ "drop table reservations", null },
		{ "create table reservations(slotno INT CONSTRAINT timeslots_fk REFERENCES timeslots, itemno INT CONSTRAINT inventory_fk REFERENCES inventory,name VARCHAR(100)) replicate", null },
		{ "INSERT INTO reservations VALUES(1, 1, 'Joe')", null },
		{ "INSERT INTO reservations VALUES(2, 2, 'Fred')", null },
//		{ "select timeslots.slotno, inventory.itemno, capacity, name from inventory left outer join timeslots on inventory.capacity = timeslots.slotno left outer join reservations on timeslots.slotno = reservations.slotno where capacity > 3 and name is null", new String[][] { {null,"1","4",null} } },
//		{ "select timeslots.slotno, inventory.itemno, capacity, name from inventory left outer join timeslots on inventory.capacity = timeslots.slotno left outer join reservations on timeslots.slotno = reservations.slotno where name is null and capacity > 3", new String[][] { {null,"1","4",null} } },
		{ "CREATE TABLE properties (name VARCHAR(50),value VARCHAR(200))", null },
		{ "INSERT INTO properties VALUES ('businessName', 'Cloud 9 Cafe')", null },
		{ "INSERT INTO properties VALUES ('lastReservationDate', '2001-12-31')", null },
		{ "DROP TABLE reservations", null },
		{ "DROP TABLE timeslots", null },
		{ "DROP TABLE inventory", null },
		{ "CREATE TABLE inventory (itemno INT NOT NULL PRIMARY KEY,capacity INT) replicate", null },
		{ "INSERT INTO inventory VALUES (1, 2)", null },
		{ "INSERT INTO inventory VALUES (2, 2)", null },
		{ "INSERT INTO inventory VALUES (3, 2)", null },
		{ "INSERT INTO inventory VALUES (4, 2)", null },
		{ "INSERT INTO inventory VALUES (5, 2)", null },
		{ "INSERT INTO inventory VALUES (6, 4)", null },
		{ "INSERT INTO inventory VALUES (7, 4)", null },
		{ "INSERT INTO inventory VALUES (8, 4)", null },
		{ "INSERT INTO inventory VALUES (9, 4)", null },
		{ "INSERT INTO inventory VALUES (10, 4)", null },
		{ "CREATE TABLE timeslots (slot TIME NOT NULL PRIMARY KEY) partition by primary key", null },
		{ "INSERT INTO timeslots VALUES('17:00:00')", null },
		{ "INSERT INTO timeslots VALUES('17:30:00')", null },
		{ "INSERT INTO timeslots VALUES('18:00:00')", null },
		{ "INSERT INTO timeslots VALUES('18:30:00')", null },
		{ "INSERT INTO timeslots VALUES('19:00:00')", null },
		{ "INSERT INTO timeslots VALUES('19:30:00')", null },
		{ "INSERT INTO timeslots VALUES('20:00:00')", null },
		{ "INSERT INTO timeslots VALUES('20:30:00')", null },
		{ "INSERT INTO timeslots VALUES('21:00:00')", null },
		{ "INSERT INTO timeslots VALUES('21:30:00')", null },
		{ "INSERT INTO timeslots VALUES('22:00:00')", null },
		{ "CREATE TABLE reservations (itemno INT CONSTRAINT inventory_fk REFERENCES inventory,slot TIME CONSTRAINT timeslots_fk REFERENCES timeslots,resdate DATE NOT NULL,name VARCHAR(100) NOT NULL,quantity INT,CONSTRAINT reservations_u UNIQUE(name, resdate)) replicate", null },
		{ "INSERT INTO reservations VALUES(6, '17:00:00', '2000-07-13', 'Williams', 4)", null },
		{ "INSERT INTO reservations VALUES(7, '17:00:00', '2000-07-13', 'Johnson',  4)", null },
		{ "INSERT INTO reservations VALUES(8, '17:00:00', '2000-07-13', 'Allen',    3)", null },
		{ "INSERT INTO reservations VALUES(9, '17:00:00', '2000-07-13', 'Dexmier',  4)", null },
		{ "INSERT INTO reservations VALUES(1, '17:30:00', '2000-07-13', 'Gates', 	 2)", null },
		{ "INSERT INTO reservations VALUES(2, '17:30:00', '2000-07-13', 'McNealy',  2)", null },
		{ "INSERT INTO reservations VALUES(3, '17:30:00', '2000-07-13', 'Hoffman',  1)", null },
		{ "INSERT INTO reservations VALUES(4, '17:30:00', '2000-07-13', 'Sippl',    2)", null },
		{ "INSERT INTO reservations VALUES(6, '17:30:00', '2000-07-13', 'Yang',     4)", null },
		{ "INSERT INTO reservations VALUES(7, '17:30:00', '2000-07-13', 'Meyers',   4)", null },
		{ "select max(name), max(resdate) from inventory join timeslots on inventory.capacity is not null left outer join reservations on inventory.itemno = reservations.itemno and reservations.slot = timeslots.slot", new String[][] { {"Yang","2000-07-13"} } },
		{ "DROP TABLE reservations", null },
		{ "DROP TABLE timeslots", null },
		{ "DROP TABLE inventory", null },
		{ "DROP TABLE properties", null },
		{ "CREATE TABLE properties (name VARCHAR(50),value VARCHAR(200))", null },
		{ "INSERT INTO properties VALUES ('businessName', 'Cloud 9 Cafe')", null },
		{ "INSERT INTO properties VALUES ('lastReservationDate', '2001-12-31')", null },
		{ "CREATE TABLE inventory (itemno INT NOT NULL PRIMARY KEY,capacity INT) replicate", null },
		{ "INSERT INTO inventory VALUES (1, 2)", null },
		{ "INSERT INTO inventory VALUES (2, 2)", null },
		{ "INSERT INTO inventory VALUES (3, 2)", null },
		{ "INSERT INTO inventory VALUES (4, 2)", null },
		{ "INSERT INTO inventory VALUES (5, 2)", null },
		{ "INSERT INTO inventory VALUES (6, 4)", null },
		{ "INSERT INTO inventory VALUES (7, 4)", null },
		{ "INSERT INTO inventory VALUES (8, 4)", null },
		{ "INSERT INTO inventory VALUES (9, 4)", null },
		{ "INSERT INTO inventory VALUES (10, 4)", null },
		{ "CREATE TABLE timeslots (slot TIME NOT NULL PRIMARY KEY) partition by primary key", null },
		{ "INSERT INTO timeslots VALUES('17:00:00')", null },
		{ "INSERT INTO timeslots VALUES('17:30:00')", null },
		{ "INSERT INTO timeslots VALUES('18:00:00')", null },
		{ "INSERT INTO timeslots VALUES('18:30:00')", null },
		{ "INSERT INTO timeslots VALUES('19:00:00')", null },
		{ "INSERT INTO timeslots VALUES('19:30:00')", null },
		{ "INSERT INTO timeslots VALUES('20:00:00')", null },
		{ "INSERT INTO timeslots VALUES('20:30:00')", null },
		{ "INSERT INTO timeslots VALUES('21:00:00')", null },
		{ "INSERT INTO timeslots VALUES('21:30:00')", null },
		{ "INSERT INTO timeslots VALUES('22:00:00')", null },
		{ "CREATE TABLE reservations (itemno INT CONSTRAINT inventory_fk REFERENCES inventory,slot TIME CONSTRAINT timeslots_fk REFERENCES timeslots,resdate DATE NOT NULL,name VARCHAR(100) NOT NULL,quantity INT,CONSTRAINT reservations_u UNIQUE(name, resdate)) replicate", null },
		{ "INSERT INTO reservations VALUES(6, '17:00:00', '2000-07-13', 'Williams', 4)", null },
		{ "INSERT INTO reservations VALUES(7, '17:00:00', '2000-07-13', 'Johnson',  4)", null },
		{ "INSERT INTO reservations VALUES(8, '17:00:00', '2000-07-13', 'Allen',    3)", null },
		{ "INSERT INTO reservations VALUES(9, '17:00:00', '2000-07-13', 'Dexmier',  4)", null },
		{ "INSERT INTO reservations VALUES(1, '17:30:00', '2000-07-13', 'Gates', 	 2)", null },
		{ "INSERT INTO reservations VALUES(2, '17:30:00', '2000-07-13', 'McNealy',  2)", null },
		{ "INSERT INTO reservations VALUES(3, '17:30:00', '2000-07-13', 'Hoffman',  1)", null },
		{ "INSERT INTO reservations VALUES(4, '17:30:00', '2000-07-13', 'Sippl',    2)", null },
		{ "INSERT INTO reservations VALUES(6, '17:30:00', '2000-07-13', 'Yang',     4)", null },
		{ "INSERT INTO reservations VALUES(7, '17:30:00', '2000-07-13', 'Meyers',   4)", null },
		{ "select max(timeslots.slot) from inventory inner join timeslots on inventory.capacity is not null left outer join reservations on inventory.capacity = reservations.itemno and reservations.slot = timeslots.slot", new String[][] { {"22:00:00"} } },
		{ "select * from t1 inner join t2 on 1=1 left outer join t3 on t1.c1 = t3.c1 where t1.c1 = t2.c1", new String[][] {
			{"1","1",null},{"3","3","3"},{"3","3","3"} } },
		{ "create table xxx (a int not null) partition by column(a)", null },
		{ "create table yyy (a int not null) partition by column(a) colocate with (xxx)", null },
		{ "insert into xxx values (1)", null },
		{ "select * from xxx left join yyy on (xxx.a=yyy.a)", new String[][] { {"1",null} } },
		{ "insert into xxx values (null)", "23502"},
		{ "select * from xxx", new String[][] { {"1"} } },
		{ "drop table yyy", null},
		{ "drop table xxx", null },
		// FIXME
		// These tables are joined on both columns, cannot colocate
		{ "create table ttab1 (a int, b int) replicate", null },
		{ "insert into ttab1 values (1,1),(2,2)", null },
		{ "create table ttab2 (c int, d int) replicate", null },
		{ "insert into ttab2 values (1,1),(2,2)", null },
		{ "select cor1.*, cor2.* from ttab1 cor1 left outer join ttab2 on (b = d),	ttab1 left outer join ttab2 cor2 on (b = d)", "42X03" },
		{ "select cor1.*, cor2.* from ttab1 cor1 left outer join ttab2 on (b = d),	ttab1 left outer join ttab2 cor2 on (b = cor2.d)", "42X03" },
		{ "select cor1.*, cor2.* from ttab1 left outer join ttab2 on (b = d), ttab1 cor1 left outer join ttab2 cor2 on (cor1.b = cor2.d)", new String[][] { 
			{"1","1","1","1"},{"2","2","2","2"},{"1","1","1","1"},{"2","2","2","2"} } },
		{ "select * from ttab1, ttab1 left outer join ttab2 on (a=c)", "42X03"},
		{ "select * from ttab1 cor1, ttab1 left outer join ttab2 on (cor1.a=c)", "42972" },
		{ "select * from ttab1, ttab1 cor1 left outer join ttab2 on (cor1.a=c)", new String[][] {
			{"1","1","1","1","1","1"},{"2","2","1","1","1","1"},{"1","1","2","2","2","2"},{"2","2","2","2","2","2"} } },
		{ "drop table ttab1", null },
		{ "drop table ttab2", null },
		// FIXME
		// This query joins all four of these tables, colocation is not possible, all must be REPLICATEd
		{ "CREATE TABLE GOVT_AGCY (GVA_ID NUMERIC(20,0) NOT NULL, GVA_ORL_ID NUMERIC(20,0) NOT NULL, GVA_GAC_ID NUMERIC(20,0)) REPLICATE", null },
		{ "CREATE TABLE GEO_STRC_ELMT (GSE_ID NUMERIC(20,0) NOT NULL, GSE_GSET_ID NUMERIC(20,0) NOT NULL, GSE_GA_ID_PRNT NUMERIC(20,0) NOT NULL, GSE_GA_ID_CHLD NUMERIC(20,0) NOT NULL) REPLICATE", null },
		{ "CREATE TABLE GEO_AREA (GA_ID NUMERIC(20,0) NOT NULL, GA_GAT_ID NUMERIC(20,0) NOT NULL, GA_NM VARCHAR(30) NOT NULL, GA_ABRV_NM VARCHAR(5)) REPLICATE", null},
		{ "CREATE TABLE REG (REG_ID NUMERIC(20,0) NOT NULL, REG_NM VARCHAR(60) NOT NULL, REG_DESC VARCHAR(240), REG_ABRV_NM VARCHAR(15), REG_CD NUMERIC(8,0) NOT NULL, REG_STRT_DT TIMESTAMP NOT NULL, REG_END_DT TIMESTAMP NOT NULL, REG_EMPR_LIAB_IND CHAR(1) NOT NULL DEFAULT 'N',REG_PAYR_TAX_SURG_CRTF_IND CHAR(1) NOT NULL DEFAULT 'N', REG_PYT_ID NUMERIC(20,0), REG_GA_ID NUMERIC(20,0) NOT NULL, REG_GVA_ID NUMERIC(20,0) NOT NULL, REG_REGT_ID NUMERIC(20,0) NOT NULL, REG_PRNT_ID NUMERIC(20,0)) REPLICATE", null },
		{ "SELECT 1 FROM reg JOIN geo_area jrsd ON (jrsd.ga_id = reg.reg_ga_id) LEFT OUTER JOIN geo_strc_elmt gse ON (gse.gse_ga_id_chld = reg.reg_ga_id)      LEFT OUTER JOIN geo_area prnt ON (prnt.ga_id = reg.reg_ga_id)      JOIN govt_agcy gva ON (reg.reg_gva_id = gva.gva_id)", new String[0][0] },
		{ "drop table t1", null },
		{ "drop table t2", null },
		{ "drop table t3", null },
		{ "drop table tt1", null },
		{ "drop table tt2", null },
		{ "drop table tt3", null },
		{ "drop table insert_test", null },
		{ "drop table empty_table", null }
	   };

	    // Start 1 client and 3 servers, use default partitioning
	    startVMs(1, 3);

	    Connection conn = TestUtil.getConnection();
	    Statement stmt = conn.createStatement();
	    // Go through the array, execute each string[0], check sqlstate [1]
	    // This will fail on the first one that succeeds where it shouldn't
	    // or throws unknown exception
	    JDBC.SQLUnitTestHelper(stmt,Script_OuterJoinUT);
	  }

}
