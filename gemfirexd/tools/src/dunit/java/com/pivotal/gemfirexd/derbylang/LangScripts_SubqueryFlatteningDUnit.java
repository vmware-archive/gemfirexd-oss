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

public class LangScripts_SubqueryFlatteningDUnit extends
		DistributedSQLTestBase {

  @Override
  protected String reduceLogging() {
    return "config";
  }

	public LangScripts_SubqueryFlatteningDUnit(String name) {
		super(name);
		// TODO Auto-generated constructor stub
	}
	  public void testLangScript_SubqueryFlatteningTest() throws Exception
	  {
	    // This is a JUnit conversion of the Derby Lang SubqueryFlattening.sql script
	    // without any GemFireXD extensions
	    // The original test compared runtime plans, but that's not how we determine correct results for GemFireXD
		  
	    // Catch exceptions from illegal syntax
	    // Tests still not fixed marked FIXME
		  
	    // Array of SQL text to execute and sqlstates to expect
	    // The first object is a String, the second is either 
	    // 1) null - this means query returns no rows and throws no exceptions
	    // 2) a string - this means query returns no rows and throws expected SQLSTATE
	    // 3) a String array - this means query returns rows which must match (unordered) given resultset
	    //       - for an empty result set, an uninitialized size [0][0] array is used
	    Object[][] Script_SubqueryFlatteningUT = {
		// test subquery flattening into outer query block
	    // FIXME
	    // This needs partitioning and colocation but the queries join on all columns eventually, so use replicated tables
		{ "create table outer1 (c1 int, c2 int, c3 int) REPLICATE", null },
		{ "create table outer2 (c1 int, c2 int, c3 int) REPLICATE", null },
		{ "create table noidx (c1 int) REPLICATE", null },
		{ "create table idx1 (c1 int primary key) REPLICATE", null },
		{ "create unique index idx1_1 on idx1(c1)", null },
	     { "create table idx2 (c1 int, c2 int primary key) REPLICATE", null },
	     { "create unique index idx2_1 on idx2(c1, c2)", null },
	     { "create table nonunique_idx1 (c1 int)", null },
	     { "create index nonunique_idx1_1 on nonunique_idx1(c1)", null },
	     { "insert into outer1 values (1, 2, 3)", null },
	     { "insert into outer1 values (4, 5, 6)", null },
	     { "insert into outer2 values (1, 2, 3)", null },
	     { "insert into outer2 values (4, 5, 6)", null },
	     { "insert into noidx values 1, 1", null },
	     { "insert into idx1 values 1, 2", null },
	     { "insert into idx2 values (1, 1), (1, 2)", null },
	     { "insert into nonunique_idx1 values 1, 1", null },
	     { "select * from outer1 where c1 in (select idx1.c1 from noidx, idx1 where idx1.c1 = noidx.c1)", new String[][] { {"1","2","3"} } },
	     { "select * from outer1 o where c1 <= (select c1 from idx1 i group by c1)", "21000" },
	     { "select * from outer1 o where c1 + 0 = 1 or c1 in (select c1 from idx1 i where i.c1 = 0)", new String[][] { {"1","2","3"} } },
	     { "select * from outer1 o where c1 in (select c1 from idx1 i where i.c1 = 0) or c1 + 0 = 1", new String[][] { {"1","2","3"} } },
	     { "select (select c1 from idx1 where c1 = 0) from outer1", new String[][] { {null},{null} } },
	     { "select * from outer1 o where exists (select * from idx2 i, idx1 where o.c1 = i.c1 and i.c2 = idx1.c1)", new String[][] { {"1","2","3"} } },
	     { "select * from outer1 o where o.c1 in (select c1 from idx1)", new String[][] { {"1","2","3"} } },
		{ "select * from outer1 o where exists (select * from idx1 i where o.c1 = i.c1)", new String[][] { {"1","2","3"} } },
		{ "select * from outer1 o where o.c1 = ANY (select c1 from idx1)", new String[][] { {"1","2","3"} } },
		{ "select * from outer1 o where o.c2 > ANY (select c1 from idx1 i where o.c1 = i.c1)", new String[][] { {"1","2","3"} } },
		{ "select * from outer1 o where exists (select * from idx2 i where o.c1 = i.c1 and i.c2 = 2)", new String[][] { {"1","2","3"} } },
		{ "select * from outer1 o where exists (select * from idx2 i, idx1 where o.c1 = i.c1 and i.c2 = idx1.c1 and i.c2 = 1)", new String[][] { {"1","2","3"} } },
		{ "select * from outer1 o where exists (select * from idx1 where idx1.c1 = 1 + 0)", new String[][] { {"1","2","3"},{"4","5","6"} } },
	     { "select * from outer1 o where exists (select * from idx2 i, idx1 where o.c1 + 0 = i.c1 and i.c2 + 0 = idx1.c1 and i.c2 = 1 + 0)", new String[][] { {"1","2","3"} } },
		{ "select * from outer1 o where exists (select * from idx2 i where exists (select * from idx1 ii where o.c1 = i.c1 and i.c2 = ii.c1 and i.c2 = 1))", new String[][] { {"1","2","3"} } },
		{ "select * from outer1 o where exists (select * from idx2 i where exists (select * from idx1 ii where o.c1 = i.c1 and i.c2 = ii.c1))", new String[][] { {"1","2","3"} } },
		{ "select * from outer1 o where exists (select * from idx2 i where  o.c1 = i.c1 and i.c2 = 1 and exists (select * from idx1 ii))", new String[][] { {"1","2","3"} } },
		{ "select * from outer1 o where c1 in (select (select c1 from idx1 where c1 = i.c1) from idx2 i where o.c1 = i.c1 and i.c2 = 1)", new String[][] { {"1","2","3"} } },
		{ "select * from outer1 o where o.c1 = (select c1 from idx1 i where o.c1 = i.c1)", new String[][] { {"1","2","3"} } },
	     { "select * from outer1 o where o.c1 <= (select c1 from idx1 i where o.c1 = i.c1)", new String[][] { {"1","2","3"} } },
		{ "select * from outer1 where c1 in (select c1 from noidx)", new String[][] { {"1","2","3"} } },
		{ "select * from outer1 where c1 in (select c1 from nonunique_idx1)", new String[][] { {"1","2","3"} } },
		{ "select * from outer1 where c1 in (select c1 from idx2)", new String[][] { {"1","2","3"} } },
		{ "select * from outer1 where exists (select * from idx1 where c1 = c1)", new String[][] { {"1","2","3"},{"4","5","6"} } },
		{ "select * from outer1 where c1 in (values 1)", new String[][] { {"1","2","3"} } },
	     { "select * from outer1 where c1 in (values (select max(c1) from outer1))", new String[][] { {"4","5","6"} } },
	     { "select o.c1 from outer1 o join outer2 o2 on (o.c1 = o2.c1) where exists (select c1 from idx1)", new String[][] { {"4"},{"1"} } },
		{ "select o.c1 from outer1 o join outer2 o2 on (o.c1 = o2.c1) where o.c1 in (select c1 from idx1)", new String[][] { {"1"} } },
		{ "select c1 from (select t.c1 from (select o.c1 from outer1 o join outer2 o2 on (o.c1 = o2.c1) where exists (select c1 from idx1)) t, outer2 where t.c1 = outer2.c1) t2", new String[][] { {"4"},{"1"} } },
		{ "create table business(businesskey int, name varchar(50), changedate int) REPLICATE", null },
	     { "create table nameelement(parentkey int, parentelt varchar(50), seqnum int) REPLICATE", null },
	     { "create table categorybag(cbparentkey int, cbparentelt varchar(50), krtModelKey varchar(50), keyvalue varchar(50))", null },
	     { "select businesskey, name, changedate from business as biz left outer join nameelement as nameElt on (businesskey = parentkey and parentelt = 'businessEntity') where (nameElt.seqnum = 1) and businesskey in (select cbparentkey from categorybag 	where (cbparentelt = 'businessEntity') and  (krtModelKey = 'UUID:CD153257-086A-4237-B336-6BDCBDCC6634' and keyvalue = '40.00.00.00.00'))  order by name asc , biz.changedate asc", new String[0][0] },
		{ "drop table outer1", null },
	     { "drop table outer2", null },
	     { "drop table noidx", null },
	     { "drop table idx1", null },
	     { "drop table idx2", null },
	     { "drop table nonunique_idx1", null },
	     { "drop table business", null },
	     { "drop table nameelement", null },
	     { "drop table categorybag", null },
	     { "CREATE TABLE COLLS (ID VARCHAR(128) NOT NULL, COLLID SMALLINT NOT NULL) REPLICATE", null },
	     { "CREATE INDEX NEW_INDEX3 ON COLLS (COLLID)", null },
	     { "CREATE INDEX NEW_INDEX2 ON COLLS (ID)", null },
	     { "ALTER TABLE COLLS ADD CONSTRAINT NEW_KEY2 UNIQUE (ID, COLLID)", null },
	     { "CREATE TABLE DOCS (ID VARCHAR(128) NOT NULL) REPLICATE", null },
	     { "CREATE INDEX NEW_INDEX1 ON DOCS (ID)", null },
	     { "ALTER TABLE DOCS ADD CONSTRAINT NEW_KEY1 PRIMARY KEY (ID)", null },
	     { "insert into colls values ('123', 2)", null },
	     { "insert into colls values ('124', -5)", null },
	     { "insert into colls values ('24', 1)", null },
	     { "insert into colls values ('26', -2)", null },
	     { "insert into colls values ('36', 1)", null },
	     { "insert into colls values ('37', 8)", null },
	     { "insert into docs values '24', '25', '36', '27', '124', '567'", null },
		{ "SELECT COUNT(*) FROM ( SELECT ID FROM DOCS WHERE ( ID NOT IN (SELECT ID FROM COLLS WHERE COLLID IN (-2,1) ) )) AS TAB", new String[][] { {"4"} } },
		{ "SELECT COUNT(*) FROM ( SELECT ID FROM DOCS WHERE ( NOT EXISTS  (SELECT ID FROM COLLS WHERE DOCS.ID = COLLS.ID AND COLLID IN (-2,1) ) )) AS TAB", new String[][] { {"4"} } },
		{ "SELECT COUNT(*) FROM ( SELECT ID FROM DOCS WHERE ( EXISTS  (SELECT ID FROM COLLS WHERE DOCS.ID = COLLS.ID AND COLLID IN (-2,1) ) )) AS TAB", new String[][] { {"2"} } },
		{ "SELECT count(ID) FROM DOCS WHERE ID IN (SELECT ID FROM COLLS WHERE COLLID IN (-2,1) )", new String[][] { {"2"} } },
		{ "SELECT count(ID) FROM DOCS WHERE ID > ANY (SELECT ID FROM COLLS WHERE COLLID IN (-2,1) )", new String[][] { {"4"} } },
		{ "SELECT count(ID) FROM DOCS WHERE ID <> ANY (SELECT ID FROM COLLS WHERE COLLID IN (-2,1) )", new String[][] { {"6"} } },
		{ "SELECT count(ID) FROM DOCS WHERE ID = ALL (SELECT ID FROM COLLS WHERE COLLID IN (-2,1) )", new String[][] { {"0"} } },
		{ "SELECT count(ID) FROM DOCS WHERE ID < ALL (SELECT ID FROM COLLS WHERE COLLID IN (-2,1) )", new String[][] { {"1"} } },
		{ "SELECT count(ID) FROM DOCS WHERE ID <> ALL (SELECT ID FROM COLLS WHERE COLLID IN (-2,1) )", new String[][] { {"4"} } },
		{ "drop table colls", null },
		{ "CREATE TABLE COLLS (ID VARCHAR(128), COLLID SMALLINT NOT NULL) REPLICATE", null },
	     { "CREATE INDEX NEW_INDEX3 ON COLLS (COLLID)", null },
	     { "CREATE INDEX NEW_INDEX2 ON COLLS (ID)", null },
	     { "insert into colls values ('123', 2)", null },
	     { "insert into colls values ('124', -5)", null },
	     { "insert into colls values ('24', 1)", null },
	     { "insert into colls values ('26', -2)", null },
	     { "insert into colls values ('36', 1)", null },
	     { "insert into colls values ('37', 8)", null },
	     { "insert into colls values (null, -2)", null },
	     { "insert into colls values (null, 1)", null },
	     { "insert into colls values (null, 8)", null },
		{ "SELECT COUNT(*) FROM ( SELECT ID FROM DOCS WHERE ( NOT EXISTS  (SELECT ID FROM COLLS WHERE DOCS.ID = COLLS.ID AND COLLID IN (-2,1) ) )) AS TAB", new String[][] { {"4"} } },
		{ "SELECT COUNT(*) FROM ( SELECT ID FROM DOCS WHERE ( EXISTS  (SELECT ID FROM COLLS WHERE DOCS.ID = COLLS.ID AND COLLID IN (-2,1) ) )) AS TAB", new String[][] { {"2"} } },
		{ "SELECT count(ID) FROM DOCS WHERE ID IN (SELECT ID FROM COLLS WHERE COLLID IN (-2,1) )", new String[][] { {"2"} } },
		{ "SELECT count(ID) FROM DOCS WHERE ID > ANY (SELECT ID FROM COLLS WHERE COLLID IN (-2,1) )", new String[][] { {"4"} } },
		{ "SELECT count(ID) FROM DOCS WHERE ID <> ALL (SELECT ID FROM COLLS WHERE COLLID IN (-2,1) )", new String[][] { {"0"} } },
		{ "drop table docs", null },
	     { "CREATE TABLE DOCS (ID VARCHAR(128)) REPLICATE", null },
	     { "CREATE INDEX NEW_INDEX1 ON DOCS (ID)", null },
	     { "insert into docs values '24', '25', '36', '27', '124', '567'", null },
	     { "insert into docs values null", null },
		{ "SELECT COUNT(*) FROM ( SELECT ID FROM DOCS WHERE ( NOT EXISTS  (SELECT ID FROM COLLS WHERE DOCS.ID = COLLS.ID AND COLLID IN (-2,1) ) )) AS TAB", new String[][] { {"5"} } },
		{ "SELECT COUNT(*) FROM ( SELECT ID FROM DOCS WHERE ( EXISTS  (SELECT ID FROM COLLS WHERE DOCS.ID = COLLS.ID AND COLLID IN (-2,1) ) )) AS TAB", new String[][] { {"2"} } },
		{ "SELECT count(ID) FROM DOCS WHERE ID IN (SELECT ID FROM COLLS WHERE COLLID IN (-2,1) )", new String[][] { {"2"} } },
		{ "SELECT count(ID) FROM DOCS WHERE ID > ANY (SELECT ID FROM COLLS WHERE COLLID IN (-2,1) )", new String[][] { {"4"} } },
		{ "SELECT count(ID) FROM DOCS WHERE ID <> ALL (SELECT ID FROM COLLS WHERE COLLID IN (-2,1) )", new String[][] { {"0"} } },
	     { "create table t1 (c1 int not null) REPLICATE", null },
	     { "create table t2 (c1 int not null) REPLICATE", null },
	     { "create table t3 (c1 int not null) REPLICATE", null },
	     { "create table t4 (c1 int) REPLICATE", null },
	     { "insert into t1 values 1,2,3,4,5,1,2", null },
	     { "insert into t2 values 1,4,5,1,1,5,4", null },
	     { "insert into t3 values 4,4,3,3", null },
	     { "insert into t4 values 1,1,2,2,3,4,5,5", null },
	     { "select * from t1 where not exists (select * from t2 where t1.c1=t2.c1)", new String[][] { {"2"},{"3"},{"2"} } },
	     { "select * from t1 where not exists (select * from t2 where t1.c1=t2.c1 and t2.c1 not in (select t3.c1 from t3, t4))", "0A000" },
	     { "drop table colls", null },
	     { "drop table docs", null },
	     { "drop table t1", null },
	     { "drop table t2", null },
	     { "drop table t3", null },
	     { "drop table t4", null },
		{ "create table digits (d int) partition by column(d)", null },
	     { "insert into digits values 1, 2, 3, 4, 5, 6, 7, 8, 9, 0", null },
	     { "create table odd (o int) partition by column(o) colocate with (digits)", null },
	     { "insert into odd values 1, 3, 5, 7, 9", null },
		// This query may hang?
		{ "select distinct temp_t0.d from (select d from digits where d > 3) temp_t0, (select o from odd) temp_t1, odd temp_t4, (select o from odd) temp_t3 where temp_t0.d = temp_t1.o and temp_t0.d = temp_t3.o and temp_t0.d in (select o from odd where o = temp_t1.o) and exists (select d from digits where d = temp_t0.d)", new String[][] { {"5"},{"7"},{"9"} } },
		{ "drop table odd", null },
	     { "drop table digits", null },
		{ "CREATE TABLE tab_a (PId BIGINT NOT NULL) REPLICATE", null },
	     { "CREATE TABLE tab_c (Id BIGINT NOT NULL PRIMARY KEY,  PAId BIGINT NOT NULL, PBId BIGINT NOT NULL)", null },
	     { "INSERT INTO tab_c VALUES (91, 81, 82)", null },
	     { "INSERT INTO tab_c VALUES (92, 81, 84)", null },
	     { "INSERT INTO tab_c VALUES (93, 81, 88)", null },
	     { "INSERT INTO tab_c VALUES (96, 81, 83)", null },
	     { "CREATE TABLE tab_v (OId BIGINT NOT NULL, UGId BIGINT NOT NULL, val CHAR(1) NOT NULL) REPLICATE", null },
	     { "CREATE UNIQUE INDEX tab_v_i1 ON tab_v (OId, UGId, val)", null },
	     { "CREATE INDEX tab_v_i2 ON tab_v (UGId, val, OId)", null },
	     { "INSERT INTO tab_v VALUES (81, 31, 'A')", null },
	     { "INSERT INTO tab_v VALUES (82, 31, 'A')", null },
	     { "INSERT INTO tab_v VALUES (83, 31, 'A')", null },
	     { "INSERT INTO tab_v VALUES (84, 31, 'A')", null },
	     { "INSERT INTO tab_v VALUES (85, 31, 'A')", null },
	     { "INSERT INTO tab_v VALUES (86, 31, 'A')", null },
	     { "INSERT INTO tab_v VALUES (87, 31, 'A')", null },
	     { "INSERT INTO tab_v VALUES (81, 32, 'A')", null },
	     { "INSERT INTO tab_v VALUES (82, 32, 'A')", null },
	     { "INSERT INTO tab_v VALUES (83, 32, 'A')", null },
	     { "INSERT INTO tab_v VALUES (84, 32, 'A')", null },
	     { "INSERT INTO tab_v VALUES (85, 32, 'A')", null },
	     { "INSERT INTO tab_v VALUES (86, 32, 'A')", null },
	     { "INSERT INTO tab_v VALUES (87, 32, 'A')", null },
	     { "CREATE TABLE tab_b (Id BIGINT NOT NULL PRIMARY KEY, OId BIGINT NOT NULL) REPLICATE", null },
	     { "INSERT INTO tab_b VALUES (141, 81)", null },
	     { "INSERT INTO tab_b VALUES (142, 82)", null },
	     { "INSERT INTO tab_b VALUES (143, 84)", null },
	     { "INSERT INTO tab_b VALUES (144, 88)", null },
	     { "INSERT INTO tab_b VALUES (151, 81)", null },
	     { "INSERT INTO tab_b VALUES (152, 83)", null },
	     { "CREATE TABLE tab_d (Id BIGINT NOT NULL PRIMARY KEY, PAId BIGINT NOT NULL, PBId BIGINT NOT NULL) REPLICATE", null },
	     { "INSERT INTO tab_d VALUES (181, 141, 142)", null },
	     { "INSERT INTO tab_d VALUES (182, 141, 143)", null },
	     { "INSERT INTO tab_d VALUES (186, 151, 152)", null },
		{ "SELECT tab_b.Id FROM tab_b JOIN tab_c ON (tab_b.OId = tab_c.PAId OR tab_b.OId = tab_c.PBId) LEFT OUTER JOIN tab_a ON tab_b.OId = PId WHERE EXISTS (SELECT 'X' FROM tab_d WHERE (PAId = 141 AND PBId = tab_b.Id) OR (PBId = 141 AND PAId = tab_b.Id)) AND EXISTS (SELECT 'X' FROM tab_v WHERE OId = tab_b.OId AND UGId = 31 AND val = 'A')", new String[][] { {"143"},{"142"} } },
	     { "drop table tab_d", null },
	     { "drop table tab_b", null },
	     { "drop table tab_v", null },
	     { "drop table tab_c", null }
	   };

	    // Start 1 client and 3 servers, use default partitioning
	    startVMs(1, 3);

	    Connection conn = TestUtil.getConnection();
	    Statement stmt = conn.createStatement();
	    // Go through the array, execute each string[0], check sqlstate [1]
	    // This will fail on the first one that succeeds where it shouldn't
	    // or throws unknown exception
	    JDBC.SQLUnitTestHelper(stmt,Script_SubqueryFlatteningUT);
	  }

}
