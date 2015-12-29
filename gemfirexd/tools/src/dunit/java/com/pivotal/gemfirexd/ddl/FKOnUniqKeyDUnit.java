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
package com.pivotal.gemfirexd.ddl;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import com.pivotal.gemfirexd.DistributedSQLTestBase;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.jdbc.FKOnUniqueKeyColumnsTest;

@SuppressWarnings("serial")
public class FKOnUniqKeyDUnit extends DistributedSQLTestBase {

  public FKOnUniqKeyDUnit(String name) {
    super(name);
  }

  public void testFKOnUniqueColumnsWhichIsInGlobalIndex() throws Exception {
    startVMs(1, 3);
    Connection conn = TestUtil.getConnection();
    Statement s = conn.createStatement();
    for (int i = 0; i < 4; i++) {
      System.out.println("testFKOnUniqueColumnsWhichIsInGlobalIndex " + i);
      String baseTableType = FKOnUniqueKeyColumnsTest.getBaseType(i);
      String referbaseTableType = FKOnUniqueKeyColumnsTest.getReferBaseType(i);
      s.execute("create table base"
          + "(c1 int not null primary key, c2 int not null unique) " + baseTableType);
      s.execute("insert into base values(1, 2), (12, 11), (22, 21), (31, 32)");

      s.execute("create table referbase(rc1 int not null, rc2 int not null,"
          + "foreign key (rc2) references base(c2))" + referbaseTableType);

      s.execute("insert into referbase values (1, 11), (2, 21)");

      try {
        s.execute("insert into referbase values (1, 8)");
        fail("above insert violating fk so should fail");
      } catch (SQLException ex) {
        if (!"23503".equals(ex.getSQLState())) {
          throw ex;
        }
      }
      s.execute("drop table referbase");
      s.execute("drop table base");
    }
  }
  
  public void testFKOnUniqueColumnsWhichIsInLocalIndex() throws Exception {
    startVMs(1, 3);
    Connection conn = TestUtil.getConnection();
    Statement s = conn.createStatement();
    for (int i = 0; i < 2; i++) {
      System.out.println("testFKOnUniqueColumnsWhichIsInLocalIndex " + i);
      String referbaseTableType = FKOnUniqueKeyColumnsTest.getReferBaseType(i);
      s.execute("create table base"
          + "(c1 int not null primary key, c2 int not null unique) partition by column(c2)");
      s.execute("insert into base values(1, 2), (12, 11), (22, 21), (31, 32)");

      s.execute("create table referbase(rc1 int not null, rc2 int not null,"
          + "foreign key (rc2) references base(c2))" + referbaseTableType);

      s.execute("insert into referbase values (1, 11), (2, 21)");

      try {
        s.execute("insert into referbase values (1, 8)");
        fail("above insert violating fk so should fail");
      } catch (SQLException ex) {
        if (!"23503".equals(ex.getSQLState())) {
          throw ex;
        }
      }
      s.execute("drop table referbase");
      s.execute("drop table base");
    }
  }
  
  public void testFKOnUniqueColumnsWhichIsInGlobalIndex_compositeKey() throws Exception {
    startVMs(1, 3);
    Connection conn = TestUtil.getConnection();
    Statement s = conn.createStatement();
    for (int i = 0; i < 4; i++) {
      System.out.println("testFKOnUniqueColumnsWhichIsInGlobalIndex_compositeKey " + i);
      String baseTableType = FKOnUniqueKeyColumnsTest.getBaseType(i);
      String referbaseTableType = FKOnUniqueKeyColumnsTest.getReferBaseType(i);
      s.execute("create table base"
          + "(c1 int not null primary key, c2 int not null, c3 int not null, "
          + " unique(c2, c3)) " + baseTableType);
      s.execute("insert into base values(1, 2, 2), (11, 12, 12), (21, 22, 22), (31, 32, 32)");

      s.execute("create table referbase(rc1 int not null, rc2 int not null,"
          + "rc3 int not null, foreign key (rc2, rc3) references base(c2, c3)) " + referbaseTableType);

      s.execute("insert into referbase values (1, 2, 2), (2, 22, 22)");

      try {
        s.execute("insert into referbase values (1, 8, 4)");
        fail("above insert violating fk so should fail");
      } catch (SQLException ex) {
        if (!"23503".equals(ex.getSQLState())) {
          throw ex;
        }
      }
      s.execute("drop table referbase");
      s.execute("drop table base");
    }
  }
  
  public void testFKOnUniqueColumnsWhichIsInLocalIndex_compositeKey() throws Exception {
    startVMs(1, 3);
    Connection conn = TestUtil.getConnection();
    Statement s = conn.createStatement();
    for (int i = 0; i < 2; i++) {
      System.out.println("testFKOnUniqueColumnsWhichIsInLocalIndex_compositeKey " + i);
      String referbaseTableType = FKOnUniqueKeyColumnsTest.getReferBaseType(i);
      s.execute("create table base"
          + "(c1 int not null primary key, c2 int not null, c3 int not null,"
          + " unique(c2, c3)) partition by column(c2, c3)");
      s.execute("insert into base values(1, 2, 3), (11, 12, 13), (21, 22, 23), (31, 32, 33)");

      s.execute("create table referbase(rc1 int not null,"
          + " rc2 int not null, rc3 int not null, "
          + "foreign key (rc2, rc3) references base(c2, c3))" + referbaseTableType);

      s.execute("insert into referbase values (1, 12, 13), (2, 22, 23)");

      try {
        s.execute("insert into referbase values (1, 8, 10)");
        fail("above insert violating fk so should fail");
      } catch (SQLException ex) {
        if (!"23503".equals(ex.getSQLState())) {
          throw ex;
        }
      }
      s.execute("drop table referbase");
      s.execute("drop table base");
    }
  }
  
  public void testFKOnUniqueColumnsWhichIsInGlobalIndex_compositeKey_MixType() throws Exception {
    startVMs(1, 3);
    Connection conn = TestUtil.getConnection();
    Statement s = conn.createStatement();
    for (int i = 0; i < 4; i++) {
      System.out.println("testFKOnUniqueColumnsWhichIsInGlobalIndex_compositeKey_MixType " + i);
      String baseTableType = FKOnUniqueKeyColumnsTest.getBaseType(i);
      String referbaseTableType = FKOnUniqueKeyColumnsTest.getReferBaseType(i);
      s.execute("create table base"
          + "(c1 int not null primary key, c2 int not null, c3 varchar(20) not null, "
          + " unique(c2, c3)) " + baseTableType);
      s.execute("insert into base values(1, 2, 'name1'), (11, 12, 'name2'), "
          + "(21, 22, 'name3'), (31, 32, 'name4')");

      s.execute("create table referbase(rc1 int not null, rc2 int not null,"
          + "rc3 varchar(20) not null, foreign key (rc2, rc3) references base(c2, c3))" + referbaseTableType);

      s.execute("insert into referbase values (1, 2, 'name1'), (2, 22, 'name3')");

      try {
        s.execute("insert into referbase values (1, 2, 'myname')");
        fail("above insert violating fk so should fail");
      } catch (SQLException ex) {
        if (!"23503".equals(ex.getSQLState())) {
          throw ex;
        }
      }
      s.execute("drop table referbase");
      s.execute("drop table base");
    }
  }
  
  public void testFKOnUniqueColumnsWhichIsInLocalIndex_compositeKey_MixType() throws Exception {
    startVMs(1, 3);
    Connection conn = TestUtil.getConnection();
    Statement s = conn.createStatement();
    for (int i = 0; i < 2; i++) {
      System.out.println("testFKOnUniqueColumnsWhichIsInLocalIndex_compositeKey_MixType " + i);
      String referbaseTableType = FKOnUniqueKeyColumnsTest.getReferBaseType(i);
      s.execute("create table base"
          + "(c1 int not null primary key, c2 int not null, c3 varchar(20) not null, "
          + " unique(c2, c3)) partition by column(c2, c3)");
      s.execute("insert into base values(1, 2, 'name1'), (11, 12, 'name2'),"
          + " (21, 22, 'name3'), (31, 32, 'name4')");

      s.execute("create table referbase(rc1 int not null,"
          + " rc2 int not null, rc3 varchar(20) not null, "
          + "foreign key (rc2, rc3) references base(c2, c3))" + referbaseTableType);

      s.execute("insert into referbase values (1, 2, 'name1'), (4, 22, 'name3')");

      try {
        s.execute("insert into referbase values (1, 2, 'myname')");
        fail("above insert violating fk so should fail");
      } catch (SQLException ex) {
        if (!"23503".equals(ex.getSQLState())) {
          throw ex;
        }
      }
      s.execute("drop table referbase");
      s.execute("drop table base");
    }
  }

  /**
   * This is the case where unique columns are a superset of partitioning
   * columns.
   */
  public void testFKOnUniqueColumnsForSupersetPartitioning() throws Exception {
    startVMs(1, 3);
    Connection conn = TestUtil.getConnection();
    Statement s = conn.createStatement();
    for (int i = 0; i < 2; i++) {
      System.out.println("testFKOnUniqueColumnsForSupersetPartitioning " + i);
      String referbaseTableType = FKOnUniqueKeyColumnsTest.getReferBaseType(i);
      s.execute("create table base"
          + "(c1 int not null primary key, c11 varchar(10), c12 int, "
          + "c2 int not null, c3 varchar(20) not null, unique (c3, c2)) "
          + "partition by column(c3)");
      s.execute("insert into base values(1, '1', 1, 2, 'name1'), "
          + "(11, '11', 1, 12, 'name2'), (21, '21', 21, 22, 'name3'), "
          + "(31, '31', 31, 32, 'name4')");

      s.execute("create table referbase(rc1 int not null, "
          + "rc2 varchar(20) not null, rc3 int not null, "
          + "foreign key (rc2, rc3) references base(c3, c2))"
          + referbaseTableType);

      s.execute("insert into referbase (rc1, rc2, rc3) values (1, 'name1', 2), "
          + "(4, 'name3', 22)");

      try {
        s.execute("insert into referbase (rc1, rc2, rc3) "
            + "values (1, 'myname', 2)");
        fail("above insert violating fk so should fail");
      } catch (SQLException ex) {
        if (!"23503".equals(ex.getSQLState())) {
          throw ex;
        }
      }
      s.execute("drop table referbase");
      s.execute("drop table base");
    }

    for (int i = 0; i < 2; i++) {
      System.out.println("testFKOnUniqueColumnsForSupersetPartitioning " + i);
      String referbaseTableType = FKOnUniqueKeyColumnsTest.getReferBaseType(i);
      s.execute("create table base"
          + "(c1 int not null primary key, c11 varchar(10), c12 int, "
          + "c2 int not null, c3 varchar(20) not null, unique (c2, c3)) "
          + "partition by column(c3)");
      s.execute("insert into base values(1, '1', 1, 2, 'name1'), "
          + "(11, '11', 1, 12, 'name2'), (21, '21', 21, 22, 'name3'), "
          + "(31, '31', 31, 32, 'name4')");

      s.execute("create table referbase(rc1 int not null, "
          + "rc3 int not null, rc2 varchar(20) not null, "
          + "foreign key (rc3, rc2) references base(c2, c3))"
          + referbaseTableType);

      s.execute("insert into referbase (rc1, rc2, rc3) values (1, 'name1', 2), "
          + "(4, 'name3', 22)");

      try {
        s.execute("insert into referbase (rc1, rc2, rc3) "
            + "values (1, 'myname', 2)");
        fail("above insert violating fk so should fail");
      } catch (SQLException ex) {
        if (!"23503".equals(ex.getSQLState())) {
          throw ex;
        }
      }
      s.execute("drop table referbase");
      s.execute("drop table base");
    }
  }
  
  public void testDeleteWhenFKOnUniqueColumns() throws Exception {
    startVMs(1, 3);
    Connection conn = TestUtil.getConnection();
    Statement s = conn.createStatement();

    s.execute("create table base"
        + "(c1 int not null primary key, c2 int not null, c3 varchar(20) not null, "
        + " unique(c2, c3)) partition by column(c2, c3)");
    s.execute("insert into base values(1, 2, 'name1'), (11, 12, 'name2'),"
        + " (21, 22, 'name3'), (31, 32, 'name4')");

    s.execute("create table referbase_replicate(rc1 int not null,"
        + " rc2 int not null, rc3 varchar(20) not null, "
        + "foreign key (rc2, rc3) references base(c2, c3)) replicate");

    s.execute("insert into referbase_replicate values (1, 2, 'name1'), (4, 22, 'name3')");
    try {
      s.execute("delete from base where c2 = 2");
      fail("above delete should fail");
    } catch (SQLException ex) {
      if (!"23503".equals(ex.getSQLState())) {
        throw ex;
      }
    }
    s.execute("create table referbase_partitioned(rc1 int not null,"
        + " rc2 int not null, rc3 varchar(20) not null, "
        + "foreign key (rc2, rc3) references base(c2, c3)) partition by column(rc1)");

    s.execute("insert into referbase_partitioned values (1, 2, 'name1'), (4, 22, 'name3')");
    try {
      s.execute("delete from base where c2 = 2");
      fail("above delete should fail");
    } catch (SQLException ex) {
      if (!"23503".equals(ex.getSQLState())) {
        throw ex;
      }
    }
  }
}
