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
package com.pivotal.gemfirexd.consistency;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import org.apache.derbyTesting.junit.JDBC;

import com.gemstone.gemfire.cache.EntryExistsException;
import com.pivotal.gemfirexd.DistributedSQLTestBase;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.engine.distributed.FunctionExecutionException;

@SuppressWarnings("serial")
public class TableIndicesConsistencyDUnit extends DistributedSQLTestBase {

  public TableIndicesConsistencyDUnit(String name) {
    super(name);
  }

  public void testOnPartitionTable() throws Exception {
    // Start one client and two servers
    startVMs(1, 2);

    clientSQLExecute(1,
        "create table EMP.PARTITIONTESTTABLE (ID int not null, "
            + "SECONDID int not null, THIRD int not null Unique, "
            + "FOURTH int not null, FIFTH int not null Unique, "
            + "primary key (ID, SECONDID)) PARTITION BY COLUMN (ID)");
    // clientSQLExecute(1,"create unique index third_index on
    // EMP.PARTITIONTESTTABLE (THIRD)");
    clientSQLExecute(1,
        "create index fourth_index on EMP.PARTITIONTESTTABLE (FOURTH)");
    // clientSQLExecute(1,"create unique index fifth_index on
    // EMP.PARTITIONTESTTABLE (fifth)");

    clientSQLExecute(1,
        "INSERT INTO EMP.PARTITIONTESTTABLE values(1,2,3,4, 10)");
    clientSQLExecute(1,
        "INSERT INTO EMP.PARTITIONTESTTABLE values(4,5,6,4, 11)");
    clientSQLExecute(1,
        "INSERT INTO EMP.PARTITIONTESTTABLE values(7,8,9,4, 12)");

    addExpectedException(new int[] { 1 }, new int[] { 1, 2 }, new Object[] {
        FunctionExecutionException.class, EntryExistsException.class });
    checkKeyViolation(1,
        "INSERT INTO EMP.PARTITIONTESTTABLE values(1,3,3,4,13)");
    checkKeyViolation(-1,
        "INSERT INTO EMP.PARTITIONTESTTABLE values(1,3,3,4,13)");
    checkKeyViolation(-2,
        "INSERT INTO EMP.PARTITIONTESTTABLE values(1,3,3,4,13)");
    removeExpectedException(new int[] { 1 }, new int[] { 1, 2 }, new Object[] {
        FunctionExecutionException.class, EntryExistsException.class });

    sqlExecuteVerify(null, new int[] { 1, 2 },
        "VALUES SYSCS_UTIL.CHECK_TABLE('EMP','PARTITIONTESTTABLE')",
        null, "1");

    ResultSet rs = null;
    PreparedStatement ps = null;
    ps = TestUtil.jdbcConn
        .prepareStatement("Select * from EMP.PARTITIONTESTTABLE "
            + "where id=1 and secondid=3");
    rs = ps.executeQuery();
    JDBC.assertDrainResults(rs, 0);

    String[][] expectedRows = { { "1", "2", "3", "4", "10" } };
    ps = TestUtil.jdbcConn
        .prepareStatement("Select * from EMP.PARTITIONTESTTABLE where THIRD=3");
    rs = ps.executeQuery();
    JDBC.assertFullResultSet(rs, expectedRows);
    rs.close();

    sqlExecuteVerify(new int[] { 1 }, new int[] { 1, 2 },
        "Select * from EMP.PARTITIONTESTTABLE where FOURTH=4", TestUtil.getResourcesDir()
            + "/lib/checkTableIndicesConsistency.xml", "rs1");

    addExpectedException(new int[] { 1 }, new int[] { 1, 2 }, new Object[] {
        FunctionExecutionException.class, EntryExistsException.class });
    checkKeyViolation(1,
        "INSERT INTO EMP.PARTITIONTESTTABLE values(1,2,5,4,13)");
    checkKeyViolation(-1,
        "INSERT INTO EMP.PARTITIONTESTTABLE values(1,2,5,4,13)");
    checkKeyViolation(-2,
        "INSERT INTO EMP.PARTITIONTESTTABLE values(1,2,5,4,13)");
    removeExpectedException(new int[] { 1 }, new int[] { 1, 2 }, new Object[] {
        FunctionExecutionException.class, EntryExistsException.class });

    sqlExecuteVerify(null, new int[] { 1, 2 },
        "VALUES SYSCS_UTIL.CHECK_TABLE('EMP','PARTITIONTESTTABLE')",
        null, "1");

    ps = TestUtil.jdbcConn
        .prepareStatement("Select * from EMP.PARTITIONTESTTABLE "
            + "where id=1 and secondid=2");
    rs = ps.executeQuery();
    JDBC.assertFullResultSet(rs, expectedRows);
    rs.close();

    ps = TestUtil.jdbcConn
        .prepareStatement("Select * from EMP.PARTITIONTESTTABLE where THIRD=5");
    rs = ps.executeQuery();
    JDBC.assertDrainResults(rs, 0);

    sqlExecuteVerify(new int[] { 1 }, new int[] { 1, 2 },
        "Select * from EMP.PARTITIONTESTTABLE where FOURTH=4", TestUtil.getResourcesDir()
            + "/lib/checkTableIndicesConsistency.xml", "rs1");

    try {
      addExpectedException(new int[] { 1 }, new int[] { 1, 2 }, new Object[] {
          FunctionExecutionException.class, EntryExistsException.class });
      checkKeyViolation(1,
          "UPDATE EMP.PARTITIONTESTTABLE SET third=6, fifth=20 "
              + "where id=1 and secondid=2");
      checkKeyViolation(-1,
          "UPDATE EMP.PARTITIONTESTTABLE SET third=6, fifth=20 "
              + "where id=1 and secondid=2");
      checkKeyViolation(-2,
          "UPDATE EMP.PARTITIONTESTTABLE SET third=6, fifth=20 "
              + "where id=1 and secondid=2");
    } finally {
      removeExpectedException(new int[] { 1 }, new int[] { 1, 2 },
          new Object[] { FunctionExecutionException.class,
              EntryExistsException.class });
    }

    sqlExecuteVerify(null, new int[] { 1, 2 },
        "VALUES SYSCS_UTIL.CHECK_TABLE('EMP','PARTITIONTESTTABLE')",
        null, "1");
    ps = TestUtil.jdbcConn
        .prepareStatement("Select * from EMP.PARTITIONTESTTABLE "
            + "where id=1 and secondid=2");
    rs = ps.executeQuery();
    JDBC.assertFullResultSet(rs, expectedRows);
    rs.close();

    ps = TestUtil.jdbcConn
        .prepareStatement("Select * from EMP.PARTITIONTESTTABLE where THIRD=5");
    rs = ps.executeQuery();
    JDBC.assertDrainResults(rs, 0);

    sqlExecuteVerify(new int[] { 1 }, new int[] { 1, 2 },
        "Select * from EMP.PARTITIONTESTTABLE where FOURTH=4", TestUtil.getResourcesDir()
            + "/lib/checkTableIndicesConsistency.xml", "rs1");

    clientSQLExecute(1, "DELETE FROM EMP.PARTITIONTESTTABLE WHERE id=1");
    sqlExecuteVerify(null, new int[] { 1, 2 },
        "VALUES SYSCS_UTIL.CHECK_TABLE('EMP','PARTITIONTESTTABLE')",
        null, "1");
    ps = TestUtil.jdbcConn
        .prepareStatement("Select * from EMP.PARTITIONTESTTABLE "
            + "where id=1 and secondid=2");
    rs = ps.executeQuery();
    JDBC.assertDrainResults(rs, 0);

    // drop the table
    clientSQLExecute(1, "drop table EMP.PARTITIONTESTTABLE");
  }

  public void testOnReplicateTable() throws Exception {
    // Start one client and two servers
    startVMs(1, 2);

    clientSQLExecute(1, "create table EMP.REPLICATETABLE (ID int not null, "
        + " SECONDID int not null, THIRD int not null, "
        + "FOURTH int not null, FIFTH int not null,"
        + " primary key (SECONDID)) REPLICATE");
    clientSQLExecute(1,
        "create unique index third_index on EMP.REPLICATETABLE (THIRD)");
    clientSQLExecute(1,
        "create index fourth_index on EMP.REPLICATETABLE (FOURTH)");
    clientSQLExecute(1,
        "create unique index fifth_index on EMP.REPLICATETABLE (fifth)");

    clientSQLExecute(1, "INSERT INTO EMP.REPLICATETABLE values(1,2,3,4, 10)");
    clientSQLExecute(1, "INSERT INTO EMP.REPLICATETABLE values(4,5,6,4, 11)");
    clientSQLExecute(1, "INSERT INTO EMP.REPLICATETABLE values(7,8,9,4, 12)");

    try {
      addExpectedException(new int[] { 1 }, new int[] { 1, 2 }, new Object[] {
          EntryExistsException.class, FunctionExecutionException.class });
      checkKeyViolation(1, "INSERT INTO EMP.REPLICATETABLE values(1,3,3,4,13)");
      checkKeyViolation(-1, "INSERT INTO EMP.REPLICATETABLE values(1,3,3,4,13)");
      checkKeyViolation(-2, "INSERT INTO EMP.REPLICATETABLE values(1,3,3,4,13)");
    } finally {
      removeExpectedException(new int[] { 1 }, new int[] { 1, 2 },
          new Object[] { EntryExistsException.class,
              FunctionExecutionException.class, java.sql.SQLException.class });
    }
    sqlExecuteVerify(null, new int[] { 1, 2 },
        "VALUES SYSCS_UTIL.CHECK_TABLE('EMP','REPLICATETABLE')", null, "1");

    ResultSet rs = null;
    PreparedStatement ps = null;
    ps = TestUtil.jdbcConn.prepareStatement("Select * from EMP.REPLICATETABLE "
        + "where id=1 and secondid=3");
    rs = ps.executeQuery();
    JDBC.assertDrainResults(rs, 0);

    String[][] expectedRows = { { "1", "2", "3", "4", "10" } };
    ps = TestUtil.jdbcConn
        .prepareStatement("Select * from EMP.REPLICATETABLE where THIRD=3");
    rs = ps.executeQuery();
    JDBC.assertFullResultSet(rs, expectedRows);
    rs.close();

    sqlExecuteVerify(new int[] { 1 }, new int[] { 1, 2 },
        "Select * from EMP.REPLICATETABLE where FOURTH=4", TestUtil.getResourcesDir()
            + "/lib/checkTableIndicesConsistency.xml", "rs1");

    addExpectedException(new int[] { 1 }, new int[] { 1, 2 }, new Object[] {
        FunctionExecutionException.class, EntryExistsException.class });

    checkKeyViolation(1, "INSERT INTO EMP.REPLICATETABLE values(1,2,5,4,13)");
    checkKeyViolation(-1, "INSERT INTO EMP.REPLICATETABLE values(1,2,5,4,13)");
    checkKeyViolation(-2, "INSERT INTO EMP.REPLICATETABLE values(1,2,5,4,13)");

    removeExpectedException(new int[] { 1 }, new int[] { 1, 2 }, new Object[] {
        FunctionExecutionException.class, EntryExistsException.class });

    sqlExecuteVerify(null, new int[] { 1, 2 },
        "VALUES SYSCS_UTIL.CHECK_TABLE('EMP','REPLICATETABLE')", null,
        "1");

    ps = TestUtil.jdbcConn.prepareStatement("Select * from EMP.REPLICATETABLE "
        + "where id=1 and secondid=2");
    rs = ps.executeQuery();
    JDBC.assertFullResultSet(rs, expectedRows);
    rs.close();

    ps = TestUtil.jdbcConn
        .prepareStatement("Select * from EMP.REPLICATETABLE where THIRD=5");
    rs = ps.executeQuery();
    JDBC.assertDrainResults(rs, 0);

    sqlExecuteVerify(new int[] { 1 }, new int[] { 1, 2 },
        "Select * from EMP.REPLICATETABLE where FOURTH=4", TestUtil.getResourcesDir()
            + "/lib/checkTableIndicesConsistency.xml", "rs1");

    try {
      addExpectedException(new int[] { 1 }, new int[] { 1, 2 }, new Object[] {
          FunctionExecutionException.class, EntryExistsException.class });
      checkKeyViolation(1, "UPDATE EMP.REPLICATETABLE SET third=6, fifth=20 "
          + "where id=1 and secondid=2");
      checkKeyViolation(-1, "UPDATE EMP.REPLICATETABLE SET third=6, fifth=20 "
          + "where id=1 and secondid=2");
      checkKeyViolation(-2, "UPDATE EMP.REPLICATETABLE SET third=6, fifth=20 "
          + "where id=1 and secondid=2");
    } finally {
      removeExpectedException(new int[] { 1 }, new int[] { 1, 2 },
          new Object[] { FunctionExecutionException.class,
              EntryExistsException.class });
    }

    sqlExecuteVerify(null, new int[] { 1, 2 },
        "VALUES SYSCS_UTIL.CHECK_TABLE('EMP','REPLICATETABLE')", null,
        "1");
    ps = TestUtil.jdbcConn.prepareStatement("Select * from EMP.REPLICATETABLE "
        + "where id=1 and secondid=2");
    rs = ps.executeQuery();
    JDBC.assertFullResultSet(rs, expectedRows);
    rs.close();

    ps = TestUtil.jdbcConn
        .prepareStatement("Select * from EMP.REPLICATETABLE where THIRD=5");
    rs = ps.executeQuery();
    JDBC.assertDrainResults(rs, 0);

    sqlExecuteVerify(new int[] { 1 }, new int[] { 1, 2 },
        "Select * from EMP.REPLICATETABLE where FOURTH=4", TestUtil.getResourcesDir()
            + "/lib/checkTableIndicesConsistency.xml", "rs1");

    clientSQLExecute(1, "DELETE FROM EMP.REPLICATETABLE WHERE id=1");
    sqlExecuteVerify(null, new int[] { 1, 2 },
        "VALUES SYSCS_UTIL.CHECK_TABLE('EMP','REPLICATETABLE')", null,
        "1");
    ps = TestUtil.jdbcConn.prepareStatement("Select * from EMP.REPLICATETABLE "
        + "where id=1 and secondid=2");
    rs = ps.executeQuery();
    JDBC.assertDrainResults(rs, 0);

    // drop the table
    clientSQLExecute(1, "drop table EMP.REPLICATETABLE");
  }
}
