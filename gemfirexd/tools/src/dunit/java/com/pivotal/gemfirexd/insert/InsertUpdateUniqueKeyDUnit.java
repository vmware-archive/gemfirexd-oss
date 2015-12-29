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
package com.pivotal.gemfirexd.insert;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.pivotal.gemfirexd.DistributedSQLTestBase;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.engine.distributed.FunctionExecutionException;

/**
 * Test class for Unique constraint checks with inserts and updates.
 * 
 * @author rdubey
 * 
 */
public class InsertUpdateUniqueKeyDUnit extends DistributedSQLTestBase {
  
  public InsertUpdateUniqueKeyDUnit(String name) {
    super(name);
  }
  
  /**
   * Test for 40040(In replicated tables, insert operation missed a 
   * unique key constraint violation) with simple insert statements.
   * @throws Exception
   */
  public void testFor40040() throws Exception {
    startVMs(1, 1);
    clientSQLExecute(1, "create table trade.securities (sec_id int not null, " +
        "symbol varchar(10) not null, price decimal (30, 20), " +
        "exchange varchar(10) not null, tid int, " +
        "constraint sec_pk primary key (sec_id), " +
        "constraint sec_uq unique (symbol, exchange), " +
        "constraint exc_ch check (exchange in ('nasdaq', 'nye', 'amex'," +
        " 'lse', 'fse', 'hkse', 'tse'))) replicate");
    clientSQLExecute(1, "insert into trade.securities values (1, 'IBM', 3, 'fse' , 1)");

    addExpectedException(new int[] { 1 }, new int[] { 1 }, new Object[] {
        com.gemstone.gemfire.cache.EntryExistsException.class,
        FunctionExecutionException.class, java.sql.SQLException.class,
        "java.sql.SQLIntegrityConstraintViolationException" });

    try {
      for (int i = 0; i < 20; i++) {
        checkKeyViolation(1,
            "insert into trade.securities values (2, 'IBM', 3, 'fse' , 1)");
        checkKeyViolation(-1,
            "insert into trade.securities values (2, 'IBM', 3, 'fse' , 1)");
      }
    } finally {
      removeExpectedException(new int[] { 1 }, new int[] { 1 }, new Object[] {
          com.gemstone.gemfire.cache.EntryExistsException.class,
          FunctionExecutionException.class, java.sql.SQLException.class,
          "java.sql.SQLIntegrityConstraintViolationException" });
    }
  }

  /**
   * Test for 40040(In replicated tables, insert operation missed a 
   * unique key constraint violation) using prepared insert statements.
   * @throws Exception
   */
  public void testFor40040UsingPreparedStatements() throws Exception {
    startVMs(1, 1);
    clientSQLExecute(1, "create table trade.securities (sec_id int not null, " +
        "symbol varchar(10) not null, price decimal (30, 20), " +
        "exchange varchar(10) not null, tid int, " +
        "constraint sec_pk primary key (sec_id), " +
        "constraint sec_uq unique (symbol, exchange), " +
        "constraint exc_ch check (exchange in ('nasdaq', 'nye', 'amex'," +
        " 'lse', 'fse', 'hkse', 'tse'))) replicate");
    Connection con = TestUtil.jdbcConn;
    PreparedStatement ps = con.prepareStatement("insert into trade.securities values (?, ?, ?, ? , ?)");
    ps.setInt(1, 1);
    ps.setString(2, "IBM");
    ps.setDouble(3,3);
    ps.setString(4, "fse");
    ps.setInt(5, 1);
    int rows = ps.executeUpdate();
    assertEquals("Insert should update one row ",1,rows);

    addExpectedException(new int[] { 1 }, new int[] { 1 }, new Object[] {
        com.gemstone.gemfire.cache.EntryExistsException.class,
        FunctionExecutionException.class, java.sql.SQLException.class,
        "java.sql.SQLIntegrityConstraintViolationException" });

    try {
    for (int i = 0; i < 1; i++) {
      try {
        ps.setInt(1, 2);
        ps.setString(2, "IBM");
        ps.setDouble(3,3);
        ps.setString(4, "fse");
        ps.setInt(5, 1);
        ps.executeUpdate();
        fail("Insert should not be allowed.");
      }
      catch (SQLException sqle) {
        assertEquals(sqle.toString(), "23505", sqle.getSQLState());
      }
    }
    }
    finally {
      removeExpectedException(new int[] { 1 }, new int[] { 1 }, new Object[] {
          com.gemstone.gemfire.cache.EntryExistsException.class,
          FunctionExecutionException.class, java.sql.SQLException.class,
          "java.sql.SQLIntegrityConstraintViolationException" });
    }
  }
}
