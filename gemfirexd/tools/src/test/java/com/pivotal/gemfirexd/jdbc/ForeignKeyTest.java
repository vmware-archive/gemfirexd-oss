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

import java.sql.*;

import com.gemstone.gemfire.cache.execute.FunctionException;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverAdapter;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverHolder;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedPreparedStatement;

import junit.framework.TestSuite;
import junit.textui.TestRunner;

/**
 * junit tests for foreign key constraints
 *
 * @author Eric Zoerner , Rahul Dubey
 */

public class ForeignKeyTest extends JdbcTestBase {
  
  public ForeignKeyTest(String name) {
    super(name);
  }
  
  public static void main(String[] args) {
    TestRunner.run(new TestSuite(ForeignKeyTest.class));
  }
  
  public void testFkInsert() throws SQLException {
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    
    int rowCount = 0;
    s.executeUpdate("create table t2(id int not null, primary key(id))" + getSuffix());
    s.executeUpdate("create table t1(id int not null, fkId int not null, " +
                    "primary key(id), foreign key (fkId) references t2(id))"+ getSuffix());
    // fk exists
    rowCount += s.executeUpdate("insert into t2 values(1)");
    assertEquals(1, rowCount);
    rowCount += s.executeUpdate("insert into t1 values(1, 1)");
    assertEquals(2, rowCount);

    // fk doesn't exist, should fail
    try {
      s.executeUpdate("insert into t1 values(2, 2)");
      fail("should have thrown an SQLException");
    }
    catch (SQLException ex) {
      if (!"23503".equals(ex.getSQLState())) {
        throw ex;
      }
    }
    conn.commit();
    s.close();
  }

  /**
   * Test that derby doesn't allow foreign key constraint on non unique/primary
   * key.
   * @throws Exception on failure.
   */
  //Neeraj: uncomment after bug #42503 is fixed
  public void _testFkNotOnPK() throws Exception {
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    
    s.executeUpdate("create table t2(id int not null)"+ getSuffix());
    try {
      s.executeUpdate("create table t1(id int not null, fkId int not null, " +
                    "primary key(id), foreign key (fkId) references t2(id))"+ getSuffix());
      fail("Create table should fail because table t2 has no primary key or " +
      		"unique which table t1 is pointing in foreign key cosnstraint");
    }
    catch (SQLException ex) {
      if (!"X0Y44".equals(ex.getSQLState())) {
        throw ex;
      }
    }
    conn.commit();
    s.close();
  }

  /**
   * Test foreign key on unique column in foreign table.
   * @throws SQLException on failure.
   */
  //Neeraj: uncomment after bug #42503 is fixed
  public void _testFkOnUniqueColumn() throws SQLException {
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    
    s.executeUpdate("create table t2(id int not null unique)"+ getSuffix());
    s.executeUpdate("create table t1(id int not null, fkId int not null, " +
                    "primary key(id), foreign key (fkId) references t2(id))"+ getSuffix());
    conn.commit();
    s.close();
  }

  /**
   * Test foreign key constraint on replicated region.
   * @throws Exception
   */
  public void testBug40025() throws Exception {
    Connection conn = null;
    Statement s = null;
    // Start one client and one server

    String updateQuery = "update Child set sector_id2 = ? where id2 = ?";

    // Create the table and insert a row
    conn = getConnection();
    s = conn.createStatement();
    s.execute("create table INSTRUMENTS (id1 int primary key, "
        + "sector_id1 int, subsector_id1 int)  replicate"+ getSuffix());
    s.execute("create table Child ( id2 int primary key, "
        + "sector_id2 int, subsector_id2 int, foreign key (sector_id2) "
        + "references instruments (id1) ) replicate"+ getSuffix());

    try {
      s.execute("insert into instruments values (1,1,1)");
      s.execute("insert into Child values (1,1,1)");
      TestUtil.setupConnection();
      EmbedPreparedStatement es = (EmbedPreparedStatement)TestUtil.jdbcConn
          .prepareStatement(updateQuery);
      es.setInt(1, 2);
      es.setInt(2, 1);
      try {
        es.executeUpdate();
        fail("Update should not have occured as foreign key violation would occur");
      } catch (SQLException sqle) {
        // Log.getLogWriter().info("Expected exception="+sqle.getMessage());
        assertEquals(sqle.toString(), "23503", sqle.getSQLState());
      }
    } finally {
      GemFireXDQueryObserverHolder
          .putInstance(new GemFireXDQueryObserverAdapter());
      conn = getConnection();
      s = conn.createStatement();
      s.execute("Drop table Child ");
      this.waitTillAllClear();
      s.execute("Drop table INSTRUMENTS ");
      this.waitTillAllClear();
    } 
  }

  /**
   * Test foreign key constraint with null values for FK field.
   * 
   * @throws Exception
   */
  public void testBug41168() throws Exception {

    // Create the table with self-reference FKs
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s.execute("create table BinaryTree (id int primary key, "
        + "leftId int, rightId int, depth int not null,"
        + " foreign key (leftId) references BinaryTree(id),"
        + " foreign key (rightId) references BinaryTree(id))"+ getSuffix());

    // some inserts with null values
    s.execute("insert into BinaryTree values (3, null, null, 2)");
    s.execute("insert into BinaryTree values (2, null, null, 1)");
    s.execute("insert into BinaryTree values (1, 3, null, 1)");
    s.execute("insert into BinaryTree values (0, 1, 2, 0)");
    this.doOffHeapValidations();

    // test FK violation for the self FK
    addExpectedException(FunctionException.class);
    try {
      s.execute("insert into BinaryTree values (4, 5, null, 2)");
      fail("insert should throw foreign key violation exception");
    } catch (SQLException sqle) {
      assertEquals(sqle.toString(), "23503", sqle.getSQLState());
    }
    try {
      s.execute("insert into BinaryTree values (5, null, 4, 2)");
      fail("insert should throw foreign key violation exception");
    } catch (SQLException sqle) {
      assertEquals(sqle.toString(), "23503", sqle.getSQLState());
    }
    try {
      s.execute("update BinaryTree set leftId=4 where id=2");
      fail("update should throw foreign key violation exception");
    } catch (SQLException sqle) {
      assertEquals(sqle.toString(), "23503", sqle.getSQLState());
    }
    try {
      s.execute("update BinaryTree set rightId=5 where id=2");
      fail("update should throw foreign key violation exception");
    } catch (SQLException sqle) {
      assertEquals(sqle.toString(), "23503", sqle.getSQLState());
    }
    this.doOffHeapValidations();
    // now update the tree with two proper nodes
    s.execute("insert into BinaryTree values (4, null, null, 2)");
    s.execute("insert into BinaryTree values (5, null, null, 2)");
    s.execute("update BinaryTree set leftId=4 where id=2");
    s.execute("update BinaryTree set rightId=5 where id=2");
    this.doOffHeapValidations();
    // run a self-join query and verify the results
    ResultSet rs = null;
    rs = s.executeQuery("select t1.id from BinaryTree t1, "
        + "BinaryTree t2 where t1.leftId = t2.id and t2.id = 3");
    assertTrue("expected one result", rs.next());
    assertEquals(1, rs.getInt(1));
    assertFalse("expected one result", rs.next());
    this.doOffHeapValidations();
    // finally check that delete works for null FK column rows as well and
    // verify the results again
    try {
      s.execute("delete from BinaryTree where id=4");
      fail("delete should throw foreign key violation exception");
    } catch (SQLException sqle) {
      assertEquals(sqle.toString(), "23503", sqle.getSQLState());
    }
    this.doOffHeapValidations();
    try {
      s.execute("delete from BinaryTree where id=5");
      fail("delete should throw foreign key violation exception");
    } catch (SQLException sqle) {
      assertEquals(sqle.toString(), "23503", sqle.getSQLState());
    }
    this.doOffHeapValidations();
    try {
      s.execute("update BinaryTree set depth=null where id=2");
    } catch (SQLException sqle) {
      assertEquals(sqle.toString(), "23502", sqle.getSQLState());
    }
    this.doOffHeapValidations();
    s.execute("update BinaryTree set leftId=null where id=2");
    s.execute("update BinaryTree set rightId=null where id=2");
    s.execute("delete from BinaryTree where id=5");
    s.execute("delete from BinaryTree where id=4");
    this.doOffHeapValidations();
    rs = s.executeQuery("select t1.id from BinaryTree t1, "
        + "BinaryTree t2 where t1.leftId = t2.id and t2.id = 4");
    assertFalse("expected no result", rs.next());
    rs = s.executeQuery("select t1.id from BinaryTree t1, "
        + "BinaryTree t2 where t1.leftId = t2.id and t2.id = 5");
    assertFalse("expected no result", rs.next());
    this.doOffHeapValidations();
  }
  
  protected String getSuffix() {
    return " ";
  }
  

  public void waitTillAllClear() {}
}
