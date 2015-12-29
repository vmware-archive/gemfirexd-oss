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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.TestSuite;
import junit.textui.TestRunner;
import org.apache.derbyTesting.junit.JDBC;

/**
 * junit tests for default values
 * 
 * @author Eric Zoerner
 *
 */
public class DefaultValueTest extends JdbcTestBase {
  
  public DefaultValueTest(String name) {
    super(name);
  }
  
  public static void main(String[] args) {
    TestRunner.run(new TestSuite(DefaultValueTest.class));
  }
  
  /**
   * create an local index after the table is populated with data.
   * 
   * @throws SQLException
   */
  
  public void testDefaultIntAndChar() throws SQLException {
    
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    
    s.execute("create table t1 (c1 int primary key, c2 int DEFAULT 42, c3 char(4) DEFAULT 'cara')");
    s.execute("insert into t1 (c1) values (10)");
    s.execute("insert into t1 (c1) values (20)");
    
    Object[][] expectedRows = {{ 10, 42, "cara" }, { 20, 42, "cara" }, };
    
    ResultSet rs = s.executeQuery("select * from t1");
    JDBC.assertUnorderedResultSet(rs, expectedRows, false);
    conn.close();
  }
  
  
  /**
   * create an local index after the table is populated with data.
   * 
   * @throws SQLException
   */
  
  public void testDefaultWithPreparedStatement() throws SQLException {
    
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    
    s.execute("create table t1 (c1 int primary key, c2 int DEFAULT 42, c3 char(4) DEFAULT 'cara')");
    
    PreparedStatement ps = conn.prepareStatement("insert into t1 (c1) values (?)");
    ps.setInt(1, 10);
    ps.executeUpdate();
    ps.setInt(1, 20);
    ps.executeUpdate();
    
    Object[][] expectedRows = {{ 10, 42, "cara" }, { 20, 42, "cara" }, };
    
    ps.close();
    ps = conn.prepareStatement("select * from t1");
    ResultSet rs = ps.executeQuery();
    JDBC.assertUnorderedResultSet(rs, expectedRows, false);
    conn.close();
  }
  
  
}
