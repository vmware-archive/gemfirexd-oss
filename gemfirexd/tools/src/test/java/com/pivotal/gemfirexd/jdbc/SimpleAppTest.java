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

//
//  SimpleAppTest.java
//  gemfire
//
//  Created by Eric Zoerner on 3/17/08.

package com.pivotal.gemfirexd.jdbc;

import java.util.*;
import java.sql.*;
import junit.textui.TestRunner;
import junit.framework.TestSuite;

public class SimpleAppTest extends JdbcTestBase {

  public static void main(String[] args) {
    TestRunner.run(new TestSuite(SimpleAppTest.class));
  }
  
  public SimpleAppTest(String name) {
    super(name);
  }
  
  public void testSimpleApp() throws SQLException {
    
    // This ArrayList usage may cause a warning when compiling this class
    // with a compiler for J2SE 5.0 or newer. We are not using generics
    // because we want the source to support J2SE 1.4.2 environments.
    ArrayList statements = new ArrayList(); // list of Statements, PreparedStatements
    PreparedStatement psInsert = null;
    PreparedStatement psUpdate = null;
    Statement s = null;
    ResultSet rs = null;
    Connection conn = null;
    try {
      conn = getConnection();
                  
      // Creating a statement object that we can use for running various
      // SQL statements commands against the database.
      s = conn.createStatement();
      statements.add(s);
      
      // We create a table...
      s.execute("create table location(num int, addr varchar(40))");
      
      // and add a few rows...
      
      // It is recommended to use PreparedStatements when you are
      // repeating execution of an SQL statement. PreparedStatements also
      // allows you to parameterize variables. By using PreparedStatements
      // you may increase performance (because the Derby engine does not
      // have to recompile the SQL statement each time it is executed) and
      // improve security (because of Java type checking).
      //
      // parameter 1 is num (int), parameter 2 is addr (varchar)
      psInsert = conn.prepareStatement("insert into location values (?, ?)");
      statements.add(psInsert);
      
      // insert 1956 Webster
      psInsert.setInt(1, 1956);
      psInsert.setString(2, "Webster St.");
      int n = psInsert.executeUpdate();
      assertEquals("Insert not working ", 1, n);

      //insert 1910 Union
      psInsert.setInt(1, 1910);
      psInsert.setString(2, "Union St.");
      n = psInsert.executeUpdate();
      assertEquals("Insert not working ", 1, n);
      // Let's update some rows as well...
      
      // parameter 1 and 3 are num (int), parameter 2 is addr (varchar)
      psUpdate = conn.prepareStatement("update location set num=?, addr=? where num=?");
      statements.add(psUpdate);
      
      // update 1956 Webster to 180 Grand
      psUpdate.setInt(1, 180);
      psUpdate.setString(2, "Grand Ave.");
      psUpdate.setInt(3, 1956);
      n = psUpdate.executeUpdate();
      
      assertEquals("Update not working  ", 1, n);
      
      // update 180 Grand to 300 Lakeshore
      psUpdate.setInt(1, 300);
      psUpdate.setString(2, "Lakeshore Ave.");
      psUpdate.setInt(3, 180);
      n = psUpdate.executeUpdate();      
      assertEquals("Update not working ", 1, n);
      
      //
      // We select the rows and verify the results.
      //
      rs = s.executeQuery("SELECT num, addr FROM location ORDER BY num");
      
      // we expect the first returned column to be an integer (num),
      // and second to be a String (addr). Rows are sorted by street
      // number (num).
      //
      // Normally, it is best to use a pattern of
      //  while(rs.next()) {
      //    // do something with the result set
      //  }
      // to process all returned rows, but we are only expecting two rows
      // this time, and want the verification code to be easy to
      // comprehend, so we use a different pattern.
      //
      
      int number; // street number retreived from the database
      if (!rs.next()) {
        fail("No rows in ResultSet");
      }
      
      if ((number = rs.getInt(1)) != 300) {
        fail("Wrong row returned, expected num=300, got " + number);
      }
      
      if (!rs.next()) {
        fail("Too few rows");
      }
      
      if ((number = rs.getInt(1)) != 1910) {
        fail("Wrong row returned, expected num=1910, got " + number);
      }
      
      if (rs.next()) {
        fail("Too many rows");
      }
            
      // delete the table
      s.execute("drop table location");
      
      //
      // We commit the transaction. Any changes will be persisted to
      // the database now.
      //
      conn.commit();
      
    }
    finally {
      // release all open resources to avoid unnecessary memory usage
      
      // ResultSet
      if (rs != null) {
        rs.close();
        rs = null;
      }
      
      // Statements and PreparedStatements
      int i = 0;
      while (!statements.isEmpty()) {
      // PreparedStatement extend Statement
      Statement st = (Statement)statements.remove(i);
        if (st != null) {
          st.close();
          st = null;
        }
      }
      
      //Connection
      if (conn != null) {
        conn.close();
        conn = null;
      }
    }
  }
}
