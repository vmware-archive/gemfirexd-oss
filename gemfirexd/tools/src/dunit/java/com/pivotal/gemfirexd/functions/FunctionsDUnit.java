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
package com.pivotal.gemfirexd.functions;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import com.pivotal.gemfirexd.DistributedSQLTestBase;
import com.pivotal.gemfirexd.TestUtil;

public class FunctionsDUnit extends DistributedSQLTestBase {
  public FunctionsDUnit(String name) {
    super(name);
  }

  public void testFunctionCoalesceNvl_defect46017() throws Exception {
    // Start one client and two servers
    startVMs(1, 2);

    {
      Connection conn = TestUtil.getConnection();
      Statement s = conn.createStatement();
      s.execute("create table tAggr (i int, j int) partition by column(j)");
      s.execute("create table tCoAggr (i int, j int) partition by column(j) " +
      		"colocate with (tAggr)");
      s.execute("create table tRepAggr (i int, j int) replicate");
      PreparedStatement ps = conn
          .prepareStatement("insert into tAggr(i,j) values (?,?)");
      PreparedStatement psCo = conn
          .prepareStatement("insert into tCoAggr(i,j) values (?,?)");
      PreparedStatement psRep = conn
          .prepareStatement("insert into tRepAggr(i,j) values (?,?)");
      for (int i = 0; i < 3; i++) {
        ps.setInt(1, i);
        ps.setInt(2, i);
        ps.executeUpdate();
        
        psCo.setInt(1, i);
        psCo.setInt(2, i);
        psCo.executeUpdate();
        
        psRep.setInt(1, i);
        psRep.setInt(2, i);
        psRep.executeUpdate();
      }
      
      // lets insert a null row
      ps.setNull(1, Types.INTEGER);
      ps.setInt(2, 3);
      ps.executeUpdate();
      
      psRep.setNull(1, Types.INTEGER);
      psRep.setInt(2, 3);
      psRep.executeUpdate();
    }

    {// constant with aggregate
      Connection conn = TestUtil.getConnection();
      Statement st = conn.createStatement();

      {
        ResultSet r = st.executeQuery("select coalesce(55, max(i)) from tRepAggr");
        int count = 0;
        while (r.next()) {
          assertEquals(55, r.getInt(1));
          count++;
        }
        assertEquals(1, count);
      }

      {
        ResultSet r = st.executeQuery("select coalesce(55, max(i)) from tAggr");
        int count = 0;
        while (r.next()) {
          assertEquals(55, r.getInt(1));
          count++;
        }
        assertEquals(1, count);
      }
    }
    
    { // avg
      Connection conn = TestUtil.getConnection();
      Statement st = conn.createStatement();

      {
        ResultSet r = st.executeQuery("select coalesce(avg(i), 55, max(i)) from tRepAggr");
        int count = 0;
        while (r.next()) {
          assertEquals(1, r.getInt(1));
          count++;
        }
        assertEquals(1, count);
      }

      {
        ResultSet r = st.executeQuery("select coalesce(avg(i), 55, max(i)) from tAggr");
        int count = 0;
        while (r.next()) {
          assertEquals(1, r.getInt(1));
          count++;
        }
        assertEquals(1, count);
      }
    }
    
    { // where clause
      Connection conn = TestUtil.getConnection();
      Statement st = conn.createStatement();

      {
        ResultSet r = st.executeQuery("select count(j) from tRepAggr where coalesce(i, j) > 2");
        int count = 0;
        while (r.next()) {
          assertEquals(1, r.getInt(1));
          count++;
        }
        assertEquals(1, count);
      }

      {
        ResultSet r = st.executeQuery("select count(j) from tAggr where coalesce(i, j) > 2");
        int count = 0;
        while (r.next()) {
          assertEquals(1, r.getInt(1));
          count++;
        }
        assertEquals(1, count);
      }
    }
    
    { // having clause, group by i,j
      Connection conn = TestUtil.getConnection();
      Statement st = conn.createStatement();

      {
        ResultSet r = st.executeQuery("select count(j) from tRepAggr group by i, j having coalesce(max(j), 0) > 2");
        int count = 0;
        while (r.next()) {
          assertEquals(1, r.getInt(1));
          count++;
        }
        assertEquals(1, count);
      }

      {
        ResultSet r = st.executeQuery("select count(j) from tAggr group by i, j having coalesce(max(j), 0) > 2");
        int count = 0;
        while (r.next()) {
          assertEquals(1, r.getInt(1));
          count++;
        }
        assertEquals(1, count);
      }
    }
    
    { // having clause, group by i
      Connection conn = TestUtil.getConnection();
      Statement st = conn.createStatement();

      {
        ResultSet r = st.executeQuery("select count(j) from tRepAggr group by i having coalesce(max(j), 0) > 2");
        int count = 0;
        while (r.next()) {
          assertEquals(1, r.getInt(1));
          count++;
        }
        assertEquals(1, count);
      }

      {
        ResultSet r = st.executeQuery("select count(j) from tAggr group by i having coalesce(max(j), 0) > 2");
        int count = 0;
        while (r.next()) {
          assertEquals(1, r.getInt(1));
          count++;
        }
        assertEquals(1, count);
      }
    }
    
    { // having clause, group by i, coalesce in select
      Connection conn = TestUtil.getConnection();
      Statement st = conn.createStatement();

      {
        ResultSet r = st.executeQuery("select coalesce(count(j), 0) from tRepAggr group by i having coalesce(max(j), 0) > 2");
        int count = 0;
        while (r.next()) {
          assertEquals(1, r.getInt(1));
          count++;
        }
        assertEquals(1, count);
      }

      {
        ResultSet r = st.executeQuery("select coalesce(count(j), 0) from tAggr group by i having coalesce(max(j), 0) > 2");
        int count = 0;
        while (r.next()) {
          assertEquals(1, r.getInt(1));
          count++;
        }
        assertEquals(1, count);
      }
    }
    
    // two tables
    {
      Connection conn = TestUtil.getConnection();
      Statement st = conn.createStatement();
      ResultSet r = st
          .executeQuery("select coalesce(max(A.i), count(*), 55) from tAggr A "
              + "inner join tCoAggr B on A.j = B.j");
      int count = 0;
      while (r.next()) {
        assertEquals(2, r.getInt(1));
        count ++;
      }
      assertEquals(1, count);
    } 

    // two tables - group by, having
    {
      Connection conn = TestUtil.getConnection();
      Statement st = conn.createStatement();
      ResultSet r = st
          .executeQuery("select count(A.j) from tAggr A inner join tCoAggr B on A.j = B.j group by A.i having coalesce(max(A.j), 0) = 2");
      int count = 0;
      while (r.next()) {
        assertEquals(1, r.getInt(1));
        count ++;
      }
      assertEquals(1, count);
    } 
    
    { // truncate and null
      Connection conn = TestUtil.getConnection();
      Statement s = conn.createStatement();
      s.executeUpdate("delete from tAggr");
      s.executeUpdate("delete from tCoAggr");
      s.executeUpdate("delete from tRepAggr");

      {
        PreparedStatement ps = conn
            .prepareStatement("insert into tAggr(i,j) values (?,?)");
        ps.setNull(1, Types.INTEGER);
        ps.setNull(2, Types.INTEGER);
        ps.executeUpdate();
      }
      
      {
        PreparedStatement psCo = conn
            .prepareStatement("insert into tCoAggr(i,j) values (?,?)");
        psCo.setNull(1, Types.INTEGER);
        psCo.setNull(2, Types.INTEGER);
        psCo.executeUpdate();
      }

      {
        PreparedStatement psRep = conn
            .prepareStatement("insert into tRepAggr(i,j) values (?,?)");
        psRep.setNull(1, Types.INTEGER);
        psRep.setNull(2, Types.INTEGER);
        psRep.executeUpdate();
      }
    }
    
    {// constant
      Connection conn = TestUtil.getConnection();
      Statement st = conn.createStatement();

      {
        ResultSet r = st.executeQuery("select coalesce(55, i) from tRepAggr");
        int count = 0;
        while (r.next()) {
          assertEquals(55, r.getInt(1));
          count++;
        }
        assertEquals(1, count);
      }

      {
        ResultSet r = st.executeQuery("select coalesce(55, i) from tAggr");
        int count = 0;
        while (r.next()) {
          assertEquals(55, r.getInt(1));
          count++;
        }
        assertEquals(1, count);
      }
    }
    
    {// constant - 2
      Connection conn = TestUtil.getConnection();
      Statement st = conn.createStatement();

      {
        ResultSet r = st.executeQuery("select coalesce(i, 55, j) from tRepAggr");
        int count = 0;
        while (r.next()) {
          assertEquals(55, r.getInt(1));
          count++;
        }
        assertEquals(1, count);
      }

      {
        ResultSet r = st.executeQuery("select coalesce(i, 55, j) from tAggr");
        int count = 0;
        while (r.next()) {
          assertEquals(55, r.getInt(1));
          count++;
        }
        assertEquals(1, count);
      }
    }

    { // max is null
      Connection conn = TestUtil.getConnection();
      Statement st = conn.createStatement();

      {
        ResultSet r = st.executeQuery("select coalesce(max(i), count(i), 55) from tRepAggr");
        int count = 0;
        while (r.next()) {
          assertEquals(0, r.getInt(1));
          count++;
        }
        assertEquals(1, count);
      }

      {
        ResultSet r = st.executeQuery("select coalesce(max(i), count(i), 55) from tAggr");
        int count = 0;
        while (r.next()) {
          assertEquals(0, r.getInt(1));
          count++;
        }
        assertEquals(1, count);
      }
    }
    
    { // max is null, coalesce in select
      Connection conn = TestUtil.getConnection();
      Statement st = conn.createStatement();

      {
        ResultSet r = st.executeQuery("select coalesce(max(j), count(j), 55) from tRepAggr group by i having coalesce(max(j), 2) = 2");
        int count = 0;
        while (r.next()) {
          assertEquals(0, r.getInt(1));
          count++;
        }
        assertEquals(1, count);
      }

      {
        ResultSet r = st.executeQuery("select coalesce(max(j), count(j), 55) from tAggr group by i having coalesce(max(j), 2) = 2");
        int count = 0;
        while (r.next()) {
          assertEquals(0, r.getInt(1));
          count++;
        }
        assertEquals(1, count);
      }
    }
    
    // two tables - nulls
    {
      Connection conn = TestUtil.getConnection();
      Statement st = conn.createStatement();
      ResultSet r = st
          .executeQuery("select coalesce(max(A.i), count(*), 55) from tAggr A "
              + "inner join tCoAggr B on A.j = B.j");
      int count = 0;
      while (r.next()) {
        assertEquals(0, r.getInt(1));
        count ++;
      }
      assertEquals(1, count);
    } 
  }
  
  public void testFunctionNvl_defect45979() throws Exception {
    // Start one client and two servers
    startVMs(1, 2);
    Connection conn = TestUtil.getConnection();
    Statement st = conn.createStatement();
    st.execute("CREATE TABLE D3764A (A0 INTEGER, A1 INTEGER, A2 INTEGER) replicate");
    st.execute("CREATE TABLE D3764B (A0 INTEGER, A1 INTEGER, A2 INTEGER) partition by column(a1)");
    st.execute("INSERT INTO D3764A (A0, A1) VALUES (1, 1), (1, 1)");
    st.execute("INSERT INTO D3764B (A0, A1) VALUES (1, 1), (1, 1)");
    
    // check for coalesce and nvl
    final int expected = 3;
    final int expectedCount = 1;
    {//D3764A
      ResultSet r = st
          .executeQuery("select SUM(a0) + nvl(a2, 1) from D3764A Group By a2");
      int r1 = 0;
      while (r.next()) {
        assertEquals(expected, r.getInt(1));
        r1++;
      }
      assertEquals(expectedCount, r1);
    }
    {//D3764B
      ResultSet r = st
          .executeQuery("select SUM(a0) + nvl(a2, 1) from D3764B Group By a2");
      int r1 = 0;
      while (r.next()) {
        assertEquals(expected, r.getInt(1));
        r1++;
      }
      assertEquals(expectedCount, r1);
    }
  }
  
  public void testFunctionCreate() throws Exception {
    // Start one client and two servers
    startVMs(1, 2);

    // Create a table
    clientSQLExecute(1, "create table ORDERS (ID int primary key, VOL int "
        + "NOT NULL unique, SECURITY_ID varchar(10)) partition by "
        + "Primary Key redundancy 1");

   // partition by Primary Key redundancy 1 
   for(int i=0; i<100; i++) {
     String statement="insert into ORDERS values ("+i+","+i+",'char"+i+"')";
     clientSQLExecute(1, statement);
   }
   
   String createFunction="CREATE FUNCTION times " +
   		"(value INTEGER) " +
   		"RETURNS INTEGER "+
   		"LANGUAGE JAVA " +
   		"EXTERNAL NAME 'com.pivotal.gemfirexd.functions.TestFunctions.times' " +
   		"PARAMETER STYLE JAVA " +
   		"NO SQL " +
   		"RETURNS NULL ON NULL INPUT ";
   
     
   clientSQLExecute(1, createFunction);
   sqlExecuteVerify(new int[] {1}, null,
       "SELECT times(ID) FROM ORDERS WHERE ID>10 and ID<15",
            TestUtil.getResourcesDir()
           + "/lib/checkFunctions.xml", "q_1"); 
   
   
   String createFunction1="CREATE FUNCTION subsetRows " +
   "(tableName VARCHAR(20), low INTEGER, high INTEGER) " +
   "RETURNS Table (ID INT, SECURITY_ID VARCHAR(10)) "+
   "LANGUAGE JAVA " +
   "EXTERNAL NAME 'com.pivotal.gemfirexd.functions.TestFunctions.subset' " +
   "PARAMETER STYLE DERBY_JDBC_RESULT_SET " +
   "READS SQL DATA " +
   "RETURNS NULL ON NULL INPUT ";
   
   clientSQLExecute(1, createFunction1);

   sqlExecuteVerify(new int[] {1}, null,
       "SELECT s.* FROM Table (subsetRows('ORDERS',10, 15)) s",
            TestUtil.getResourcesDir()
           + "/lib/checkFunctions.xml", "q_2"); 

   clientSQLExecute(1, "DROP FUNCTION times");
   clientSQLExecute(1, "DROP FUNCTION subsetRows");
  }

  public void _testFunctionExecutionTimes() throws Exception {
 // Start one client and two servers
    startVMs(1, 2);

    // Create a table
    clientSQLExecute(1, "create table ORDERS (ID int primary key, VOL int NOT NULL unique, "
        + "SECURITY_ID varchar(10)) partition by Primary Key redundancy 1");
    
   // partition by Primary Key redundancy 1 
   for(int i=0; i<100; i++) {
     String statement="insert into ORDERS values ("+i+","+i+",'char"+i+"')";
     clientSQLExecute(1, statement);
   }
   
// Create a table
   clientSQLExecute(1, "create table LOG (ThreadID bigint primary key)");
   
   String createFunction="CREATE FUNCTION times " +
                "(value INTEGER) " +
                "RETURNS INTEGER "+
                "LANGUAGE JAVA " +
                "EXTERNAL NAME 'com.pivotal.gemfirexd.functions.TestFunctions.timesWithLog' " +
                "PARAMETER STYLE JAVA " +
                "NO SQL " +
                "RETURNS NULL ON NULL INPUT ";
   
     
   clientSQLExecute(1, createFunction);
   sqlExecuteVerify(new int[] {1}, null,
       "SELECT times(ID) FROM ORDERS WHERE ID=10",
            TestUtil.getResourcesDir()
           + "/lib/checkFunctions.xml", "q_3"); 
   
   
   sqlExecuteVerify(new int[] {1}, null,
       "SELECT * FROM LOG",
            TestUtil.getResourcesDir()
           + "/lib/checkFunctions.xml", "q_4"); 
   
   
  /* String createFunction1="CREATE FUNCTION subsetRows " +
   "(tableName VARCHAR(20), low INTEGER, high INTEGER) " +
   "RETURNS Table (ID INT, SECURITY_ID VARCHAR(10)) "+
   "LANGUAGE JAVA " +
   "EXTERNAL NAME 'com.pivotal.gemfirexd.functions.TestFunctions.subset' " +
   "PARAMETER STYLE DERBY_JDBC_RESULT_SET " +
   "READS SQL DATA " +
   "RETURNS NULL ON NULL INPUT ";
   
   clientSQLExecute(1, createFunction1);
   
   sqlExecuteVerify(new int[] {1}, null,
       "SELECT s.* FROM Table (subsetRows('ORDERS',10, 15)) s",
            TestUtil.getResourcesDir()
           + "/lib/checkFunctions.xml", "q_2"); 
   
   clientSQLExecute(1, "DROP FUNCTION times");
   
   clientSQLExecute(1, "DROP FUNCTION subsetRows");*/
       
  }
  
}
