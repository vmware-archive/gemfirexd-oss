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
import java.sql.Types;



import junit.framework.Assert;
import junit.framework.TestSuite;
import junit.textui.TestRunner;
import org.apache.derbyTesting.junit.JDBC;
import com.gemstone.gemfire.cache.Region;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverAdapter;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverHolder;
import com.pivotal.gemfirexd.internal.engine.store.RegionKey;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.monitor.Monitor;
/**
 * junit tests for the Hash1Index
 * 
 * @todo investigate the problem that the result set is closed after
 *        accessing the run time statistics. 
 * @author yjing
 * @author rdubey
 *
 */
public class UniqueKeyIndexTest extends JdbcTestBase {

  public UniqueKeyIndexTest(String name) {
    super(name);
  }

  public static void main(String[] args) {
    TestRunner.run(new TestSuite(UniqueKeyIndexTest.class));
  }
  
  
  
  public String getOverflowSuffix() {
    return  "";
  }

  /**
   * Test Primary index with DML.
   * 
   * @throws SQLException
   */

  public void testDMLOnUniqeKeyIndex() throws SQLException {
    Connection conn = getConnection();
    Statement s = conn.createStatement();

    s.execute("create table t1 (c1 int primary key, c2 int NOT NULL, c3 char(20) NOT NULL, UNIQUE (c2,c3)) " +getOverflowSuffix());

    s.execute("insert into t1 (c1, c2, c3) values (10, 10, 'XXXX')");
    s.execute("insert into t1 (c1, c2, c3) values (20, 20, 'YYYY')");
    s.execute("insert into t1 (c1, c2, c3) values (30, 30, 'ZZZZ')");
    

    String[][] expectedRows = { { "10", "10", "XXXX" }, };

    // s.execute("call SYSCS_UTIL.SET_RUNTIMESTATISTICS(1)");
    //test select 
    ResultSet rs = s.executeQuery("select * from t1 where t1.c1=10");
    JDBC.assertFullResultSet(rs, expectedRows);
    try {
       Monitor.getStream().println(
    	          "<ExpectedException action=add>"
    	              + "com.gemstone.gemfire.cache.EntryExistsException"
    	              + "</ExpectedException>");
    	       Monitor.getStream().flush();      
      s.execute("insert into t1 (c1, c2, c3) values (40, 10, 'XXXX')");
      
    }
    catch (SQLException e) {
      return;
    }

    finally {
    	
    Monitor.getStream().println("<ExpectedException action=remove>"
                + "com.gemstone.gemfire.cache.EntryExistsException"
                + "</ExpectedException>");
    Monitor.getStream().flush();
    }
    
    throw new SQLException("Duplicate key Exception should be thrown!");
        
  }
  
  public void  testUniqueKeyMultiColumn() throws SQLException {

    Connection conn = getConnection();
    Statement s = conn.createStatement();

    s.execute("create table t1 (c1 int primary key, c2 int NOT NULL, c3 char(20) NOT NULL, UNIQUE (c2,c3))" +getOverflowSuffix());

    s.execute("insert into t1 (c1, c2, c3) values (10, 10, 'XXXX')");
    s.execute("insert into t1 (c1, c2, c3) values (20, 20, 'YYYY')");
    s.execute("insert into t1 (c1, c2, c3) values (30, 30, 'ZZZZ')");
    s.execute("insert into t1 (c1, c2, c3) values (40, 10, 'ZZZZ')");

    String[][] expectedRows = { { "10", "10", "XXXX" }, };

    // s.execute("call SYSCS_UTIL.SET_RUNTIMESTATISTICS(1)");
    // test select
    ResultSet rs = s.executeQuery("select * from t1 where t1.c1=10");
    JDBC.assertFullResultSet(rs, expectedRows);
    try {
    	Monitor.getStream().println(
  	          "<ExpectedException action=add>"
  	              + "com.gemstone.gemfire.cache.EntryExistsException"
  	              + "</ExpectedException>");
  	       Monitor.getStream().flush();      

      s.execute("update t1 set t1.c3='XXXX' where t1.c1=40");
    }
    catch (SQLException e) {
      if (e.toString().toLowerCase().contains("duplicate")) {
        return;
      }
    }

    finally {
    	Monitor.getStream().println("<ExpectedException action=remove>"
                + "com.gemstone.gemfire.cache.EntryExistsException"
                + "</ExpectedException>");
    Monitor.getStream().flush();
    }

    throw new SQLException("Duplicate key Exception should be thrown!");

  }
  
  /**
   * Test for different combinations for 39436 bug.
   * 
   * @throws SQLException
   *           on failure.
   */
  public void  testFor39436() throws SQLException {

    Connection conn = getConnection();
    Statement s = conn.createStatement();

    s.execute("create table t1 (c1 int primary key, c2 int NOT NULL, " +
    		"c3 char(20) NOT NULL, c4 int , UNIQUE (c2,c3))" + getOverflowSuffix());
    // create some indexes to check for proper updates for one or more indexes
    // with exceptions in some
    s.execute("create index idx1 on t1(c2, c3)");
    s.execute("create unique index idx2 on t1(c2, c3)");
    s.execute("create index idx3 on t1(c3, c4)");
    s.execute("create unique index idx4 on t1(c3, c4)");

    s.execute("insert into t1 (c1, c2, c3, c4) values (10, 10, 'XXXX', 10)");
    s.execute("insert into t1 (c1, c2, c3, c4) values (20, 20, 'YYYY', 20)");
    s.execute("insert into t1 (c1, c2, c3, c4) values (30, 30, 'ZZZZ', 30)");
    s.execute("insert into t1 (c1, c2, c3, c4) values (40, 30, 'XXXX', 40)");

    String[][] expectedRows = { { "10", "10", "XXXX", "10" }, };

    ResultSet rs = s.executeQuery("select * from t1 where t1.c1=10");
    JDBC.assertFullResultSet(rs, expectedRows);
    s.execute("update t1 set t1.c4=70 where t1.c1=40");
    boolean gotExpectedExcepiton = false;
    try {
    	Monitor.getStream().println(
  	          "<ExpectedException action=add>"
  	              + "com.gemstone.gemfire.cache.EntryExistsException"
  	              + "</ExpectedException>");
  	       Monitor.getStream().flush();      
      
      s.execute("update t1 set t1.c3='ZZZZ' where t1.c1=40");
    }
    catch (SQLException e) {
      if (!"23505".equals(e.getSQLState())) {
        throw e;
      }
      gotExpectedExcepiton = true;
      // return;
    }
    finally {
    	Monitor.getStream().println("<ExpectedException action=remove>"
                + "com.gemstone.gemfire.cache.EntryExistsException"
                + "</ExpectedException>");
        Monitor.getStream().flush();
    }

    if (!gotExpectedExcepiton) {
      throw new SQLException("Test failed");
    }

    gotExpectedExcepiton = false;

    try {
    	Monitor.getStream().println(
  	          "<ExpectedException action=add>"
  	              + "com.gemstone.gemfire.cache.EntryExistsException"
  	              + "</ExpectedException>");
  	       Monitor.getStream().flush();      	
      
      s.execute("update t1 set t1.c2=10 where t1.c1=40");
    }
    catch (SQLException e) {
      if (!"23505".equals(e.getSQLState())) {
        throw e;
      }
      gotExpectedExcepiton = true;
    }
    finally {
    	Monitor.getStream().println("<ExpectedException action=remove>"
                + "com.gemstone.gemfire.cache.EntryExistsException"
                + "</ExpectedException>");
        Monitor.getStream().flush();
    }

    if (!gotExpectedExcepiton) {
      throw new SQLException("Test failed");
    }

    gotExpectedExcepiton = false;
    try {
    	Monitor.getStream().println(
  	          "<ExpectedException action=add>"
  	              + "com.gemstone.gemfire.cache.EntryExistsException"
  	              + "</ExpectedException>");
  	       Monitor.getStream().flush();      	
      
      s.execute("update t1 set t1.c3='ZZZZ' where t1.c1=40 and t1.c4=70");
    }
    catch (SQLException e) {
      if (!"23505".equals(e.getSQLState())) {
        throw e;
      }
      gotExpectedExcepiton = true;
    }
    finally {
    	Monitor.getStream().println("<ExpectedException action=remove>"
                + "com.gemstone.gemfire.cache.EntryExistsException"
                + "</ExpectedException>");
      Monitor.getStream().flush();	
    }

    if (!gotExpectedExcepiton) {
      throw new SQLException("Test failed");
    }


    conn = getConnection();
    s = conn.createStatement();
    s.execute("update t1 set t1.c3='XXXX' where t1.c1=40 and t1.c4=70");

    // also check for value row after this update
    rs = s.executeQuery("select c1, c3 from t1 where c1=40");
    assertTrue(rs.next());
    assertEquals("XXXX", rs.getString("c3").trim());
    assertEquals(40, rs.getInt(1));
    assertFalse(rs.next());

    // also try deleting this row
    s.execute("delete from t1 where c3='XXXX'");
    rs = s.executeQuery("select c1, c3 from t1 where c1=40");
    assertFalse(rs.next());
  }

  /**
   * Test Mics methods added to get GemFireKey,
   */
  public void testMiscGetGemFireKey() throws SQLException, StandardException {

    Connection conn = getConnection();
    Statement s = conn.createStatement();

    s.execute("create table TestTable (c1 int primary key, c2 int NOT NULL, " +
        "c3 char(20) NOT NULL, c4 int , UNIQUE (c2,c3))"+ getOverflowSuffix());

    s.execute("insert into TestTable (c1, c2, c3, c4) values (10, 10, 'XXXX', 10)");
    s.execute("insert into TestTable (c1, c2, c3, c4) values (20, 20, 'YYYY', 20)");
    s.execute("insert into TestTable (c1, c2, c3, c4) values (30, 30, 'ZZZZ', 30)");
    s.execute("insert into TestTable (c1, c2, c3, c4) values (40, 30, 'XXXX', 40)");

    //Remember to captilize the name.
    final Region<?, ?> tableRegion = Misc
        .getRegionForTable(getCurrentDefaultSchemaName() + ".TESTTABLE", true);

    RegionKey key = getGemFireKey(10, tableRegion);
    Object value = tableRegion.get(key);
    assertNotNull("Value cannot be null", value);

    int i = 20;
    key = getGemFireKey(i, tableRegion);
    value = tableRegion.get(key);
    assertNotNull("Value cannot be null", value);
  }

  /** 
   * Test the index updates including the single row update and multiple rows update.
   * 
   * 
   */
  public void testFor39605() throws SQLException {
	  Connection conn = getConnection();
	  Statement s = conn.createStatement();
	  s.execute("create table trade.customers (cid int not null, cust_name varchar(100), " +
	  		                                   "since date, addr varchar(100)," +
	  		                                   "tid int, primary key (cid))"+getOverflowSuffix());
	  s.execute("create table trade.networth (cid int not null, cash decimal (30, 20), " +
	  		                                  "securities decimal (30, 20), loanlimit int, " +
	  		                                  "availloan decimal (30, 20),  tid int," +
	  		                                  "constraint netw_pk primary key (cid), " +
	  		                                  "constraint cust_newt_fk foreign key (cid) references trade.customers (cid) on delete RESTRICT, " +
	  		                                  "constraint cash_ch check (cash>=0), " +
	  		                                  "constraint sec_ch check (securities >=0)," +
	  		                                  "constraint availloan_ck check (loanlimit>=availloan and availloan >=0))"+ getOverflowSuffix());
	  s.execute("INSERT INTO trade.customers VALUES (10, 'name10', CURRENT_DATE, 'address10',10)");
	  s.execute("INSERT INTO trade.customers VALUES (20, 'name20', CURRENT_DATE, 'address20',20)");
	  s.execute("INSERT INTO trade.customers VALUES (30, 'name30', CURRENT_DATE, 'address30',30)");
	  s.execute("INSERT INTO trade.customers VALUES (40, 'name40', CURRENT_DATE, 'address40',40)");
	  s.execute("INSERT INTO trade.customers VALUES (50, 'name50', CURRENT_DATE, 'address50',50)");
	  
	  s.execute("INSERT INTO trade.networth  VALUES (10, 2310.00, 1234.00, 56,   45.00, 10)");
	  s.execute("INSERT INTO trade.networth  VALUES (20, 2310.00, 1234.00, 456,  245.00, 10)");
	  s.execute("INSERT INTO trade.networth  VALUES (30, 2310.00, 1234.00, 3456, 1245.00, 10)");
	  s.execute("INSERT INTO trade.networth  VALUES (40, 2310.00, 1234.00, 3456, 1245.00, 10)");
	  s.execute("INSERT INTO trade.networth  VALUES (50, 2310.00, 1234.00, 3456, 1245.00, 10)");
	  
	       
	     
	  boolean gotExpectedException=false;
      try {
    	 
    	 Monitor.getStream().println(
    	          "<ExpectedException action=add>"
    	              + "java.sql.SQLIntegrityConstraintViolationException"
    	              + "</ExpectedException>");
    	       Monitor.getStream().flush();

	  s.executeUpdate("UPDATE trade.networth set loanlimit=1000 where securities>1000 and tid=10");
     }catch (Exception e) {
    	 gotExpectedException=true;
     }        
     finally {
    	 Monitor.getStream().println("<ExpectedException action=remove>"
    	          + "java.sql.SQLIntegrityConstraintViolationException"
    	          + "</ExpectedException>");
    	Monitor.getStream().flush();
     }
     
     if (!gotExpectedException) {
         throw new SQLException("Test failed");
     }
     ResultSet rs = s.executeQuery("select * from trade.networth where loanlimit=1000");
	 int count=0;
	 while(rs.next()) {
	    	++count; 
	 }     
	 Assert.assertTrue((count>=0) && (count<3)); 
     
  }
  
  /**
   * Tests unique constraints on replicated table.
   * @throws Exception
   */
  public void testBug40040() throws Exception {
    Connection conn = null;
    Statement s = null;
    try {
      conn = getConnection();
      s = conn.createStatement();
    
      s.execute("create table Child ( id2 int primary key, " +
          "sector_id2 int unique not null, " +
          "subsector_id2 int ) replicate" + getOverflowSuffix());
       
      s.execute("insert into Child values (1,1,1)");
      Monitor.getStream().println("<ExpectedException action=add>"
              + "com.gemstone.gemfire.cache.EntryExistsException"
              + "</ExpectedException>");
  Monitor.getStream().flush();
        for (int i=0; i < 10 ; i++) {
        
          try {
            s.execute("insert into Child values (2,1,1)");
            fail("Insert should not have successed  as unique key" +
            " violation would occur");
          } catch(SQLException sqle) {
            assertEquals(sqle.toString(), "23505", sqle.getSQLState());
          }
        }
                  
      
    }
    finally {
    	Monitor.getStream().println("<ExpectedException action=remove>"
                + "com.gemstone.gemfire.cache.EntryExistsException"
                + "</ExpectedException>");
        Monitor.getStream().flush();
        GemFireXDQueryObserverHolder
        .setInstance(new GemFireXDQueryObserverAdapter());
        conn = getConnection();
        s = conn.createStatement();
        s.execute("Drop table Child ");     
             
    } 
    
  }
  
 
  public void testBug40342_1() throws Exception {
    Connection conn = null;
    Statement s = null;
    try {
      conn = getConnection();
      s = conn.createStatement();
      s.execute("create table master ( id int primary key, dummy int ) "+getOverflowSuffix());

    
      s.execute("create table Child ( id2 int primary key, " +
          "sector_id2 int , " +
          "subsector_id2 int, constraint cust_newt_fk foreign key (sector_id2) references master (id)  ) partition by column(id2) "+getOverflowSuffix());
      PreparedStatement psInsertMaster = conn.prepareStatement("insert into Master values (?,?)");
      PreparedStatement psInsertChild = conn.prepareStatement("insert into Child values (?,?,?)");
      for(int i=0; i < 10;++i) {
        psInsertMaster.setInt(1, i);
        psInsertMaster.setInt(2, i);        
        psInsertMaster.execute();
        psInsertChild.setInt(1, i);
        psInsertChild.setInt(2, i);
        psInsertChild.setInt(3, 1);
        psInsertChild.execute();
       
      }
      
      String sql ="select id2 from Child where id2 IN (?,?,?) and id2 < ?"    ;
      PreparedStatement ps  = conn.prepareStatement(sql);
      ps.setInt(1, 3);
      ps.setInt(2, 4);
      ps.setInt(3, 5);
      ps.setInt(4, 4);
      ResultSet rs = ps.executeQuery();
      int size =0;
      while(rs.next()) {
        ++size;        
      }
      assertEquals(1,size);
             
    }
    finally {
        Monitor.getStream().println("<ExpectedException action=remove>"
                + "com.gemstone.gemfire.cache.EntryExistsException"
                + "</ExpectedException>");
        Monitor.getStream().flush();
        GemFireXDQueryObserverHolder
        .setInstance(new GemFireXDQueryObserverAdapter());
        conn = getConnection();
        s = conn.createStatement();
        s.execute("Drop table Child ");
        s.execute("Drop table Master ");  
             
    } 
    
  }
  public void _testBug40427() throws Exception {
    Connection conn = null;
    Statement s = null;
    try {
      conn = getConnection();
      s = conn.createStatement();
      s.execute("create table master ( id int primary key, dummy int ) "+getOverflowSuffix());

    
      s.execute("create table Child ( id2 int , " +
          "sector_id2 int , " +
          "subsector_id2 int, constraint cust_newt_fk foreign key (sector_id2) references master (id))   "+ getOverflowSuffix());
      PreparedStatement psInsertMaster = conn.prepareStatement("insert into Master values (?,?)");
      PreparedStatement psInsertChild = conn.prepareStatement("insert into Child values (?,?,?)");
      for(int i=0; i < 16;++i) {
        psInsertMaster.setInt(1, i);
        psInsertMaster.setInt(2, i);        
        psInsertMaster.execute();       
       
      }
      for(int i=0; i < 9 ; ++i) {
        psInsertChild.setInt(1, i);
        psInsertChild.setInt(2, i+1);
        psInsertChild.setInt(3, i+1);
        psInsertChild.execute();
      }
      for(int i=9; i < 14 ; ++i) {
        psInsertChild.setInt(1, i);
        psInsertChild.setNull(2, Types.INTEGER);
        psInsertChild.setInt(3, i+1);
        psInsertChild.execute();
      }
      String sql ="select id2,sector_id2 from Child where id2 =?  and id2  < ?"    ;
      PreparedStatement ps  = conn.prepareStatement(sql);
      ps.setInt(1, 9);
      ps.setInt(2, 10);
            
      ResultSet rs = ps.executeQuery();
      int size =0;
      while(rs.next()) {
        assertEquals(rs.getInt(1),9);
        assertEquals(rs.getInt(2),0);
        ++size;        
      }
      assertEquals(1,size);
             
    }
    finally {
        Monitor.getStream().println("<ExpectedException action=remove>"
                + "com.gemstone.gemfire.cache.EntryExistsException"
                + "</ExpectedException>");
        Monitor.getStream().flush();
        GemFireXDQueryObserverHolder
        .setInstance(new GemFireXDQueryObserverAdapter());
        conn = getConnection();
        s = conn.createStatement();
        s.execute("Drop table Child ");     
             
    } 
    
  }
 
  
  }
