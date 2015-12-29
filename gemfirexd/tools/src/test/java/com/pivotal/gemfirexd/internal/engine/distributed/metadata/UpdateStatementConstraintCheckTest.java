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
package com.pivotal.gemfirexd.internal.engine.distributed.metadata;

import java.net.BindException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import junit.framework.TestSuite;
import junit.textui.TestRunner;

import com.gemstone.gemfire.cache.EntryExistsException;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.internal.AvailablePort;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserver;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverAdapter;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverHolder;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.QueryInfo;
import com.pivotal.gemfirexd.internal.engine.sql.execute.AbstractGemFireActivation;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.impl.sql.GenericPreparedStatement;
import com.pivotal.gemfirexd.jdbc.JdbcTestBase;

/**
 * Tests to verify that the constraints like Foreign key, unique, not null etc are respected. 
 * @author Asif
 *
 */
public class UpdateStatementConstraintCheckTest extends JdbcTestBase {
  private boolean callbackInvoked = false;
  private final int index = 0;

  public UpdateStatementConstraintCheckTest(String name) {
    super(name);
  }
  
  public static void main(String[] args) {
    TestRunner.run(new TestSuite(UpdateStatementConstraintCheckTest.class));
  }

  public void testForeignKeyCheckForRegionPutConvertibleStatement_SingleColumnForeignKey()
      throws Exception {
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s.execute("create table INSTRUMENTS (id1 int primary key, sector_id1 int, subsector_id1 int) PARTITION BY COLUMN (sector_id1) " + getSuffix());
    s.execute("create table Child ( id2 int primary key, sector_id2 int, subsector_id2 int, " +
    		"foreign key (sector_id2) references instruments (id1) ) "+ getSuffix());
    s.execute("insert into instruments values (1,1,1)");
    s.execute("insert into Child values (1,1,1)");
    String query = "update Child set sector_id2 = ? where id2 = ?";
    PreparedStatement ps = conn.prepareStatement(query);
    ps.setInt(1, 2); 
    ps.setInt(2, 1);
    GemFireXDQueryObserver old = null;
    addExpectedException(new Object[] { FunctionException.class,
        EntryNotFoundException.class });
    try {
      ps.executeUpdate();
      fail("Update should not have occured as foreign key violation should occur");
    } catch (SQLException sqle) {     
      assertEquals(sqle.toString(), "23503", sqle.getSQLState());
    }
    finally {
      removeExpectedException(new Object[] { FunctionException.class,
          EntryNotFoundException.class });
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
      this.callbackInvoked = false;
    }
  }

  public void testBug39995() throws Exception {
    Connection conn = getConnectionWithRandomMcastPort();
    Statement s = conn.createStatement();
    s.execute("create table INSTRUMENTS (id1 int primary key, sector_id1 int, subsector_id1 int) " +
    		"PARTITION BY COLUMN (id1) "+ getSuffix());
    s.execute("create table Child ( id2 int primary key, sector_id2 int, subsector_id2 int, " +
    		"foreign key (sector_id2) references instruments (id1) ) "+ getSuffix());
    s.execute("insert into instruments values (1,1,1)");
    s.execute("insert into Child values (1,1,1)");
    String query = "update Child set subsector_id2 = ? where id2 = ?";
    PreparedStatement ps = conn.prepareStatement(query);
    ps.setInt(1, 2); 
    ps.setInt(2, 1);
    GemFireXDQueryObserver old = null;
    try {  
      ps.executeUpdate();
    }    
    finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
      this.callbackInvoked = false;
    }
  }
  
  public void testOtherConstraintCheckForRegionPutConvertibleStatement_SingleColumn_Bug40017() throws Exception{
    Connection conn = getConnection();
    Statement s = conn.createStatement();  
    s.execute("create table Child ( id2 int primary key, sector_id2 int unique, subsector_id2 int  ) "+ getSuffix());    
    s.execute("insert into Child values (1,1,1)");
    s.execute("insert into Child values (2,2,2)");
    String query = "update Child set sector_id2 = ? where id2 = ?";
    PreparedStatement ps = conn.prepareStatement(query);
    ps.setInt(1, 1); 
    ps.setInt(2, 2);
    GemFireXDQueryObserver old = null;
    TestUtil.addExpectedException(EntryExistsException.class);
    try {
      ps.executeUpdate();
      fail("Update should not have occured as unique constraint violation would occur");
    } catch (SQLException sqle) {     
      assertTrue(sqle.getMessage().indexOf("duplicate key value in a unique or primary key constraint") != -1);
    } finally {
      TestUtil.removeExpectedException(EntryExistsException.class);
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
      this.callbackInvoked = false;
    }
  }
  
  /**
   * If the column being updated is updated to existing value it is not a violation
   * @throws Exception
   */
  public void testNoUniqueConstraintViolationForRegionPutConvertibleStatement_SingleColumn() throws Exception{
    Connection conn = getConnection();
    Statement s = conn.createStatement();  
    s.execute("create table Child ( id2 int primary key, sector_id2 int unique, subsector_id2 int  ) "+ getSuffix());    
    s.execute("insert into Child values (1,1,1)");
    s.execute("insert into Child values (2,2,2)");
    String query = "update Child set sector_id2 = ? where id2 = ?";
    PreparedStatement ps = conn.prepareStatement(query);
    ps.setInt(1, 2); 
    ps.setInt(2, 2);
    GemFireXDQueryObserver old = null;
    try {  
      int n = ps.executeUpdate();     
      assertTrue(n == 1);
    } catch(SQLException sqle) {     
      fail("Update should be allowed as the column is being updated to the existing value only");
      
    } finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
      this.callbackInvoked = false;
    }
  }

  public void testMultiColumnForeignKeyViolation_1_Bug39987() throws Exception {
    GemFireXDQueryObserver old = null;
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s.execute("create table INSTRUMENTS (id1 int , sector_id1 int, subsector_id1 int , primary key(id1,sector_id1))" +
    		" PARTITION BY Primary key "+ getSuffix());
    s.execute("create table Child ( id2 int primary key, sector_id2 int, subsector_id2 int, " +
    		"foreign key (subsector_id2,sector_id2) references instruments (id1,sector_id1) ) partition by primary key "+ getSuffix());
    s.execute("insert into instruments values (1,1,1)");
    s.execute("insert into instruments values (2,2,1)");
    s.execute("insert into Child values (1,1,1)");
    String query = "update Child set sector_id2 = ? where id2 = ?";
    
    PreparedStatement ps = conn.prepareStatement(query);
    ps.setInt(1, 2);
    ps.setInt(2, 1);
    addExpectedException(FunctionException.class);
    try {
      ps.executeUpdate();
      fail("Update should have failed as it is violation of foreign key constraint");
    } catch (SQLException sqle) {
      assertEquals(sqle.toString(), "23503", sqle.getSQLState());
    } finally {
      removeExpectedException(FunctionException.class);
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
      this.callbackInvoked = false;
    }
    
  }
  
  public void testMultiColumnForeignKeyViolation_3_Bug39987() throws Exception {
    GemFireXDQueryObserver old = null;
    try {
      Connection conn = getConnectionWithRandomMcastPort();
      Statement s = conn.createStatement();
      s.execute("create table INSTRUMENTS (id1 int , sector_id1 int, subsector_id1 int , " +
      		"primary key(id1,sector_id1)) PARTITION BY Primary key "+ getSuffix());
      s.execute("create table Child ( id2 int primary key, sector_id2 int, subsector_id2 int, " +
      		"foreign key (subsector_id2,sector_id2) references instruments (id1,sector_id1) ) partition by primary key "+ getSuffix());
      s.execute("insert into instruments values (1,1,1)");
      s.execute("insert into instruments values (2,2,1)");
      s.execute("insert into instruments values (1,2,1)");
      s.execute("insert into Child values (1,1,1)");
      String query = "update Child set sector_id2 = ? where id2 = ?";
      /*
       old = GemFireXDQueryObserverHolder
       .setInstance(new GemFireXDQueryObserverAdapter() {
       public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo, GenericPreparedStatement gps) {
       if (qInfo instanceof UpdateQueryInfo) {
       UpdateStatementConstraintCheckTest.this.callbackInvoked = true;
       UpdateQueryInfo uqi = (UpdateQueryInfo)qInfo;
       assertFalse(uqi.isPrimaryKeyBased());
       }
       
       }
       
       });*/
      
      PreparedStatement ps = conn.prepareStatement(query);
      ps.setInt(1,2);
      ps.setInt(2,1);
      ps.executeUpdate();
    } finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
      this.callbackInvoked = false;
    }
    
  }

  public void testMultiColumnForeignKeyViolation_2_Bug39987() throws Exception {
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s.execute("create table INSTRUMENTS (id1 int , sector_id1 int, subsector_id1 int , " +
    		"primary key(id1,sector_id1)) PARTITION BY Primary key "+ getSuffix());
    s.execute("create table Child ( id2 int primary key, sector_id2 int, subsector_id2 int, " +
    		"foreign key (subsector_id2,sector_id2) references instruments (id1,sector_id1) ) partition by primary key"+ getSuffix());
    s.execute("insert into instruments values (1,1,1)");
    s.execute("insert into instruments values (2,2,1)");
    s.execute("insert into instruments values (3,2,2)");
    s.execute("insert into instruments values (1,2,3)");
    s.execute("insert into Child values (1,1,1)");
    String query = "update Child set sector_id2 = ?, subsector_id2 = ? where id2 = ?";
    PreparedStatement ps = conn.prepareStatement(query);
    ps.setInt(1, 2); 
    ps.setInt(2, 1);
    ps.setInt(3, 1);
    GemFireXDQueryObserver old = null;
    try {  
      ps.executeUpdate();
      
    } catch(SQLException sqle) {     
      fail("Update should have occured as foreign key violation is not the case");      
    } finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
      this.callbackInvoked = false;
    }
    
  }

  public void testMultiColumnForeignKeyViolation_4_Bug39987() throws Exception {
    GemFireXDQueryObserver old = null;
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s.execute("create table INSTRUMENTS (id1 varchar(5) , sector_id1 int, subsector_id1 int ," +
    		" primary key(id1,sector_id1)) PARTITION BY Primary key "+ getSuffix());
    s.execute("create table Child ( id2 int primary key, sector_id2 int," +
    		" subsector_id2 varchar(5), foreign key (subsector_id2,sector_id2) references" +
    		" instruments (id1,sector_id1) ) partition by primary key "+ getSuffix());
    s.execute("insert into instruments values ('1',1,1)");
    s.execute("insert into instruments values ('2',2,1)");
    s.execute("insert into Child values (1,1,'1')");
    String query = "update Child set sector_id2 = ? where id2 = ?";
    
    PreparedStatement ps = conn.prepareStatement(query);
    ps.setInt(1, 2);
    ps.setInt(2, 1);
    addExpectedException(FunctionException.class);
    try {
      ps.executeUpdate();
      fail("Update should have failed as it is violation of foreign key constraint");
    } catch (SQLException sqle) {
      assertEquals(sqle.toString(), "23503", sqle.getSQLState());
    } finally {
      removeExpectedException(FunctionException.class);
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
      this.callbackInvoked = false;
    }
    
  }
  
  /**
   * Tests the not null constraint check
   * @throws Exception
   */
  public void testNotNullConstraintForRegionPutConvertibleStatement_Bug40018() throws Exception{
    Connection conn = getConnection();
    Statement s = conn.createStatement();  
    s.execute("create table Child ( id2 int primary key, sector_id2 int not null, subsector_id2 int  ) "+ getSuffix());    
    s.execute("insert into Child values (1,1,1)");
    s.execute("insert into Child values (2,2,2)");
    String query = "update Child set sector_id2 = ? where id2 = ?";
    PreparedStatement ps = conn.prepareStatement(query);   
    ps.setNull(1, java.sql.Types.INTEGER); 
    ps.setInt(2, 2);
    GemFireXDQueryObserver old = null;
    try {  
      ps.executeUpdate();
      fail("Update should not  be allowed as the not null constraint check is violated");
    } catch(SQLException sqle) {     
      assertTrue(sqle.getMessage().indexOf("cannot accept a NULL value") != -1);
      
    } finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
      this.callbackInvoked = false;
    }
  }
  
  
  public void testNoDoubleFKCheckForPutConvertibleStatement_SingleColumnForeignKey_Bug40000() throws Exception {
    Connection conn = getConnectionWithRandomMcastPort();
    Statement s = conn.createStatement();
    s.execute("create table INSTRUMENTS (id1 int primary key, sector_id1 int, subsector_id1 int) PARTITION BY COLUMN (sector_id1) "+ getSuffix());
    s.execute("create table Child ( id2 int primary key, sector_id2 int, subsector_id2 int, " +
    		"foreign key (sector_id2) references instruments (id1) ) "+ getSuffix());
    s.execute("insert into instruments values (1,1,1)");
    s.execute("insert into Child values (1,1,1)");
    String query = "update Child set sector_id2 = ? where id2 = ?";
    PreparedStatement ps = conn.prepareStatement(query);
    ps.setInt(1, 2); 
    ps.setInt(2, 1);
    final boolean regnLevelCheckInvoked [] = new boolean[]{false};
    GemFireXDQueryObserver old = GemFireXDQueryObserverHolder.setInstance(
      new GemFireXDQueryObserverAdapter() {
         @Override
         public void beforeForeignKeyConstraintCheckAtRegionLevel() {   
           regnLevelCheckInvoked[0] = true;
         }
      });
    addExpectedException(new Object[] { FunctionException.class,
        EntryNotFoundException.class });
    try {
      ps.executeUpdate();
      fail("Update should not have occured as foreign key violation should occur");
    } catch (SQLException sqle) {
      assertEquals(sqle.toString(), "23503", sqle.getSQLState());
      assertTrue( regnLevelCheckInvoked[0] );
    } finally {
      removeExpectedException(new Object[] { FunctionException.class,
          EntryNotFoundException.class });
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
      this.callbackInvoked = false;
    }
  }
  
  public void _testNoDoubleUniqueConstraintCheckForPutConvertibleStatement_Bug40000() throws Exception{
    Connection conn = getConnection();
    Statement s = conn.createStatement();  
    s.execute("create table Child ( id2 int primary key, sector_id2 int unique, subsector_id2 int  ) "+ getSuffix());    
    s.execute("insert into Child values (1,1,1)");
    s.execute("insert into Child values (2,2,2)");
    String query = "update Child set sector_id2 = ? where id2 = ?";
    PreparedStatement ps = conn.prepareStatement(query);
    ps.setInt(1, 3); 
    ps.setInt(2, 2);
    final boolean regnLevelCheckInvoked [] = new boolean[]{false};
    GemFireXDQueryObserver old = GemFireXDQueryObserverHolder.setInstance(
      new GemFireXDQueryObserverAdapter() {
        @Override
        public void beforeUniqueConstraintCheckAtRegionLevel() {   
          regnLevelCheckInvoked[0] = true;
        }
      });
    
    try {  
      ps.executeUpdate();
      assertTrue(regnLevelCheckInvoked[0]);
    } finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
      this.callbackInvoked = false;
    }
  }
  
  /**
   * The update statement should be put convertible, with unique
   * constraint check being done at query accessor level, while
   * foreign key check is not possible at query node level so 
   * the foreign key check should be done at region level.
   * @throws Exception
   */
  public void testPartialConstraintCheckBehaviourForRegionPutStatement_Bug40000()
      throws Exception {
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s.execute("create table INSTRUMENTS (id1 int , sector_id1 int, subsector_id1 int , " +
    		"primary key(id1,sector_id1)) PARTITION BY Primary key "+ getSuffix());
    s.execute("create table Child ( id2 int primary key, sector_id2 int, subsector_id2 int, " +
    		"temp int unique , foreign key (subsector_id2,sector_id2) references " +
    		"instruments (id1,sector_id1)) partition by primary key  "+ getSuffix());
    s.execute("insert into instruments values (1,1,1)");
    s.execute("insert into instruments values (2,2,1)");
    s.execute("insert into instruments values (1,2,1)");
    s.execute("insert into Child values (1,1,1,1)");
    final boolean regnLevelCheckInvoked [] = new boolean[]{false,false};
    GemFireXDQueryObserver old = GemFireXDQueryObserverHolder.setInstance(
      new GemFireXDQueryObserverAdapter() {
        @Override
        public void beforeUniqueConstraintCheckAtRegionLevel() {   
          regnLevelCheckInvoked[0] = true;
        }
                                                                          
        @Override
        public void beforeForeignKeyConstraintCheckAtRegionLevel() {   
          regnLevelCheckInvoked[1] = true;
        }
                                                                          
        @Override        
        public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo, GenericPreparedStatement gps, LanguageConnectionContext lcc)  {
          try {
            assertTrue(qInfo.isPrimaryKeyBased());
          } catch(Exception e) {
            e.printStackTrace();
            fail(e.toString());
          }
        }
      });
    
    String query = "update Child set sector_id2 = ? , temp = ? where id2 = ?";
    PreparedStatement ps = conn.prepareStatement(query);
    ps.setInt(1, 2); 
    ps.setInt(2, 3);
    ps.setInt(3, 1);
    
    try {  
      ps.executeUpdate();
      assertTrue(regnLevelCheckInvoked[1]);
   //   assertTrue(regnLevelCheckInvoked[0]);
    } catch(SQLException sqle) {     
      fail("Update should have occured as foreign key violation is not the case");      
    } finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
      this.callbackInvoked = false;
    }    
  }
  
  /**
   *Verifies that the presence of Check constraint makes an otherwise region.put convertible update
   *into a Derby Activation
   */
  public void testNonRegionPutDueToCheckConstraint() throws Exception {
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s.execute("create table INSTRUMENTS (id1 int , sector_id1 int, subsector_id1 int , " +
    		"primary key(id1),constraint myconstraint check (subsector_id1 in (1, 2,3,4))) PARTITION BY Primary key "+ getSuffix());
   
    s.execute("insert into instruments values (1,1,1)");
    s.execute("insert into instruments values (2,2,2)");
    s.execute("insert into instruments values (3,2,3)");
    

    GemFireXDQueryObserver old = GemFireXDQueryObserverHolder.setInstance(
      new GemFireXDQueryObserverAdapter() {
        @Override        
        public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qInfo, GenericPreparedStatement gps, LanguageConnectionContext lcc)  {
          callbackInvoked = true;
          try {
            assertFalse(qInfo.isPrimaryKeyBased());
          } catch(Exception e) {
            e.printStackTrace();
            fail(e.toString());
          }
        }

        @Override
        public void beforeGemFireResultSetExecuteOnActivation(
            AbstractGemFireActivation activation) {
          fail("Activation used should be of Derby.");
        }
      });
    
    String query = "update instruments set subsector_id1 = ?  where id1 = ?";
    PreparedStatement ps = conn.prepareStatement(query);
    ps.setInt(1,4); 
    ps.setInt(2, 3);    
    
    try {  
      int n = ps.executeUpdate();
      assertEquals(1,n);
      assertTrue(callbackInvoked);      
    } catch(SQLException sqle) {     
      fail("Update should have occured");      
    } finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
      this.callbackInvoked = false;
    }    
  }

  /** keeping a few tests with mcast-port */
  private static Connection getConnectionWithRandomMcastPort()
      throws SQLException {
    Properties props = new Properties();
    RETRY: while (true) {
      final int mcastPort = AvailablePort
          .getRandomAvailablePort(AvailablePort.JGROUPS);
      props.setProperty("mcast-port", String.valueOf(mcastPort));
      try {
        return getConnection(props);
      } catch (SQLException ex) {
        if ("XJ040".equals(ex.getSQLState())) {
          // check if a BindException then retry
          Throwable t = ex;
          while ((t = t.getCause()) != null) {
            if (t instanceof BindException) {
              continue RETRY;
            }
          }
        }
        throw ex;
      }
    }
  }
  
  protected String getSuffix() {
    return " ";
  }
}
