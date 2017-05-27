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
package com.pivotal.gemfirexd.dbsync;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;

import com.gemstone.gemfire.cache.asyncqueue.AsyncEventQueue;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.callbacks.Event;
import com.pivotal.gemfirexd.dbsync.DBSynchronizerTestBase.TestNewGatewayEventListenerNotify;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.ddl.wan.GfxdGatewayEventListener;
import com.pivotal.gemfirexd.internal.engine.jdbc.GemFireXDRuntimeException;
import com.pivotal.gemfirexd.internal.engine.store.GemFireStore;

import io.snappydata.test.dunit.SerializableCallable;
import io.snappydata.test.dunit.SerializableRunnable;


import com.pivotal.gemfirexd.callbacks.AsyncEventListener;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.shared.common.sanity.SanityManager;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;


public class SerialAsyncEventListenerDUnit extends DBSynchronizerTestBase {

  public SerialAsyncEventListenerDUnit(String name) {
    super(name);
  }
  
  
  public static class AggregationListener implements AsyncEventListener {

    private static final Logger LOG = LoggerFactory.getLogger(
        AggregationListener.class.getName());

    private static final String DRIVER = "com.pivotal.gemfirexd.jdbc.ClientDriver";

    private static final String CONN_URL = "jdbc:gemfirexd:";

    private static final String SELECT_SQL = "select * from load_averages where weekday=? and time_slice=? and plug_id=?";

    private static final String UPDATE_SQL = "update load_averages set total_load=?, event_count=? where weekday=? and time_slice=? and plug_id=?";

    private String valueColumn;

    //load driver
    static {
      try {
        Class.forName(DRIVER).newInstance();
      } catch (ClassNotFoundException cnfe) {
        throw new RuntimeException("Unable to load the JDBC driver", cnfe);
      } catch (InstantiationException ie) {
        throw new RuntimeException("Unable to instantiate the JDBC driver", ie);
      } catch (IllegalAccessException iae) {
        throw new RuntimeException("Not allowed to access the JDBC driver", iae);
      }
    }

    private static ThreadLocal<Connection> dConn = new ThreadLocal<Connection>() {
      protected Connection initialValue() {
        return getConnection();
      }
    };

    private static Connection getConnection() {
      Connection conn;
      try {
        conn = DriverManager.getConnection(CONN_URL);
      } catch (SQLException e) {
        throw new IllegalStateException("Unable to create connection", e);
      }
      return conn;
    }

    private static ThreadLocal<PreparedStatement> selectStmt = new ThreadLocal<PreparedStatement> () {
      protected PreparedStatement initialValue()  {
        PreparedStatement stmt = null;
        try {
          stmt = dConn.get().prepareStatement(SELECT_SQL);
        } catch (SQLException se) {
          throw new IllegalStateException("Unable to retrieve statement ", se);
        }
        return stmt;
      }
    };

    private static ThreadLocal<PreparedStatement> updateStmt = new ThreadLocal<PreparedStatement> () {
      protected PreparedStatement initialValue()  {
        PreparedStatement stmt = null;
        try {
          stmt = dConn.get().prepareStatement(UPDATE_SQL);
        } catch (SQLException se) {
          throw new IllegalStateException("Unable to retrieve statement ", se);
        }
        return stmt;
      }
    };

    @Override
    public boolean processEvents(List<Event> events) {
      for (Event e : events) {
        LOG.info("AggregateListener::Processing event" + e);
        if (e.getType() == Event.Type.AFTER_INSERT) {
          ResultSet eventRS = e.getNewRowsAsResultSet();
          try {
            PreparedStatement s = selectStmt.get();
            s.setInt(1, eventRS.getInt("weekday"));
            s.setInt(2, eventRS.getInt("time_slice"));
            s.setInt(3, eventRS.getInt("plug_id"));
            ResultSet queryRS = s.executeQuery();

            if (queryRS.next()) {
              PreparedStatement update = updateStmt.get();
              update.setFloat(1,
                  queryRS.getFloat("total_load") + eventRS.getFloat(valueColumn));
              update.setInt(2, queryRS.getInt("event_count") + 1);
              update.setInt(3, queryRS.getInt("weekday"));
              update.setInt(4, queryRS.getInt("time_slice"));
              update.setInt(5, queryRS.getInt("plug_id"));
              update.executeUpdate();
            }
          } catch (SQLException ex) {
            ex.printStackTrace();
          }
        }
      }
      return true;
    }

    @Override
    public void close() {
      System.out.println("--->>> Closing connection from AEQ listener");
      try {
        getConnection().close();
      } catch (SQLException ex) {
      }
    }

    @Override
    public void init(String s) {
      valueColumn = s;
      SanityManager.TRACE_ON(GfxdConstants.TRACE_LOCK_PREFIX + "RAW_SENSOR");
      SanityManager.TRACE_ON(GfxdConstants.TRACE_LOCK_PREFIX + "APP.RAW_SENSOR");
      SanityManager.TRACE_ON(GfxdConstants.TRACE_LOCK_PREFIX + "LOAD_AVERAGES");
      SanityManager.TRACE_ON(GfxdConstants.TRACE_LOCK_PREFIX + "APP.LOAD_AVERAGES");
    }

    @Override
    public void start() {
    }
  }  
  
  public void testBug50091() throws Exception {
    startVMs(1, 2, -1, "SG1", null);
    
    //clientSQLExecute(1, "drop table if exists app.load_averages");
    //clientSQLExecute(1, "drop table if exists app.raw_sensor");
    clientSQLExecute(1, "create table app.raw_sensor" +
      "(id bigint, timestamp bigint, value float(23), " +
      "property smallint, plug_id integer, household_id integer, " +
      "house_id integer, weekday smallint, " +
      "time_slice smallint ) " +
      "partition by column (house_id) " 
      );

    //clientSQLExecute(1, "drop index if exists app.raw_sensor_idx");
    clientSQLExecute(1, "create index app.raw_sensor_idx on app.raw_sensor (weekday, time_slice, plug_id)");

    clientSQLExecute(1, "create table app.load_averages (house_id integer not null, " +
        "household_id integer, " +
        "plug_id integer not null, " +
        "weekday smallint not null, " +
        "time_slice smallint not null, " +
        "total_load float(23), " +
        "event_count integer) partition by column (house_id) colocate with (app.raw_sensor)");

    clientSQLExecute(1, "alter table app.load_averages " +
    		"add constraint LOAD_AVERAGES_PK PRIMARY KEY (house_id, plug_id, weekday, time_slice)");
    //clientSQLExecute(1, "drop index if exists app.load_averages_idx");
    clientSQLExecute(1, "create index app.load_averages_idx on app.load_averages (weekday, time_slice, plug_id)");
    //clientSQLExecute(1, "drop asynceventlistener if exists AggListener");    
    Runnable runnable = getExecutorForWBCLConfiguration(
        "SG1",
        "AggListener",
        "com.pivotal.gemfirexd.dbsync.SerialAsyncEventListenerDUnit$AggregationListener",
        "org.apache.derby.jdbc.EmbeddedDriver",
        "jdbc:derby:newDB;create=true,app,app", true, Integer.valueOf(1), null,
        Boolean.TRUE, null, null, null, 100000, null, false);
    runnable.run();
    
    clientSQLExecute(1, "alter table app.raw_sensor set asynceventlistener (AggListener)");
    clientSQLExecute(1, "call sys.start_async_event_listener('AggListener')");
    clientSQLExecute(1, "insert into app.raw_sensor values(1, null, 1.1, 1, 1, 1, 1, 1, 1)");
    clientSQLExecute(1, "insert into app.raw_sensor values(1, null, 1.1, 1, 1, 1, 1, 1, 1)");
    clientSQLExecute(1, "insert into app.raw_sensor values(1, null, 1.1, 1, 1, 1, 1, 1, 1)");
    clientSQLExecute(1, "call SYS.WAIT_FOR_SENDER_QUEUE_FLUSH('AGGLISTENER', 'true', 0)");
    clientSQLExecute(1, "drop table if exists app.load_averages");
    
    Connection connection = TestUtil.getConnection();
    ResultSet metadataRs = connection.getMetaData().getTables(null, "APP",
        "LOAD_AVERAGES", null);
    boolean foundTable = false;
    while (metadataRs.next()) {
      foundTable = metadataRs.getString(3).equalsIgnoreCase("LOAD_AVERAGES")
          && metadataRs.getString(2).equalsIgnoreCase("APP");
      assertFalse(foundTable);
    }
    metadataRs.close();
    connection.close();
  }

  public void testAsyncEventListenerConfiguration() throws Exception {
    startClientVMs(1, 0, null);
    startServerVMs(2, -1, "SG1");
    startServerVMs(2, -1, "SG2");

    Runnable runnable = getExecutorForWBCLConfiguration(
        "SG1",
        "WBCL1",
        "com.pivotal.gemfirexd.dbsync.DBSynchronizerTestBase$TestNewGatewayEventListener",
        "org.apache.derby.jdbc.EmbeddedDriver",
        "jdbc:derby:newDB;create=true,app,app", true, Integer.valueOf(1), null,
        Boolean.TRUE, null, null, null, 100000,
        "org.apache.derby.jdbc.EmbeddedDriver,jdbc:derby:newDB;create=true",
        false);
    runnable.run();
    // Verify Listener not attached on client
    Runnable listenerNotAttached = DBSynchronizerTestBase
        .getExecutorToCheckListenerNotAttached("WBCL1");
    Runnable listenerAttached = DBSynchronizerTestBase
        .getExecutorToCheckListenerAttached("WBCL1");
    clientExecute(1, listenerNotAttached);
    serverExecute(3, listenerNotAttached);
    serverExecute(4, listenerNotAttached);
    serverExecute(1, listenerAttached);
    serverExecute(2, listenerAttached);
    Runnable startWBCL = startAsyncEventListener("WBCL1");
    clientExecute(1, startWBCL);

    checkHubRunningAndIsPrimaryVerifier("WBCL1");

    Runnable wbclConfigVerifier = getExecutorForWBCLConfigurationVerification(
        "WBCL1", Integer.valueOf(1), null, Boolean.TRUE, null, null, null,
        100000,
        "org.apache.derby.jdbc.EmbeddedDriver,jdbc:derby:newDB;create=true,app,app");
    serverExecute(1, wbclConfigVerifier);
    serverExecute(2, wbclConfigVerifier);
  }

  public void testCreateAsyncEventListenerDDLReplay() throws Exception {
    startClientVMs(1, 0, null);
    startServerVMs(2, -1, "SG1");
    Runnable runnable = getExecutorForWBCLConfiguration(
        "SG1",
        "WBCL1",
        "com.pivotal.gemfirexd.dbsync.DBSynchronizerTestBase$TestNewGatewayEventListener",
        "org.apache.derby.jdbc.EmbeddedDriver", "jdbc:derby:newDB;create=true",
        true, Integer.valueOf(1), null, Boolean.TRUE, null, null, null, 100000,
        "org.apache.derby.jdbc.EmbeddedDriver,jdbc:derby:newDB;create=true",
        false);
    runnable.run();
    // Verify Listener not attached on client ( controller VM);
    Runnable listenerNotAttached = getExecutorToCheckListenerNotAttached("WBCL1");
    getExecutorToCheckListenerAttached("WBCL1");
    clientExecute(1, listenerNotAttached);

    Runnable startWBCL = startAsyncEventListener("WBCL1");
    clientExecute(1, startWBCL);
    // Start the other 2 dunit VMs as Server which belong to different Server
    // Group SG2
    startServerVMs(2, -1, "SG2");
    Callable<?> hubRunningAndIsPrimaryVerifier = getExecutorToCheckForHubRunningAndIsPrimary(
        "WBCL1", true /* wbcl should be running */);
    boolean[] isHubRunningAndPrimary1 = (boolean[])serverExecute(1,
        hubRunningAndIsPrimaryVerifier);

    boolean[] isHubRunningAndPrimary2 = (boolean[])serverExecute(2,
        hubRunningAndIsPrimaryVerifier);
    assertTrue(isHubRunningAndPrimary2[0]);
    assertTrue(isHubRunningAndPrimary1[0]);
    assertTrue(isHubRunningAndPrimary1[1] || isHubRunningAndPrimary2[1]);
    assertFalse(isHubRunningAndPrimary1[1] && isHubRunningAndPrimary2[1]);

    Runnable wbclConfigVerifier = getExecutorForWBCLConfigurationVerification(
        "WBCL1", Integer.valueOf(1), null, Boolean.TRUE, null, null, null,
        100000,
        "org.apache.derby.jdbc.EmbeddedDriver,jdbc:derby:newDB;create=true");
    serverExecute(1, wbclConfigVerifier);
    serverExecute(2, wbclConfigVerifier);

    serverExecute(3, listenerNotAttached);
    serverExecute(4, listenerNotAttached);
  }

  public void testAsyncEventListenerStopPropagationAndReplay() throws Exception {
    startVMs(1, 2, -1, "SG1", null);
    Runnable runnable = getExecutorForWBCLConfiguration(
        "SG1",
        "WBCL1",
        "com.pivotal.gemfirexd.dbsync.DBSynchronizerTestBase$TestNewGatewayEventListener",
        "org.apache.derby.jdbc.EmbeddedDriver",
        "jdbc:derby:newDB;create=true,app,app", true, Integer.valueOf(1), null,
        Boolean.TRUE, null, null, null, 100000, null, false);
    runnable.run();
    // Verify Listener not attached on client ( controller VM);
    Runnable listenerNotAttached = getExecutorToCheckListenerNotAttached("WBCL1");
    Runnable listenerAttached = getExecutorToCheckListenerAttached("WBCL1");
    clientExecute(1, listenerNotAttached);
    serverExecute(1, listenerAttached);
    serverExecute(2, listenerAttached);
    Runnable startWBCL = startAsyncEventListener("WBCL1");
    clientExecute(1, startWBCL);

    checkHubRunningAndIsPrimaryVerifier("WBCL1");

    // Now stop wbcl.
    Runnable stopWBCL = stopAsyncEventListener("WBCL1");
    clientExecute(1, stopWBCL);
    Callable<?> hubStoppedVerifier = getExecutorToCheckForHubRunningAndIsPrimary(
        "WBCL1", false);
    serverExecute(1, hubStoppedVerifier);
    serverExecute(2, hubStoppedVerifier);

    // Now Start two more server VMs , one part of SG1 while other is not.
    startServerVMs(1, -1, "SG1");
    startServerVMs(1, -1, "SG2");
    serverExecute(3, listenerAttached);
    serverExecute(3, hubStoppedVerifier);
    serverExecute(4, listenerNotAttached);
  }

  public void testAsyncEventListenerRemovePropagationAndReplay() throws Exception {
    startVMs(1, 2, -1, "SG1", null);
    Runnable runnable = getExecutorForWBCLConfiguration(
        "SG1",
        "WBCL1",
        "com.pivotal.gemfirexd.dbsync.DBSynchronizerTestBase$TestNewGatewayEventListener",
        "org.apache.derby.jdbc.EmbeddedDriver", "jdbc:derby:newDB;create=true",
        true, Integer.valueOf(1), null, Boolean.TRUE, null, null, null, 100000,
        null, false);
    runnable.run();
    // Verify Listener not attached on client ( controller VM);
    Runnable listenerNotAttached = getExecutorToCheckListenerNotAttached("WBCL1");
    Runnable listenerAttached = getExecutorToCheckListenerAttached("WBCL1");
    clientExecute(1, listenerNotAttached);
    serverExecute(1, listenerAttached);
    serverExecute(2, listenerAttached);
    Runnable startWBCL = startAsyncEventListener("WBCL1");
    clientExecute(1, startWBCL);

    checkHubRunningAndIsPrimaryVerifier("WBCL1");

    // Now stop wbcl.
    Runnable stopWBCL = stopAsyncEventListener("WBCL1");
    clientExecute(1, stopWBCL);
    Callable<?> hubStoppedVerifier = getExecutorToCheckForHubRunningAndIsPrimary(
        "WBCL1", false);
    serverExecute(1, hubStoppedVerifier);
    serverExecute(2, hubStoppedVerifier);

    // Now remove WBCL
    Runnable removeWBCL = dropAsyncEventListener("WBCL1");
    clientExecute(1, removeWBCL);
    Callable<?> wbclRemovedVerifier = getExecutorToCheckWBCLRemoved("WBCL1");
    serverExecute(1, wbclRemovedVerifier);
    serverExecute(2, wbclRemovedVerifier);

    // Now Start two more server VMs , one part of SG1 while other is not.
    startServerVMs(1, -1, "SG1");
    startServerVMs(1, -1, "SG2");
    serverExecute(3, wbclRemovedVerifier);
    serverExecute(4, listenerNotAttached);
  }

  public void _testAsyncEventListenerDispatching() throws Exception {
    startVMs(1, 1, -1, "SG2", null);
    clientSQLExecute(
        1,
        "create table TESTTABLE (ID int not null , "
            + "DESCRIPTION varchar(1024) , ADDRESS varchar(1024)) AsyncEventListener (WBCL1)");
    // now lets first do a dummy insert to create bucket on server 1
    clientSQLExecute(1, "insert into testtable values(114,'desc114','add114')");
    startServerVMs(1, -1, "SG2");
    Runnable runnable = getExecutorForWBCLConfiguration(
        "SG2",
        "WBCL1",
        "com.pivotal.gemfirexd.dbsync.DBSynchronizerTestBase$TestNewGatewayEventListenerNotify",
        "org.apache.derby.jdbc.EmbeddedDriver", "jdbc:derby:newDB;create=true",
        true, Integer.valueOf(1), null, Boolean.FALSE, null, null, null,
        100000, null, false);
    runnable.run();
    // configure the listener to collect events
    SerializableCallable sr = new SerializableCallable("Set Events Collector") {

      public Object call() {
        AsyncEventQueue asyncQueue = Misc.getGemFireCache().getAsyncEventQueue(
            "WBCL1");
        GfxdGatewayEventListener listener = (GfxdGatewayEventListener)asyncQueue
            .getAsyncEventListener();
        TestNewGatewayEventListenerNotify tgen = (TestNewGatewayEventListenerNotify)listener
            .getAsyncEventListenerForTest();
        Event[] events = new Event[2];
        tgen.setEventsExpected(events);
        return GemFireStore.getMyId().toString();
      }

    };
    final String listenerMember = (String)serverExecute(2, sr);
    Runnable startWBCL = startAsyncEventListener("WBCL1");
    clientExecute(1, startWBCL);
    // now lets first do an insert & then update
    clientSQLExecute(1, "insert into testtable values(1,'desc1','add1')");
    clientSQLExecute(1,
        "update TESTTABLE set description = 'modified' where ID =1");
    // validate data
    SerializableRunnable sc = new SerializableRunnable("validate callback data") {

      public void run() {
        try {
          AsyncEventQueue asyncQueue = Misc.getGemFireCache()
              .getAsyncEventQueue("WBCL1");
          GfxdGatewayEventListener listener = (GfxdGatewayEventListener)asyncQueue
              .getAsyncEventListener();
          TestNewGatewayEventListenerNotify tgen = (TestNewGatewayEventListenerNotify)listener
              .getAsyncEventListenerForTest();

          while (tgen.getNumEventsProcessed() != 2) {
            Thread.sleep(1000);
          }
          Event createdEvent = tgen.getEvents()[0];
          Object pk = createdEvent.getPrimaryKey()[0];
          Event ev = tgen.getEvents()[1];
          assertNotNull(ev);
          List<Object> list = ev.getNewRow();
          assertEquals(list.size(), 3);
          assertEquals(list.get(1), "modified");
          assertNull(list.get(0));
          assertNull(list.get(2));
          assertEquals(pk, ev.getPrimaryKey()[0]);
        } catch (Exception e) {
          throw GemFireXDRuntimeException.newRuntimeException(null, e);
        }
      }
    };
    serverExecute(2, sc);
  }

  public void Bug51213testAsyncEventListenerOnNonBucketHostingNode() throws Exception {
    startVMs(1, 1, -1, "SG1", null);
    clientSQLExecute(
        1,
        "create table TESTTABLE (ID int not null , "
            + "DESCRIPTION varchar(1024) , ADDRESS varchar(1024)) AsyncEventListener (WBCL1)");
    clientSQLExecute(1, "insert into testtable values(114,'desc114','add114')");
    startServerVMs(1, -1, "SG2");
    Runnable runnable = getExecutorForWBCLConfiguration(
        "SG2",
        "WBCL1",
        "com.pivotal.gemfirexd.dbsync.DBSynchronizerTestBase$TestNewGatewayEventListenerNotify",
        "org.apache.derby.jdbc.EmbeddedDriver", "jdbc:derby:newDB;create=true",
        true, Integer.valueOf(1), null, Boolean.FALSE, null, null, null,
        100000, null, false);
    runnable.run();
    // configure the listener to collect events
    SerializableCallable sr = new SerializableCallable("Set Events Collector") {

      public Object call() {
        AsyncEventQueue asyncQueue = Misc.getGemFireCache().getAsyncEventQueue(
            "WBCL1");
        GfxdGatewayEventListener listener = (GfxdGatewayEventListener)asyncQueue
            .getAsyncEventListener();
        TestNewGatewayEventListenerNotify tgen = (TestNewGatewayEventListenerNotify)listener
            .getAsyncEventListenerForTest();
        Event[] events = new Event[2];
        tgen.setEventsExpected(events);
        return GemFireStore.getMyId().toString();
      }

    };
    final String listenerMember = (String)serverExecute(2, sr);
    Runnable startWBCL = startAsyncEventListener("WBCL1");
    clientExecute(1, startWBCL);
    // now lets first do an insert & then update
    clientSQLExecute(1, "insert into testtable values(1,'desc1','add1')");
    clientSQLExecute(1,
        "update TESTTABLE set description = 'modified' where id =1");
    // validate data
    SerializableRunnable sc = new SerializableRunnable("validate callback data") {

      public void run() {
        try {
          AsyncEventQueue asyncQueue = Misc.getGemFireCache()
              .getAsyncEventQueue("WBCL1");
          GfxdGatewayEventListener listener = (GfxdGatewayEventListener)asyncQueue
              .getAsyncEventListener();
          TestNewGatewayEventListenerNotify tgen = (TestNewGatewayEventListenerNotify)listener
              .getAsyncEventListenerForTest();

          while (tgen.getNumEventsProcessed() != 2) {
            Thread.sleep(1000);
          }
          Event createdEvent = tgen.getEvents()[0];
          Object pk = createdEvent.getPrimaryKey()[0];
          Event ev = tgen.getEvents()[1];
          assertNotNull(ev);
          List<Object> list = ev.getNewRow();
          assertEquals(list.size(), 3);
          assertEquals(list.get(1), "modified");
          assertNull(list.get(0));
          assertNull(list.get(2));
          assertEquals(pk, ev.getPrimaryKey()[0]);
        } catch (Exception e) {
          throw GemFireXDRuntimeException.newRuntimeException(null, e);
        }
      }
    };
    serverExecute(2, sc);

    // check SYSTABLES entries on the server
    Statement stmt = TestUtil.jdbcConn.createStatement();
    ResultSet rs = stmt
        .executeQuery("select t.*, m.ID DSID from SYS.SYSTABLES t, SYS.MEMBERS m "
            + "where t.tablename='TESTTABLE' and m.SERVERGROUPS='SG2'");
    assertTrue(rs.next());
    assertEquals("TESTTABLE", rs.getString("TABLENAME"));
    assertEquals("PARTITION", rs.getString("DATAPOLICY"));
    // check for partition resolver
    assertEquals("PARTITION BY COLUMN ()", rs.getString("RESOLVER"));
    assertTrue(rs.getBoolean("GATEWAYENABLED"));
    assertEquals("WBCL1", rs.getString("ASYNCLISTENERS"));
    // null check for other attributes
    assertNull(rs.getString("EVICTIONATTRS"));
    assertNull(rs.getString("DISKATTRS"));
    assertNull(rs.getString("EXPIRATIONATTRS"));
    assertNull(rs.getString("LOADER"));
    assertNull(rs.getString("WRITER"));
    assertNull(rs.getString("LISTENERS"));
    assertEquals(listenerMember, rs.getObject("DSID"));
    assertFalse(rs.next());
    rs = stmt.executeQuery("select t.*, m.ID DSID from "
        + "SYS.SYSTABLES t, SYS.MEMBERS m, SYS.ASYNCEVENTLISTENERS a "
        + "where t.tablename='TESTTABLE' and groupsintersect("
        + "a.SERVER_GROUPS, m.SERVERGROUPS) and "
        + "groupsintersect(t.ASYNCLISTENERS, a.ID)");
    assertTrue(rs.next());
    assertEquals("TESTTABLE", rs.getString("TABLENAME"));
    assertEquals("PARTITION", rs.getString("DATAPOLICY"));
    // check for partition resolver
    assertEquals("PARTITION BY COLUMN ()", rs.getString("RESOLVER"));
    assertTrue(rs.getBoolean("GATEWAYENABLED"));
    assertEquals("WBCL1", rs.getString("ASYNCLISTENERS"));
    // null check for other attributes
    assertNull(rs.getString("EVICTIONATTRS"));
    assertNull(rs.getString("DISKATTRS"));
    assertNull(rs.getString("EXPIRATIONATTRS"));
    assertNull(rs.getString("LOADER"));
    assertNull(rs.getString("WRITER"));
    assertNull(rs.getString("LISTENERS"));
    assertEquals(listenerMember, rs.getObject("DSID"));
    assertFalse(rs.next());
    // check for entry for async listeners on the client
    rs = stmt
        .executeQuery("select * from SYS.SYSTABLES where tablename='TESTTABLE'");
    assertTrue(rs.next());
    assertEquals("TESTTABLE", rs.getString("TABLENAME"));
    assertEquals("PARTITION", rs.getString("DATAPOLICY"));
    // check for partition resolver
    assertEquals("PARTITION BY COLUMN ()", rs.getString("RESOLVER"));
    assertTrue(rs.getBoolean("GATEWAYENABLED"));
    assertEquals("WBCL1", rs.getString("ASYNCLISTENERS"));
    // null check for other attributes
    assertNull(rs.getString("EVICTIONATTRS"));
    assertNull(rs.getString("DISKATTRS"));
    assertNull(rs.getString("EXPIRATIONATTRS"));
    assertNull(rs.getString("LOADER"));
    assertNull(rs.getString("WRITER"));
    assertNull(rs.getString("LISTENERS"));
    assertFalse(rs.next());
  }

  public void Bug51213testAsyncEventListenerOnNonPkBasedTable() throws Exception {
    startVMs(1, 2, -1, "SG1", null);
    try {
      clientSQLExecute(
          1,
          "create table TESTTABLE (ID int  , "
              + "DESCRIPTION varchar(1024) , ADDRESS varchar(1024)) AsyncEventListener (WBCL1)");
      // now lets first do a dummy insert to create bucket on server 1
      clientSQLExecute(1,
          "insert into testtable values(114,'desc114','add114')");
      startServerVMs(1, -1, "SG2");
      Runnable runnable = getExecutorForWBCLConfiguration(
          "SG2",
          "WBCL1",
          "com.pivotal.gemfirexd.dbsync.DBSynchronizerTestBase$TestNewGatewayEventListenerNotify",
          null, null, true, Integer.valueOf(1), null, Boolean.FALSE, null,
          null, null, 100000, null, false);
      runnable.run();
      // configure the listener to collect events
      SerializableRunnable sr = new SerializableRunnable("Set Events Collector") {

        public void run() {
          AsyncEventQueue asyncQueue = Misc.getGemFireCache()
              .getAsyncEventQueue("WBCL1");
          GfxdGatewayEventListener listener = (GfxdGatewayEventListener)asyncQueue
              .getAsyncEventListener();
          TestNewGatewayEventListenerNotify tgen = (TestNewGatewayEventListenerNotify)listener
              .getAsyncEventListenerForTest();
          Event[] events = new Event[25];
          tgen.setEventsExpected(events);
        }

      };
      serverExecute(3, sr);
      Runnable startWBCL = startAsyncEventListener("WBCL1");
      clientExecute(1, startWBCL);
      // now lets first do an insert & then update
      for (int i = 1; i < 11; ++i) {
        clientSQLExecute(1, "insert into testtable values(" + i + ",'desc" + i
            + "','add" + i + "')");
      }
      for (int i = 1; i < 11; ++i) {
        clientSQLExecute(1, "update TESTTABLE set description = 'modified"
            + (i + 1) + "' where id = " + i);
      }

      for (int i = 1; i < 6; ++i) {
        clientSQLExecute(1, "Delete from TESTTABLE where id = " + i);
      }

      // validate data
      SerializableRunnable sc = new SerializableRunnable(
          "validate callback data") {
        public void run() {
          try {
            AsyncEventQueue asyncQueue = Misc.getGemFireCache()
                .getAsyncEventQueue("WBCL1");
            GfxdGatewayEventListener listener = (GfxdGatewayEventListener)asyncQueue
                .getAsyncEventListener();
            TestNewGatewayEventListenerNotify tgen = (TestNewGatewayEventListenerNotify)listener
                .getAsyncEventListenerForTest();

            while (tgen.getNumEventsProcessed() != 25) {
              Thread.sleep(1000);
            }
            Long createdPKs[] = new Long[10];
            for (int i = 0; i < 10; ++i) {
              Event ev = tgen.getEvents()[i];
              assertNotNull(ev);
              List<Object> list = ev.getNewRow();
              assertEquals(list.size(), 3);
              assertEquals(list.get(0), new Integer(i + 1));
              assertEquals(list.get(1), "desc" + (i + 1));
              assertEquals(list.get(2), "add" + (i + 1));
              createdPKs[i] = (Long)ev.getPrimaryKey()[0];
            }

            for (int i = 10; i < 20; ++i) {
              Event ev = tgen.getEvents()[i];
              assertNotNull(ev);
              List<Object> list = ev.getNewRow();
              assertEquals(list.size(), 3);
              assertEquals(list.get(1), "modified" + (i - 8));
              assertNull(list.get(0));
              assertNull(list.get(2));
              assertEquals(ev.getPrimaryKey()[0], createdPKs[i - 10]);
            }

            for (int i = 20; i < 25; ++i) {
              Event ev = tgen.getEvents()[i];
              assertNotNull(ev);
              List<Object> list = ev.getNewRow();
              assertNull(list);
              assertEquals(ev.getPrimaryKey()[0], createdPKs[i - 20]);
            }

          } catch (Exception e) {
            throw GemFireXDRuntimeException.newRuntimeException(null, e);
          }

        }

      };
      serverExecute(3, sc);
    } finally {
      clientSQLExecute(1, "drop table TESTTABLE");
    }
  }

  public void Bug51213testAsyncEventListenerOnMultiColumnPkBasedTable()
      throws Exception {
    startVMs(1, 2, -1, "SG1", null);
    try {
      clientSQLExecute(
          1,
          "create table TESTTABLE (ID int  , DESCRIPTION varchar(1024) , ADDRESS varchar(1024),"
              + " constraint pk primary key (ID, ADDRESS)) AsyncEventListener (WBCL1)");
      // now lets first do a dummy insert to create bucket on server 1
      clientSQLExecute(1,
          "insert into testtable values(114,'desc114','add114')");
      startServerVMs(1, -1, "SG2");
      Runnable runnable = getExecutorForWBCLConfiguration(
          "SG2",
          "WBCL1",
          "com.pivotal.gemfirexd.dbsync.DBSynchronizerTestBase$TestNewGatewayEventListenerNotify",
          null, null, true, Integer.valueOf(1), null, Boolean.FALSE, null,
          null, null, 100000, null, false);
      runnable.run();
      // configure the listener to collect events
      SerializableRunnable sr = new SerializableRunnable("Set Events Collector") {

        public void run() {
          AsyncEventQueue asyncQueue = Misc.getGemFireCache()
              .getAsyncEventQueue("WBCL1");
          GfxdGatewayEventListener listener = (GfxdGatewayEventListener)asyncQueue
              .getAsyncEventListener();
          TestNewGatewayEventListenerNotify tgen = (TestNewGatewayEventListenerNotify)listener
              .getAsyncEventListenerForTest();
          Event[] events = new Event[25];
          tgen.setEventsExpected(events);

        }

      };
      serverExecute(3, sr);

      Runnable startWBCL = startAsyncEventListener("WBCL1");
      clientExecute(1, startWBCL);

      // now lets first do an insert & then update
      for (int i = 1; i < 11; ++i) {
        clientSQLExecute(1, "insert into testtable values(" + i + ",'desc" + i
            + "','add" + i + "')");
      }
      for (int i = 1; i < 11; ++i) {
        clientSQLExecute(1, "update TESTTABLE set description = 'modified"
            + (i + 1) + "' where id = " + i);
      }

      for (int i = 1; i < 6; ++i) {
        clientSQLExecute(1, "Delete from TESTTABLE where id = " + i);
      }
      // validate data
      SerializableRunnable sc = new SerializableRunnable(
          "validate callback data") {

        public void run() {
          try {
            AsyncEventQueue asyncQueue = Misc.getGemFireCache()
                .getAsyncEventQueue("WBCL1");
            GfxdGatewayEventListener listener = (GfxdGatewayEventListener)asyncQueue
                .getAsyncEventListener();
            TestNewGatewayEventListenerNotify tgen = (TestNewGatewayEventListenerNotify)listener
                .getAsyncEventListenerForTest();

            while (tgen.getNumEventsProcessed() != 25) {
              getLogWriter().info(
                  "Number of events  processed  = "
                      + tgen.getNumEventsProcessed()
                      + ". Total events expected = " + tgen.getEvents().length);
              Thread.sleep(1000);

            }
            Object createdPKs[] = new Object[10];
            for (int i = 0; i < 10; ++i) {
              Event ev = tgen.getEvents()[i];
              assertNotNull(ev);
              List<Object> list = ev.getNewRow();
              assertEquals(list.size(), 3);
              assertEquals(list.get(0), new Integer(i + 1));
              assertEquals(list.get(1), "desc" + (i + 1));
              assertEquals(list.get(2), "add" + (i + 1));
              createdPKs[i] = ev.getPrimaryKey();
            }

            for (int i = 10; i < 20; ++i) {
              Event ev = tgen.getEvents()[i];
              assertNotNull(ev);
              List<Object> list = ev.getNewRow();
              assertEquals(list.size(), 3);
              assertEquals(list.get(1), "modified" + (i - 8));
              assertNull(list.get(0));
              assertNull(list.get(2));
              Object[] pkArrUpdate = ev.getPrimaryKey();
              Object[] pkArrCreate = (Object[])createdPKs[i - 10];
              assertTrue(Arrays.equals(pkArrCreate, pkArrUpdate));
            }

            for (int i = 20; i < 25; ++i) {
              Event ev = tgen.getEvents()[i];
              assertNotNull(ev);
              List<Object> list = ev.getNewRow();
              assertNull(list);
              Object[] pkArrDelete = ev.getPrimaryKey();
              Object[] pkArrCreate = (Object[])createdPKs[i - 20];
              assertTrue(Arrays.equals(pkArrCreate, pkArrDelete));
            }

          } catch (Exception e) {
            throw GemFireXDRuntimeException.newRuntimeException(null, e);
          }
        }
      };
      serverExecute(3, sc);
    } finally {
      clientSQLExecute(1, "drop table TESTTABLE");
    }
  }
  public void testSkipListenerBehaviourForAsyncEventListenerReplicate()
      throws Exception {
    this.skipListenerBehaviourForAsyncEventListener(true, false, -1);
  }

  public void testSkipListenerBehaviourForAsyncEventListenerPR()
      throws Exception {
    this.skipListenerBehaviourForAsyncEventListener(false, false, -1);
  }

  public void testSkipListenerBehaviourForAsyncEventListenerReplicateUsingNetConnection()
      throws Exception {
    this.skipListenerBehaviourForAsyncEventListener(true, true, 2726);
  }

  public void testSkipListenerBehaviourForAsyncEventListenerPRUsingNetConnection()
      throws Exception {
    this.skipListenerBehaviourForAsyncEventListener(false, true, 2727);
  }

  private void skipListenerBehaviourForAsyncEventListener(boolean useReplicate,
      boolean useNetConnection, int port) throws Exception {
    // create one accessor
    // create two data store.
    try {
      startVMs(1, 2, -1, "SG1", null);
      clientSQLExecute(
          1,
          "create table TESTTABLE (ID int not null primary key, "
              + "DESCRIPTION varchar(1024) , ADDRESS varchar(1024)) AsyncEventListener (WBCL1) "
              + (useReplicate ? " replicate " : ""));

      clientSQLExecute(1,
          "insert into testtable values(114,'desc114','add114')");
      clientSQLExecute(1,
          "insert into testtable values(115,'desc115','add115')");
      clientSQLExecute(1,
          "insert into testtable values(116,'desc116','add116')");

      if (useNetConnection) {
        TestUtil.startNetServer(port, null);
      }
      Runnable runnable = getExecutorForWBCLConfiguration(
          "SG1",
          "WBCL1",
          "com.pivotal.gemfirexd.dbsync.DBSynchronizerTestBase$TestNewGatewayEventListenerNotify",
          null, null, true, Integer.valueOf(1), null, Boolean.FALSE, null,
          null, null, 100000, null, false);
      runnable.run();
      // configure the listener to collect events
      SerializableRunnable sr = new SerializableRunnable("Set Events Collector") {

        public void run() {
          AsyncEventQueue asyncQueue = Misc.getGemFireCache()
              .getAsyncEventQueue("WBCL1");
          GfxdGatewayEventListener listener = (GfxdGatewayEventListener)asyncQueue
              .getAsyncEventListener();
          TestNewGatewayEventListenerNotify tgen = (TestNewGatewayEventListenerNotify)listener
              .getAsyncEventListenerForTest();
          Event[] events = new Event[0];
          tgen.setEventsExpected(events);
        }
      };
      serverExecute(1, sr);
      serverExecute(2, sr);
      Runnable startWBCL = startAsyncEventListener("WBCL1");
      clientExecute(1, startWBCL);
      // PK based inserts
      Properties props = new Properties();
      props.put(com.pivotal.gemfirexd.Attribute.SKIP_LISTENERS, "true");
      Connection conn = useNetConnection ? TestUtil.getNetConnection(port,
          null, props) : TestUtil.getConnection(props);
      Statement stmt = conn.createStatement();
      stmt.execute("insert into testtable values(1,'desc1','add1')");
      stmt.execute("insert into testtable values(2,'desc2','add2')");
      // PK based updates
      stmt.execute("update TESTTABLE set description = 'modified' where id = 114");

      // Bulk updates
      stmt.execute("update TESTTABLE set description = 'modified' ");

      // PK based delete
      stmt.execute("delete from  TESTTABLE where id = 114");

      // Bulk delete
      stmt.execute("delete from  TESTTABLE ");
      Thread.sleep(3000);
      // validate data
      SerializableRunnable sc = new SerializableRunnable(
          "validate callback data") {

        public void run() {
          try {
            AsyncEventQueue asyncQueue = Misc.getGemFireCache()
                .getAsyncEventQueue("WBCL1");
            GfxdGatewayEventListener listener = (GfxdGatewayEventListener)asyncQueue
                .getAsyncEventListener();
            TestNewGatewayEventListenerNotify tgen = (TestNewGatewayEventListenerNotify)listener
                .getAsyncEventListenerForTest();
            assertFalse(tgen.exceptionOccured);
            assertEquals(0, tgen.getNumCallbacks());
          }
          catch (Exception e) {
            throw GemFireXDRuntimeException.newRuntimeException(null, e);
          }
        }
      };
      serverExecute(1, sc);
      serverExecute(2, sc);
    }
    finally {
      if (useNetConnection) {
        TestUtil.stopNetServer();
      }
    }
  }
}
