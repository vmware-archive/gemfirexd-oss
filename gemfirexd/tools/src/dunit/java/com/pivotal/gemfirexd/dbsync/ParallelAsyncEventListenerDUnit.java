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

import java.util.List;

import com.gemstone.gemfire.cache.asyncqueue.AsyncEventQueue;
import com.gemstone.gemfire.internal.AvailablePort;
import com.pivotal.gemfirexd.callbacks.Event;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.ddl.wan.GfxdGatewayEventListener;
import com.pivotal.gemfirexd.internal.engine.jdbc.GemFireXDRuntimeException;
import com.pivotal.gemfirexd.internal.engine.store.GemFireStore;

import io.snappydata.test.dunit.SerializableCallable;
import io.snappydata.test.dunit.SerializableRunnable;

public class ParallelAsyncEventListenerDUnit extends DBSynchronizerTestBase {

  public ParallelAsyncEventListenerDUnit(String name) {
    super(name);
  }

  public void testDummy() {
  }

  public void DISABLED_testParallelAsyncEventListener() throws Exception {
    startVMs(1, 1, -1, "SGRP", null);
    clientSQLExecute(
        1,
        "create table PARTTABLE (ID int not null , "
            + "DESCRIPTION varchar(1024) , ADDRESS varchar(1024)) AsyncEventListener (PAELISTENER)");
    clientSQLExecute(1, "insert into PARTTABLE values(114,'desc114','add114')");
    startServerVMs(1, -1, "SGRP");
    Runnable runnable = getExecutorForWBCLConfiguration(
        "SGRP",
        "PAELISTENER",
        "com.pivotal.gemfirexd.dbsync.DBSynchronizerTestBase$TestNewGatewayEventListenerNotify",
        //"org.apache.derby.jdbc.EmbeddedDriver", "jdbc:derby:newDB;create=true",
        "","",
        true, Integer.valueOf(1), null, Boolean.FALSE, null, null, null,
        100000, null,true);
    runnable.run();
    // configure the listener to collect events
    SerializableCallable sr = new SerializableCallable("Set Events Collector") {

      public Object call() {
        AsyncEventQueue asyncQueue = Misc.getGemFireCache().getAsyncEventQueue(
            "PAELISTENER");
        GfxdGatewayEventListener listener = (GfxdGatewayEventListener)asyncQueue
            .getAsyncEventListener();
        TestNewGatewayEventListenerNotify tgen = (TestNewGatewayEventListenerNotify)listener
            .getAsyncEventListenerForTest();
        Event[] events = new Event[2];
        tgen.setEventsExpected(events);
        return GemFireStore.getMyId().toString();
      }

    };
    serverExecute(2, sr);
    Runnable startWBCL = startAsyncEventListener("PAELISTENER");
    clientExecute(1, startWBCL);
    // now lets first do an insert & then update
    clientSQLExecute(1, "insert into PARTTABLE values(1,'desc1','add1')");
    clientSQLExecute(1,
        "update PARTTABLE set description = 'modified' where ID =1");
       // "update PARTTABLE set description = 'modified' where ADDRESS ='add1'");
    // validate data
    SerializableRunnable sc = new SerializableRunnable("validate callback data") {

      public void run() {
        try {
          AsyncEventQueue asyncQueue = Misc.getGemFireCache()
              .getAsyncEventQueue("PAELISTENER");
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
        }
        catch (Exception e) {
          throw GemFireXDRuntimeException.newRuntimeException(null, e);
        }
      }
    };
    serverExecute(2, sc);
    clientSQLExecute(1, "drop table PARTTABLE");
  }
  
//  @Override
//  protected void vmTearDown() throws Exception {
//    //yogesh: handle this properly
//  }
//  @Override
//  public void setUp() throws Exception {
//    //yogesh: handle this properly
//    super.setUp();
//    netPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
//  }
  
}
