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
package com.gemstone.gemfire.cache.snapshot;

import java.io.File;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicInteger;

import com.examples.snapshot.MyObject;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEvent;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventListener;
import com.gemstone.gemfire.cache.snapshot.SnapshotOptions.SnapshotFormat;

public class WanSnapshotJUnitTest extends SnapshotTestCase {
  private Region<Integer, MyObject> region;
  private WanListener wan;
  private static final long MAX_WAIT = 5 * 60 * 1000; //6 minutes
  
  public void testWanCallback() throws Exception {
    int count = 1000;
    for (int i = 0; i < count; i++) {
      region.put(i, new MyObject(i, "clienttest " + i));
    }

    File snapshot = new File("wan.snapshot");
    region.getSnapshotService().save(snapshot, SnapshotFormat.GEMFIRE);
    region.clear();
    
    long start = System.currentTimeMillis();
    // wait for the events to drain out
    while (!wan.ticker.compareAndSet(count, 0)) {
      Thread.sleep(100);
      if(System.currentTimeMillis() - start > MAX_WAIT) {
        fail("Event did not drain in 5 minutes");
      }
    }

    region.getSnapshotService().load(snapshot, SnapshotFormat.GEMFIRE);

    // delay, just in case we get any events
    Thread.sleep(1000);
    
    assertEquals("WAN callback detected during import", 0, wan.ticker.get());
    assertEquals(count, region.size());
  }
  
  public void setUp() throws Exception {
    super.setUp();
    
    wan = new WanListener();
    cache.createAsyncEventQueueFactory().setBatchSize(1).create("wanqueue", wan);
    region = cache.<Integer, MyObject>createRegionFactory(RegionShortcut.REPLICATE)
        .addAsyncEventQueueId("wanqueue").create("test");
  }
  
  private class WanListener implements AsyncEventListener<Object, Object> {
    private final AtomicInteger ticker = new AtomicInteger(0);
    
    @Override
    public void close() {
    }

    @Override
    public boolean processEvents(List<AsyncEvent<Object, Object>> events) {
      ticker.incrementAndGet();
      return true;
    }
  }

}

