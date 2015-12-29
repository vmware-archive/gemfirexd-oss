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
package com.gemstone.gemfire.internal.cache;

import java.util.*;
import junit.framework.Assert;
import junit.framework.TestCase;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionEvent;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.util.CacheWriterAdapter;
import com.gemstone.gemfire.distributed.DistributedSystem;

public class MapInterfaceJUnitTest extends TestCase {

  public MapInterfaceJUnitTest(String arg0) {
    super(arg0);
  }

  protected void setUp() throws Exception {
    super.setUp();
  }

  protected void tearDown() throws Exception {
    super.tearDown();
  }

  public void testLocalClear() {
    Properties props = new Properties();
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");
    DistributedSystem ds = DistributedSystem.connect(props);
    Cache cache = null;
    Region region = null;
    AttributesFactory factory = null;
    try {
      cache = CacheFactory.create(ds);
      factory = new AttributesFactory();
      factory.setScope(Scope.LOCAL);
      region = cache.createRegion("testingRegion", factory
          .create());
    }
    catch (Exception e) {
      fail(" failed due to " + e);
    }
    for (int i = 0; i < 100; i++) {
      region.put(new Integer(i), new Integer(i));
    }
    Assert.assertEquals(new Integer(50), region.get(new Integer(50)));
    region.localClear();
    Assert.assertEquals(null, region.get(new Integer(50)));
    region.close();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    try {
      region = cache.createRegion("testingRegion", factory
          .create());
    }
    catch (Exception e) {
      fail(" failed in creating region due to " + e);
    }
    boolean exceptionOccured = false;
    try {
      region.localClear();
    }
    catch (UnsupportedOperationException e) {
      exceptionOccured = true;
    }
    if (!exceptionOccured) {
      fail(" exception did not occur when it was supposed to occur");
    }
    region.close();
    cache.close();
    ds.disconnect();
  }

  /**
   * Make sure putAll works on Scope.LOCAL (see bug 35087)
   */
  public void testLocalPutAll() {
    Properties props = new Properties();
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");
    DistributedSystem ds = DistributedSystem.connect(props);
    Cache cache = null;
    Region region = null;
    AttributesFactory factory = null;
    try {
      cache = CacheFactory.create(ds);
      factory = new AttributesFactory();
      factory.setScope(Scope.LOCAL);
      region = cache.createRegion("testingRegion", factory
          .create());
    }
    catch (Exception e) {
      fail(" failed due to " + e);
    }
    HashMap m = new HashMap();
    m.put("aKey", "aValue");
    m.put("bKey", "bValue");
    region.putAll(m);
    assertEquals("aValue", region.get("aKey"));
    assertEquals("bValue", region.get("bKey"));
    for (int i = 0; i < 100; i++) {
      region.put(new Integer(i), new Integer(i));
    }
    Assert.assertEquals(new Integer(50), region.get(new Integer(50)));
    region.localClear();
    Assert.assertEquals(null, region.get(new Integer(50)));
    region.close();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    try {
      region = cache.createRegion("testingRegion", factory
          .create());
    }
    catch (Exception e) {
      fail(" failed in creating region due to " + e);
    }
    boolean exceptionOccured = false;
    try {
      region.localClear();
    }
    catch (UnsupportedOperationException e) {
      exceptionOccured = true;
    }
    if (!exceptionOccured) {
      fail(" exception did not occur when it was supposed to occur");
    }
    region.close();
    cache.close();
    ds.disconnect();
  }
 
  protected boolean hasBeenNotified = false;

  public void testBeforeRegionClearCallBack() {
   
    Properties props = new Properties();
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");
    DistributedSystem ds = DistributedSystem.connect(props);
    Cache cache = null;
    Region region = null;
    AttributesFactory factory = null;
    try {
      cache = CacheFactory.create(ds);
      factory = new AttributesFactory();
      factory.setScope(Scope.LOCAL);
      factory.setCacheWriter(new CacheWriterAdapter() {

        public void beforeRegionClear(RegionEvent event) throws CacheWriterException {
          synchronized (this) {
            this.notify();
            MapInterfaceJUnitTest.this.hasBeenNotified = true;
          }
        }
      });
      region = cache.createRegion("testingRegion", factory
          .create());
      DoesClear doesClear = new DoesClear(region);
      new Thread(doesClear).start();
      synchronized (this) {
        if (!this.hasBeenNotified) {
          this.wait(3000);
        }
      }
      if (!this.hasBeenNotified) {
        fail(" beforeRegionClear call back did not come");
      }
    }
    catch (Exception e) {
      fail(" failed due to " + e);
    }
    for (int i = 0; i < 100; i++) {
      region.put(new Integer(i), new Integer(i));
    }
    Assert.assertEquals(new Integer(50), region.get(new Integer(50)));
    region.localClear();
    Assert.assertEquals(null, region.get(new Integer(50)));
    region.close();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    try {
      region = cache.createRegion("testingRegion", factory
          .create());
    }
    catch (Exception e) {
      fail(" failed in creating region due to " + e);
    }
    boolean exceptionOccured = false;
    try {
      region.localClear();
    }
    catch (UnsupportedOperationException e) {
      exceptionOccured = true;
    }
    if (!exceptionOccured) {
      fail(" exception did not occur when it was supposed to occur");
    }
    region.close();
    cache.close();
    ds.disconnect();
  }

  protected Region region2 = null; 
  protected int counter = 0;
  public void testSetValue() {

    Properties props = new Properties();
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");
    DistributedSystem ds = DistributedSystem.connect(props);
    Cache cache = null;
  
    AttributesFactory factory = null;
    try {
      cache = CacheFactory.create(ds);
      factory = new AttributesFactory();
      factory.setScope(Scope.LOCAL);
      factory.setCacheWriter(new CacheWriterAdapter() {

        public void beforeUpdate(EntryEvent event) throws CacheWriterException {
          synchronized (this) {
            this.notify();
            counter++;
            MapInterfaceJUnitTest.this.hasBeenNotified = true;
          }
        }
        
      });
      region2 = cache.createRegion("testingRegion", factory
          .create());
      region2.put(new Integer(2),new Integer(2));
      this.hasBeenNotified = false;
      DoesPut doesPut = new DoesPut();
      new Thread(doesPut).start();
      synchronized (this) {
        if (!this.hasBeenNotified) {
          this.wait(3000);
        }
      }
      if (!this.hasBeenNotified) {
        fail(" beforeCreate call back did not come");
      }
      
      Assert.assertEquals(counter,1);
    }
    catch (Exception e) {
      fail(" failed due to " + e);
    }
   
  }
  
  class DoesClear implements Runnable {

    private Region region;

    DoesClear(Region reg) {
      this.region = reg;
    }

    public void run() {
      this.region.clear();
    }
  }
  
  class DoesPut implements Runnable {

  
    DoesPut() {
    }

    public void run() {
     ((Map.Entry)(MapInterfaceJUnitTest.this.region2.entrySet().iterator().next())).setValue(new Integer(8));
    }
  }
}
