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
package com.gemstone.gemfire;

import com.gemstone.gemfire.internal.*;
import com.gemstone.gemfire.distributed.*;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.cache.*;
import junit.framework.*;
import java.util.Properties;
import java.net.*;

/**
 * This class makes sure an isolated "loner" distribution manager
 * can be created and do some cache functions.
 */
public class LonerDMTest extends TestCase {

  public LonerDMTest(String name) {
    super(name);
  }

  ////////  Test life cycle methods

  @Override
  public void setUp() {
  }

  @Override
  public void tearDown() {
  }

  public void testLoner() throws CacheException {
    long start;
    long end;
    DistributedSystem ds = null;
    Cache c = null;
    Properties cfg = new Properties();
    cfg.setProperty("mcast-port", "0");
    cfg.setProperty("locators", "");
    cfg.setProperty("statistic-sampling-enabled", "false");

    for (int i=0; i < 2; i++) {
      start = System.currentTimeMillis();
      ds = DistributedSystem.connect(cfg);
      end = System.currentTimeMillis();
      System.out.println("ds.connect took    " + (end -start) + " ms");
      try {

        start = System.currentTimeMillis();
        c = CacheFactory.create(ds);
        end = System.currentTimeMillis();
        System.out.println("Cache create took " + (end -start) + " ms");

        try {
          AttributesFactory af = new AttributesFactory();
          af.setScope(Scope.GLOBAL);
          Region r = c.createRegion("loner", af.create());
          r.put("key1", "value1");
          r.get("key1");
          r.get("key2");
          r.invalidate("key1");
          r.destroy("key1");
          r.destroyRegion();
        } finally {
//          if (c != null) 
          {
            start = System.currentTimeMillis();
            c.close();
            end = System.currentTimeMillis();
            System.out.println("Cache close took " + (end -start) + " ms");
//            c = null; dead store
          }
        }
      } finally {
        if (ds != null) {
          start = System.currentTimeMillis();
          ds.disconnect();
          end = System.currentTimeMillis();
          System.out.println("ds.disconnect took " + (end -start) + " ms");
        }
        ds = null;
      }
    }
  }
  public void testLonerWithStats() throws CacheException {
    long start;
    long end;
    DistributedSystem ds = null;
    Cache c = null;
    Properties cfg = new Properties();
    cfg.setProperty("mcast-port", "0");
    cfg.setProperty("locators", "");
    cfg.setProperty("statistic-sampling-enabled", "true");
    cfg.setProperty("statistic-archive-file", "lonerStats.gfs");

    for (int i=0; i < 1; i++) {
      start = System.currentTimeMillis();
      ds = DistributedSystem.connect(cfg);
      end = System.currentTimeMillis();
      System.out.println("ds.connect took    " + (end -start) + " ms");
      try {

        start = System.currentTimeMillis();
        c = CacheFactory.create(ds);
        end = System.currentTimeMillis();
        System.out.println("Cache create took " + (end -start) + " ms");

        try {
          AttributesFactory af = new AttributesFactory();
          af.setScope(Scope.GLOBAL);
          Region r = c.createRegion("loner", af.create());
          r.put("key1", "value1");
          r.get("key1");
          r.get("key2");
          r.invalidate("key1");
          r.destroy("key1");
          r.destroyRegion();
        } finally {
//          if (c != null) 
          {
            start = System.currentTimeMillis();
            c.close();
            end = System.currentTimeMillis();
            System.out.println("Cache close took " + (end -start) + " ms");
//            c = null; dead store
          }
        }
      } finally {
        if (ds != null) {
          start = System.currentTimeMillis();
          ds.disconnect();
          end = System.currentTimeMillis();
          System.out.println("ds.disconnect took " + (end -start) + " ms");
        }
        ds = null;
      }
    }
  }
  
  public void testMemberId() throws UnknownHostException {
    String host = SocketCreator.getLocalHost().getCanonicalHostName();
    String name = "Foo";

    Properties cfg = new Properties();
    cfg.setProperty("mcast-port", "0");
    cfg.setProperty("locators", "");
    cfg.setProperty(DistributionConfig.NAME_NAME, name);
    DistributedSystem ds = DistributedSystem.connect(cfg);
    System.out.println("MemberId = " + ds.getMemberId());
    assertEquals(host.toString(), ds.getDistributedMember().getHost());
    assertEquals(OSProcess.getId(), ds.getDistributedMember().getProcessId());
    if(!PureJavaMode.isPure()) {
      String pid = String.valueOf(OSProcess.getId());
      assertTrue(ds.getMemberId().indexOf(pid) > -1);
    }
    assertTrue(ds.getMemberId().indexOf(name) > -1);
    String memberid = ds.getMemberId();
    String shortname = shortName(host);
    assertTrue("'" + memberid + "' does not contain '" + shortname + "'",
               memberid.indexOf(shortname) > -1);
  }

  private String shortName(String hostname) {
    assertNotNull(hostname);
    int index = hostname.indexOf('.');

    if (index > 0 && !Character.isDigit(hostname.charAt(0)))
      return hostname.substring(0, index);
    else
      return hostname;
  }
  
}
