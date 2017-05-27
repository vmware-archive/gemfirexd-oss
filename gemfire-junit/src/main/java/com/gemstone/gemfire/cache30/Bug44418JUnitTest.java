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
/**
 * 
 */
package com.gemstone.gemfire.cache30;

import java.util.Properties;

import junit.framework.TestCase;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CustomExpiry;
import com.gemstone.gemfire.cache.ExpirationAttributes;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Region.Entry;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.cache.LocalRegion;

import io.snappydata.test.dunit.DistributedTestBase;
import io.snappydata.test.dunit.DistributedTestBase.WaitCriterion;

/**
 * Test for Bug 44418.
 * 
 * @author darrel
 * @since 7.0
 */
public class Bug44418JUnitTest extends TestCase {

  DistributedSystem ds;
  Cache cache;

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public void testPut() {

    System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
    try {
      final Region r = this.cache.createRegionFactory(RegionShortcut.LOCAL)
      .setStatisticsEnabled(true)
      .setCustomEntryTimeToLive(new CustomExpiry() {
        @Override
        public void close() {
        }
        @Override
        public ExpirationAttributes getExpiry(Entry entry) {
          ExpirationAttributes result;
          if (entry.getValue().equals("longExpire")) {
            result = new ExpirationAttributes(5000);
          } else {
            result = new ExpirationAttributes(1);
          }
          //Bug44418JUnitTest.this.cache.getLogger().info("in getExpiry result=" + result, new RuntimeException("STACK"));
          return result;
        }
      })
      .create("bug44418");
      r.put("key", "longExpire");
      // should take 5000 ms to expire.
      // Now update it with a short expire time
      r.put("key", "quickExpire");
      // now wait to see it expire. We only wait
      // 1000 ms. If we need to wait that long
      // for a 1 ms expire then the expiration
      // is probably still set at 5000 ms.
      DistributedTestBase.waitForCriterion(new WaitCriterion() {
        @Override
        public boolean done() {
          // If the value is gone then we expired and are done
          return !r.containsValueForKey("key");
        }

        @Override
        public String description() {
          return "key=" + r.get("key");
        }
      }, 1000, 10, false);

      if (r.containsValueForKey("key")) {
        fail("1 ms expire did not happen after waiting 1000 ms");
      }
    } finally {
      System.getProperties().remove(LocalRegion.EXPIRY_MS_PROPERTY);
    }
  }
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public void testGet() {

    System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
    try {
      final Region r = this.cache.createRegionFactory(RegionShortcut.LOCAL)
      .setStatisticsEnabled(true)
      .setCustomEntryIdleTimeout(new CustomExpiry() {
        private boolean secondTime;
        @Override
        public void close() {
        }
        @Override
        public ExpirationAttributes getExpiry(Entry entry) {
          ExpirationAttributes result;
          if (!this.secondTime) {
            result = new ExpirationAttributes(5000);
            this.secondTime = true;
          } else {
            result = new ExpirationAttributes(1);
          }
          Bug44418JUnitTest.this.cache.getLogger().info("in getExpiry result=" + result, new RuntimeException("STACK"));
          return result;
        }
      })
      .create("bug44418");
      r.put("key", "longExpire");
      // should take 5000 ms to expire.
      r.get("key");
      // now wait to see it expire. We only wait
      // 1000 ms. If we need to wait that long
      // for a 1 ms expire then the expiration
      // is probably still set at 5000 ms.
      DistributedTestBase.waitForCriterion(new WaitCriterion() {
        @Override
        public boolean done() {
          // If the value is gone then we expired and are done
          return !r.containsValueForKey("key");
        }

        @Override
        public String description() {
          return "key=" + r.get("key");
        }
      }, 1000, 10, false);

      if (r.containsValueForKey("key")) {
        fail("1 ms expire did not happen after waiting 1000 ms");
      }
    } finally {
      System.getProperties().remove(LocalRegion.EXPIRY_MS_PROPERTY);
    }
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
    if (this.cache != null) {
      this.cache.close();
      this.cache = null;
    }
    if (this.ds != null) {
      this.ds.disconnect();
      this.ds = null;
    }
  }

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    Properties props = new Properties();
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");
    this.ds = DistributedSystem.connect(props);
    this.cache = CacheFactory.create(this.ds);
  }

}
