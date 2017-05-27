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

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.naming.NamingException;
import javax.transaction.UserTransaction;

import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;

import junit.framework.*;

/**
 * Ensure that the ignoreJTA Region setting works
 *
 * @author Bruce Schuchardt
 * @since 4.1.1
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class JtaNoninvolvementTest extends TestCase {

  private Cache cache;
  private Region nonTxRegion;
  private Region txRegion;

  public JtaNoninvolvementTest(String name) {
    super(name);
  }

  ////////  Test methods


  private void createCache(boolean copyOnRead) throws CacheException {
    Properties p = new Properties();
    p.setProperty("mcast-port", "0"); // loner
    this.cache = CacheFactory.create(DistributedSystem.connect(p));

    AttributesFactory af = new AttributesFactory();
    af.setScope(Scope.LOCAL);
    af.setIgnoreJTA(true);
    this.nonTxRegion = this.cache.createRegion("JtaNoninvolvementTest", af.create());
    af.setIgnoreJTA(false);
    this.txRegion = this.cache.createRegion("JtaInvolvementTest", af.create());
  }

  private void closeCache() throws CacheException {
    if (this.cache != null) {
      this.txRegion = null;
      this.nonTxRegion = null;
      Cache c = this.cache;
      this.cache = null;
      c.close();
    }
  }
  
  public void testDummy() throws CacheException {

  }

  public void test000Noninvolvement() throws Exception {
    try {
      if (cache == null) {
        createCache(false);
      }
      javax.transaction.UserTransaction ut =
        (javax.transaction.UserTransaction)cache.getJNDIContext()
          .lookup("java:/UserTransaction");
      {
        ut.begin();
        txRegion.put("transactionalPut", "xxx");
        assertTrue("expect cache to be in a transaction",
            cache.getCacheTransactionManager().exists());
        ut.commit();
      }
      Assert.assertFalse("ensure there is no transaction before testing non-involvement", 
          cache.getCacheTransactionManager().exists());
      {
        ut.begin();
        nonTxRegion.put("nontransactionalPut", "xxx");
        Assert.assertFalse("expect cache to not be in a transaction",
          cache.getCacheTransactionManager().exists());
        ut.commit();
      }
    }
    
    finally {
      closeCache();
      cache = null;
    }
  }

  public void test001NoninvolvementMultipleRegions_bug45541() throws Exception {
    javax.transaction.UserTransaction ut = null;
    try {
      if (cache == null) {
        createCache(false);
      }
      final CountDownLatch l = new CountDownLatch(1);
      final AtomicBoolean exceptionOccured = new AtomicBoolean(false);
      ut = 
          (UserTransaction) cache.getJNDIContext().lookup("java:/UserTransaction");
      ut.begin();
      txRegion.put("key", "value");
      nonTxRegion.put("key", "value");
      Thread t = new Thread(new Runnable() {
        @Override
        public void run() {
          if (txRegion.get("key") != null) {
            exceptionOccured.set(true);
          }
          if (nonTxRegion.get("key") != null) {
            exceptionOccured.set(true);
          }
          l.countDown();
        }
      });
      t.start();
      l.await();
      assertFalse(exceptionOccured.get());
    } finally {
      if (ut != null) {
        ut.commit();
      }
      closeCache();
      cache = null;
    }
  }
}
