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

import java.util.Properties;

import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;

import javax.transaction.RollbackException;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.AttributesMutator;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CacheTransactionManager;
import com.gemstone.gemfire.cache.CacheWriter;
import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.IllegalTransactionStateException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionEvent;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.SynchronizationCommitConflictException;
import com.gemstone.gemfire.cache.TransactionEvent;
import com.gemstone.gemfire.cache.TransactionException;
import com.gemstone.gemfire.cache.TransactionListener;
import com.gemstone.gemfire.cache.TransactionWriter;
import com.gemstone.gemfire.cache.TransactionWriterException;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;

import junit.framework.*;

@SuppressWarnings({ "unchecked", "rawtypes" })
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TXWriterTest extends TestCase {

  protected int cbCount;
  protected TransactionEvent te;
  protected int listenerAfterCommit;
  protected int listenerAfterFailedCommit;
  protected int listenerAfterRollback;
  protected int listenerClose;

  protected int failedCommits = 0;
  protected int afterCommits = 0;
  protected int afterRollbacks = 0;

  private GemFireCacheImpl cache;
  private CacheTransactionManager txMgr;
  protected Region region;

  public TXWriterTest(String name) {
    super(name);
  }

  public static void main(String[] args) {
    junit.textui.TestRunner.run(new TestSuite(TXWriterTest.class));
  }

  ////////  Test methods

  private void createCache() throws CacheException {
    Properties p = new Properties();
    p.setProperty("mcast-port", "0"); // loner
    this.cache = (GemFireCacheImpl)new CacheFactory(p).create();
    AttributesFactory af = new AttributesFactory();
    af.setScope(Scope.DISTRIBUTED_NO_ACK);
    af.setIndexMaintenanceSynchronous(true);
    this.region = this.cache.createRegion("TXTest", af.create());
    this.txMgr = this.cache.getCacheTransactionManager();
    this.listenerAfterCommit = 0;
    this.listenerAfterFailedCommit = 0;
    this.listenerAfterRollback = 0;
    this.listenerClose = 0;
  }

  private void closeCache() {
    if (this.cache != null) {
      if (this.txMgr != null) {
        try {
          this.txMgr.rollback();
        } catch (IllegalTransactionStateException ignore) {
        }
      }
      this.region = null;
      this.txMgr = null;
      Cache c = this.cache;
      this.cache = null;
      c.close();
    }
  }

  @Override
  public void setUp() throws Exception {
    //(new RuntimeException("STACKTRACE: " + getName())).printStackTrace();
    createCache();
  }

  @Override
  public void tearDown() throws Exception {
    try {
    closeCache();
    } catch(Throwable t) {
      // IGNORE THIS!!!
    }
    //(new RuntimeException("STACKTRACE: " + getName())).printStackTrace();
  }

  /**
   * Make sure standard Cache(Listener,Writer) are not called during rollback
   * due to transaction writer throw.
   * 
   * Also check that CacheWriterException allows continuing the transaction
   * without affecting other entries.
   */
  public void test000NoCallbacksOnTransactionWriterThrow() throws CacheException {
    final Object cwThrowKey = "keyCW";
    // install listeners
    try {
      AttributesMutator mutator = this.region.getAttributesMutator();
      mutator.addCacheListener(new CacheListenerAdapter() {
          @Override
          public void close() {cbCount++;}
          @Override
          public void afterCreate(EntryEvent event) {cbCount++;}
          @Override
          public void afterUpdate(EntryEvent event) {cbCount++;}
          @Override
          public void afterInvalidate(EntryEvent event) {cbCount++;}
          @Override
          public void afterDestroy(EntryEvent event) {cbCount++;}
          @Override
          public void afterRegionInvalidate(RegionEvent event) {cbCount++;}
          @Override
          public void afterRegionDestroy(RegionEvent event) {cbCount++;}
        });

      mutator.setCacheWriter(new CacheWriter() {

        public void close() {
          cbCount++;
        }

        public void beforeUpdate(EntryEvent event) throws CacheWriterException {
          cbCount++;
          if (event.getKey().equals(cwThrowKey)) {
            throw new CacheWriterException("failing...");
          }
        }

        public void beforeCreate(EntryEvent event) throws CacheWriterException {
          cbCount++;
          if (event.getKey().equals(cwThrowKey)) {
            throw new CacheWriterException("failing...");
          }
        }

        public void beforeDestroy(EntryEvent event) throws CacheWriterException {
          cbCount++;
          if (event.getKey().equals(cwThrowKey)) {
            throw new CacheWriterException("failing...");
          }
        }

        public void beforeRegionDestroy(RegionEvent event)
            throws CacheWriterException {
          cbCount++;
        }

        public void beforeRegionClear(RegionEvent event)
            throws CacheWriterException {
          cbCount++;
        }
      });

      this.txMgr.setWriter(new TransactionWriter() {
        public void beforeCommit(TransactionEvent event) throws TransactionWriterException {
          throw new TransactionWriterException("Rollback now!");
        }
        public void close() {}
      });

      this.txMgr.begin();
      this.region.create("key1", "value1");
      this.cbCount = 0;
      try {
        this.txMgr.commit();
        fail("Commit should have thrown TransactionException");
      } catch (TransactionException te) {
        assertNotNull(te.getCause());
        assertTrue(te.getCause() instanceof TransactionWriterException);
      }
      assertEquals(0, this.cbCount);

      this.cbCount = 0;
      this.region.create("key1", "value1");
      // do a santity check to make sure callbacks are installed
      assertEquals(2, this.cbCount); // 2 -> 1writer + 1listener

      this.txMgr.begin();
      this.cbCount = 0;
      this.region.put("key1", "value2");
      try {
        this.txMgr.commit();
        fail("Commit should have thrown TransactionException");
      } catch (TransactionException te) {
        assertNotNull(te.getCause());
        assertTrue(te.getCause() instanceof TransactionWriterException);
      }
      assertEquals(1, this.cbCount);
      this.region.localDestroy("key1");

      this.region.create("key1", "value1");
      this.txMgr.begin();
      this.region.localDestroy("key1");
      this.cbCount = 0;
      try {
        this.txMgr.commit();
        fail("Commit should have thrown TransactionException");
      } catch (TransactionException te) {
        assertNotNull(te.getCause());
        assertTrue(te.getCause() instanceof TransactionWriterException);
      }
      assertEquals(0, this.cbCount);
  
      this.region.put("key1", "value1");
      this.txMgr.begin();
      this.cbCount = 0;
      this.region.destroy("key1");
      try {
        this.txMgr.commit();
        fail("Commit should have thrown TransactionException");
      } catch (TransactionException te) {
        assertNotNull(te.getCause());
        assertTrue(te.getCause() instanceof TransactionWriterException);
      }
      assertEquals(1, this.cbCount);
  
      this.region.put("key1", "value1");
      this.txMgr.begin();
      this.cbCount = 0;
      this.region.localInvalidate("key1");
      try {
        this.txMgr.commit();
        fail("Commit should have thrown TransactionException");
      } catch (TransactionException te) {
        assertNotNull(te.getCause());
        assertTrue(te.getCause() instanceof TransactionWriterException);
      }
      assertEquals(0, this.cbCount);
      this.region.localDestroy("key1");
  
      this.region.put("key1", "value1");
      this.txMgr.begin();
      this.cbCount = 0;
      this.region.invalidate("key1");
      try {
        this.txMgr.commit();
        fail("Commit should have thrown TransactionException");
      } catch (TransactionException te) {
        assertNotNull(te.getCause());
        assertTrue(te.getCause() instanceof TransactionWriterException);
      }
      assertEquals(0, this.cbCount);

      this.region.localDestroy("key1");
    } finally {
      this.txMgr.setWriter(null);
    }

    // check the CacheWriterException does not affect other keys in the
    // transaction
    this.txMgr.begin();
    this.region.put("key1", "value1");
    try {
      this.region.put(cwThrowKey, "value");
      fail("expected CacheWriterException");
    } catch (CacheWriterException cwe) {
      // ignore
    }
    assertEquals("value1", this.region.get("key1"));
    assertNull(this.region.get(cwThrowKey));
    this.txMgr.commit();
    assertEquals("value1", this.region.get("key1"));
    assertNull(this.region.get(cwThrowKey));
  }

  // make sure standard Cache(Listener,Writer)
  // are not called during rollback due to transaction writer throw
  public void test001AfterCommitFailedOnTransactionWriterThrow() throws CacheException {
    // install listeners
    try {
      this.txMgr.setWriter(new TransactionWriter() {
        public void beforeCommit(TransactionEvent event) throws TransactionWriterException {
          throw new TransactionWriterException("Rollback now!");
        }
        public void close() {}
      });

      this.txMgr.addListener(new TransactionListener() {
        public void afterFailedCommit(TransactionEvent event) {
          failedCommits++;
        }
        public void afterCommit(TransactionEvent event) {
          afterCommits++;
        }
        public void afterRollback(TransactionEvent event) {
          afterRollbacks++;
        }
        public void close() {}
      });
      
      
      
      this.txMgr.begin();
      this.region.create("key1", "value1");
      this.cbCount = 0;
      try {
        this.txMgr.commit();
        fail("Commit should have thrown TransactionException");
      } catch (TransactionException te) {
        assertNotNull(te.getCause());
        assertTrue(te.getCause() instanceof TransactionWriterException);
      }
      assertEquals(0, this.cbCount);
      assertEquals(1, this.failedCommits);
      assertEquals(0, this.afterCommits);
      assertEquals(0, this.afterRollbacks);
  
    } finally {
      this.txMgr.setWriter(null);
      this.txMgr.initListeners(null);
    }
  }

  // make sure standard Cache(Listener,Writer)
  // are not called during rollback due to transaction writer throw
  public void test002AfterCommitFailedOnTransactionWriterThrowWithJTA() throws Exception {
    // install listeners
    try {
      this.txMgr.setWriter(new TransactionWriter() {
        public void beforeCommit(TransactionEvent event) throws TransactionWriterException {
          throw new TransactionWriterException("Rollback now!");
        }
        public void close() {}
      });

      this.txMgr.addListener(new TransactionListener() {
        public void afterFailedCommit(TransactionEvent event) {
          failedCommits++;
        }
        public void afterCommit(TransactionEvent event) {
          afterCommits++;
        }
        public void afterRollback(TransactionEvent event) {
          afterRollbacks++;
        }
        public void close() {}
      });
      
      javax.transaction.UserTransaction userTx = null;
      try {
        userTx = 
          (javax.transaction.UserTransaction) this.cache.getJNDIContext().lookup("java:/UserTransaction");
      } 
      catch (VirtualMachineError e) {
        SystemFailure.initiateFailure(e);
        throw e;
      }
      catch (Throwable badDog) {
        fail("Expected to get a healthy UserTransaction!");
      }
      
      
//      this.txMgr.begin();
      userTx.begin();
      this.region.create("key1", "value1");
      this.cbCount = 0;
      try {
  //      this.txMgr.commit();
        userTx.commit();
        fail("Commit should have thrown RollbackException");
      } catch(RollbackException re) {
        Throwable cause = re.getCause();
        assertNotNull(cause);
        assertTrue(cause instanceof SynchronizationCommitConflictException);
        while (cause.getCause() != null) {
          cause = cause.getCause();
        }
        assertTrue(cause instanceof TransactionWriterException);
      }
      assertEquals(0, this.cbCount);
      assertEquals(1, this.failedCommits);
      assertEquals(0, this.afterCommits);
      assertEquals(0, this.afterRollbacks);

    } finally {
      this.txMgr.setWriter(null);
      this.txMgr.initListeners(null);
    }
  }

  public void test003AfterCommitFailedOnThrowNPE() throws CacheException {
    // install listeners
    try {
      this.txMgr.setWriter(new TransactionWriter() {
        public void beforeCommit(TransactionEvent event) throws TransactionWriterException {
          throw new NullPointerException("this is expected!");
        }
        public void close() {}
      });

      this.txMgr.addListener(new TransactionListener() {
        public void afterFailedCommit(TransactionEvent event) {
          failedCommits++;
        }
        public void afterCommit(TransactionEvent event) {
          afterCommits++;
        }
        public void afterRollback(TransactionEvent event) {
          afterRollbacks++;
        }
        public void close() {}
      });

      this.txMgr.begin();
      this.region.create("key1", "value1");
      this.cbCount = 0;
      try {
        this.txMgr.commit();
        fail("Commit should have thrown TransactionException");
      } catch (TransactionException te) {
        assertNotNull(te.getCause());
        assertTrue(te.getCause() instanceof NullPointerException);
      }
      assertEquals(0, this.cbCount);
      assertEquals(1, this.failedCommits);
      assertEquals(0, this.afterCommits);
      assertEquals(0, this.afterRollbacks);
  
    } finally {
      this.txMgr.setWriter(null);
      this.txMgr.initListeners(null);
    }
  }

  public void test004AfterCommitFailedOnThrowOOM() throws Throwable {
    // install listeners
    try {
      this.txMgr.setWriter(new TransactionWriter() {
        public void beforeCommit(TransactionEvent event) throws TransactionWriterException {
          throw new OutOfMemoryError("this is expected!");
        }
        public void close() {}
      });

      this.txMgr.addListener(new TransactionListener() {
        public void afterFailedCommit(TransactionEvent event) {
          failedCommits++;
        }
        public void afterCommit(TransactionEvent event) {
          afterCommits++;
        }
        public void afterRollback(TransactionEvent event) {
          afterRollbacks++;
        }
        public void close() {}
      });

      this.txMgr.begin();
      this.region.create("key1", "value1");
      this.cbCount = 0;
      try {
        this.txMgr.commit();
        fail("Commit should have thrown OOME");
      } catch(OutOfMemoryError oome) {
        // this is what we expect
      }
      assertEquals(0, this.cbCount);
      assertEquals(0, this.failedCommits);
      assertEquals(0, this.afterCommits);
      assertEquals(0, this.afterRollbacks);
    } finally {
      this.txMgr.setWriter(null);
      this.txMgr.initListeners(null);
    }
    //System.out.println("happytest");
  }
}
