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
package com.gemstone.gemfire.distributed.internal.locks;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.cache.locks.ExclusiveSharedSynchronizer;
import com.gemstone.gemfire.internal.cache.locks.LockMode;
import com.gemstone.gemfire.internal.cache.locks.LockingPolicy;
import com.gemstone.gemfire.internal.cache.locks.NonReentrantLock;
import com.gemstone.gemfire.internal.cache.locks.NonReentrantReadWriteLock;
import com.gemstone.gemfire.internal.cache.locks.QueuedSynchronizer;
import com.gemstone.gemfire.internal.cache.locks.ReentrantReadWriteWriteShareLock;

import junit.framework.TestCase;

public class LocalLockingTest extends TestCase {

  // dummy class just to expose the various statics
  @SuppressWarnings("serial")
  static final class DummySync extends ExclusiveSharedSynchronizer {

    static final int MAX_READ_SHARED_COUNT =
      ExclusiveSharedSynchronizer.MAX_READ_SHARED_COUNT;

    static final int MAX_READ_ONLY_COUNT =
      ExclusiveSharedSynchronizer.MAX_READ_ONLY_COUNT;

    static final int MAX_WRITE_COUNT =
      ExclusiveSharedSynchronizer.MAX_WRITE_COUNT;

    @Override
    public Object getOwnerId(Object context) {
      return null;
    }

    @Override
    public void setOwnerId(Object ownerId, Object context) {
    }

    @Override
    protected void clearOwnerId(Object context) {
    }

    @Override
    protected QueuedSynchronizer getQueuedSynchronizer(Object context) {
      return null;
    }

    @Override
    protected void queuedSynchronizerCleanup(QueuedSynchronizer sync,
        Object context) {
    }

    public boolean attemptLock(LockMode mode, int flags,
        LockingPolicy lockPolicy, long msecs, Object owner, Object context) {
      return false;
    }

    public void releaseLock(LockMode mode, boolean releaseAll, Object owner,
        Object context) {
    }
  }

  // The variables below are actually in ExclusiveSharedSynchronizer class as
  // static fields, but are protected so use a dummy class instead of copying.

  static final int MAX_READ_SHARED_COUNT = DummySync.MAX_READ_SHARED_COUNT;

  static final int MAX_READ_ONLY_COUNT = DummySync.MAX_READ_ONLY_COUNT;

  static final int MAX_WRITE_COUNT = DummySync.MAX_WRITE_COUNT;

  private Cache cache;

  @Override
  public void setUp() throws Exception {
    final InternalDistributedSystem sys = InternalDistributedSystem
        .getConnectedInstance();
    if (sys != null && sys.isConnected()) {
      sys.disconnect();
    }
    final Properties props = new Properties();
    props.setProperty("mcast-port", "0"); // loner
    this.cache = new CacheFactory(props).create();
  }

  @Override
  public void tearDown() throws Exception {
    if (this.cache != null && !this.cache.isClosed()) {
      this.cache.close();
    }
  }

  public void testMaxReadsMaxWritesAndMaxExclusive() throws Exception {
    final Object id = new String("id");
    final Object id2 = new String("id2");
    final ReentrantReadWriteWriteShareLock lock =
      new ReentrantReadWriteWriteShareLock();

    // Take max possible read locks
    for (int i = 0; i < MAX_READ_SHARED_COUNT; i++) {
      assertTrue(lock.attemptLock(LockMode.SH, 0, null));
    }
    try {
      lock.attemptLock(LockMode.SH, 0, null);
      fail("attempt to acquire read lock after max possible read: "
          + MAX_READ_SHARED_COUNT + " should have failed");
    } catch (IllegalMonitorStateException e) {
      // ignore expected exception
    }

    // Now release all the
    // Take max possible read locks
    for (int i = 0; i < MAX_READ_SHARED_COUNT; i++) {
      lock.releaseLock(LockMode.SH, false, null);
    }
    // Attempt to again release a read lock will throw
    // IllegalMonitorStateException
    try {
      lock.releaseLock(LockMode.SH, true, null);
      fail("attempt to release read lock when read count is 0 "
          + "should have failed");
    } catch (IllegalMonitorStateException e) {
      // ignore expected exception
    }

    // Take max possible write share locks
    for (int i = 0; i < MAX_WRITE_COUNT; i++) {
      assertTrue(lock.attemptLock(LockMode.EX_SH, 0, id));
    }
    try {
      lock.attemptLock(LockMode.EX_SH, 0, id);
      fail("attempt to acquire write share lock after max possible write share: "
          + MAX_WRITE_COUNT + " should have failed");
    } catch (IllegalMonitorStateException e) {
      // ignore expected exception
    }

    // Releasing the write ex lock by non-owner should throw an exception
    try {
      lock.releaseLock(LockMode.EX_SH, true, id2);
      fail("attempt to release write share lock when "
          + "owner is different should fail");
    } catch (IllegalMonitorStateException e) {
      // ignore expected exception
    }

    // Now release the write share lock
    lock.releaseLock(LockMode.EX_SH, true, id);
    // Attempt to again release a write share lock should throw
    // IllegalMonitorStateException
    try {
      lock.releaseLock(LockMode.EX_SH, false, id);
      fail("attempt to release write share lock when share counter "
          + "is reset should have failed");
    } catch (IllegalMonitorStateException e) {
      // ignore expected exception
    }
    // Releasing the write ex lock by non-owner should throw an exception
    try {
      lock.releaseLock(LockMode.EX_SH, true, id2);
      fail("attempt to release write share lock when "
          + "owner is different should fail");
    } catch (IllegalMonitorStateException e) {
      // ignore expected exception
    }

    // Take max possible write ex locks
    for (int i = 0; i < MAX_WRITE_COUNT; i++) {
      assertTrue(lock.attemptLock(LockMode.EX, 0, id));
    }
    try {
      lock.attemptLock(LockMode.EX, 0, id);
      fail("attempt to acquire write ex lock after max possible write ex: "
          + MAX_WRITE_COUNT + " should have failed");
    } catch (IllegalMonitorStateException e) {
      // ignore expected exception
    }

    // Releasing the write lock by non-owner should throw an exception
    try {
      lock.releaseLock(LockMode.EX, true, id2);
      fail("attempt to release write share lock when "
          + "owner is different should fail");
    } catch (IllegalMonitorStateException e) {
      // ignore expected exception
    }

    // Now release the write ex lock
    lock.releaseLock(LockMode.EX, true, id);
    // Attempt to again release a write ex lock will throw
    // IllegalMonitorStateException
    try {
      lock.releaseLock(LockMode.EX, true, id);
      fail("attempt to release write share lock when share counter is reset "
          + "should have failed");
    } catch (IllegalMonitorStateException e) {
      // ignore expected exception
    }
    // Releasing the write lock by non-owner should throw an exception
    try {
      lock.releaseLock(LockMode.EX, false, id2);
      fail("attempt to release write share lock when "
          + "owner is different should fail");
    } catch (IllegalMonitorStateException e) {
      // ignore expected exception
    }
  }

  public void testLockBehaviourWithSameThread() throws Exception {
    final Object id = new String("id");
    final ReentrantReadWriteWriteShareLock lock =
      new ReentrantReadWriteWriteShareLock();

    assertTrue(lock.attemptLock(LockMode.SH, 0, null));
    assertTrue(lock.attemptLock(LockMode.EX_SH, 0, id));
    // EX lock upgrade will wait for existing readers
    assertFalse(lock.attemptLock(LockMode.EX, 0, id));

    lock.releaseLock(LockMode.SH, true, null);

    // again with releaseAll as false
    assertTrue(lock.attemptLock(LockMode.SH, 0, null));
    assertTrue(lock.attemptLock(LockMode.EX_SH, 0, id));
    // EX lock upgrade will wait for existing readers
    assertFalse(lock.attemptLock(LockMode.EX, 0, id));

    lock.releaseLock(LockMode.SH, false, null);
    lock.releaseLock(LockMode.EX_SH, false, id);

    // no SH to EX lock upgrade
    assertTrue(lock.attemptLock(LockMode.SH, 0, id));
    assertFalse(lock.attemptLock(LockMode.EX, 0, id));
    lock.releaseLock(LockMode.SH, true, null);
    assertTrue(lock.attemptLock(LockMode.SH, 0, null));
    assertFalse(lock.attemptLock(LockMode.EX, 0, id));
    lock.releaseLock(LockMode.SH, false, id);
    assertTrue(lock.attemptLock(LockMode.SH, 0, id));
    assertFalse(lock.attemptLock(LockMode.EX, 0, id));
    lock.releaseLock(LockMode.SH, true, id);
    assertTrue(lock.attemptLock(LockMode.EX, 0, id));
    lock.releaseLock(LockMode.EX, true, id);

    assertTrue(lock.attemptLock(LockMode.EX_SH, 0, id));
    assertTrue(lock.attemptLock(LockMode.SH, 0, null));
    assertFalse(lock.attemptLock(LockMode.EX, 0, "newid"));
    // EX lock upgrade will wait for existing readers
    assertFalse(lock.attemptLock(LockMode.EX, 0, id));
    lock.releaseLock(LockMode.SH, false, null);
    assertTrue(lock.attemptLock(LockMode.EX, 0, id));

    lock.releaseLock(LockMode.EX, false, id);

    // try exclusive with releaseAll
    assertTrue(lock.attemptLock(LockMode.EX, 0, id));
    assertFalse(lock.attemptLock(LockMode.SH, 0, null));
    // do not allow to hold both EX and EX_SH locks simultaneously
    assertFalse(lock.attemptLock(LockMode.EX_SH, 0, id));
    assertFalse(lock.attemptLock(LockMode.EX_SH, 0, "newid"));
    assertFalse(lock.attemptLock(LockMode.EX, 0, "newid"));
    assertTrue(lock.attemptLock(LockMode.EX, 0, id));
    assertFalse(lock.attemptLock(LockMode.EX_SH, 0, id));
    // allow to hold EX, SH locks simultaneously
    assertTrue(lock.attemptLock(LockMode.SH, 0, id));
    assertTrue(lock.attemptLock(LockMode.SH, 0, id));

    lock.releaseLock(LockMode.EX, true, id);
    try {
      lock.releaseLock(LockMode.EX, true, id);
      fail("expected IllegalMonitorStateException");
    } catch (IllegalMonitorStateException ex) {
      // got expected exception
    }
    try {
      lock.releaseLock(LockMode.EX, false, id);
      fail("expected IllegalMonitorStateException");
    } catch (IllegalMonitorStateException ex) {
      // got expected exception
    }
    // EX_SH release should fail since now we do not account for
    // it separately from EX
    try {
      lock.releaseLock(LockMode.EX_SH, true, id);
      fail("expected IllegalMonitorStateException");
    } catch (IllegalMonitorStateException ex) {
      // got expected exception
    }
    try {
      lock.releaseLock(LockMode.EX_SH, false, id);
      fail("expected IllegalMonitorStateException");
    } catch (IllegalMonitorStateException ex) {
      // got expected exception
    }
    lock.releaseLock(LockMode.SH, true, id);
    try {
      lock.releaseLock(LockMode.SH, true, id);
      fail("expected IllegalMonitorStateException");
    } catch (IllegalMonitorStateException ex) {
      // got expected exception
    }
    try {
      lock.releaseLock(LockMode.SH, false, id);
      fail("expected IllegalMonitorStateException");
    } catch (IllegalMonitorStateException ex) {
      // got expected exception
    }

    // try exclusive without releaseAll
    assertTrue(lock.attemptLock(LockMode.EX, 0, id));
    assertFalse(lock.attemptLock(LockMode.SH, 0, null));
    assertFalse(lock.attemptLock(LockMode.EX_SH, 0, id));
    assertFalse(lock.attemptLock(LockMode.EX, 0, "newid"));
    assertTrue(lock.attemptLock(LockMode.SH, 0, id));
    assertTrue(lock.attemptLock(LockMode.EX, 0, id));
    assertFalse(lock.attemptLock(LockMode.EX_SH, 0, id));
    assertTrue(lock.attemptLock(LockMode.SH, 0, id));

    lock.releaseLock(LockMode.EX_SH, false, id);
    lock.releaseLock(LockMode.EX_SH, false, id);
    try {
      lock.releaseLock(LockMode.EX_SH, false, id);
      fail("expected IllegalMonitorStateException");
    } catch (IllegalMonitorStateException ex) {
      // got expected exception
    }
    try {
      lock.releaseLock(LockMode.EX_SH, true, id);
      fail("expected IllegalMonitorStateException");
    } catch (IllegalMonitorStateException ex) {
      // got expected exception
    }
    lock.releaseLock(LockMode.SH, false, id);
    lock.releaseLock(LockMode.SH, false, id);
    try {
      lock.releaseLock(LockMode.SH, false, id);
      fail("expected IllegalMonitorStateException");
    } catch (IllegalMonitorStateException ex) {
      // got expected exception
    }
    try {
      lock.releaseLock(LockMode.SH, true, id);
      fail("expected IllegalMonitorStateException");
    } catch (IllegalMonitorStateException ex) {
      // got expected exception
    }
    // EX release should fail since now we do not account for
    // it separately from EX_SH
    try {
      lock.releaseLock(LockMode.EX, false, id);
      fail("expected IllegalMonitorStateException");
    } catch (IllegalMonitorStateException ex) {
      // got expected exception
    }
    try {
      lock.releaseLock(LockMode.EX, true, id);
      fail("expected IllegalMonitorStateException");
    } catch (IllegalMonitorStateException ex) {
      // got expected exception
    }
  }

  public void testLockBehaviourWithDIfferentThread() throws Exception {
    final Object id = new String("id");
    final ReentrantReadWriteWriteShareLock lock =
      new ReentrantReadWriteWriteShareLock();

    assertTrue(lock.attemptLock(LockMode.EX_SH, 0, id));

    Locker locker = new Locker(lock, id, true, true, "R:S:E", "P:P:P");
    Thread t = new Thread(locker);
    t.start();
    t.join();

    // lock again as it would have been released
    assertTrue(lock.attemptLock(LockMode.EX_SH, 0, id));
    locker = new Locker(lock, id, false, true, "R:S:E", "P:F:F");
    Thread t1 = new Thread(locker);
    t1.start();
    t1.join();
  }

  private static volatile boolean failed = false;

  public void _testLockBehaviourQueue() throws Exception {
    final Object id = new String("id");
    final ReentrantReadWriteWriteShareLock lock =
      new ReentrantReadWriteWriteShareLock();

    assertTrue(lock.attemptLock(LockMode.EX_SH, 0, id));
    assertTrue(lock.attemptLock(LockMode.SH, 0, null));
    for (int i = 0; i < MAX_READ_SHARED_COUNT * 5; i++) {
      if (i % MAX_WRITE_COUNT == 0) {
        // if (shareCnt == MAX_WRITE_SHARED_COUNT) {
        // continue;
        // }
        // shareCnt++;
        // Thread t = new Thread(new WriteShareLocker(lock, id));
        // t.start();
      }
      else {
        Thread t = new Thread(new ReadLocker(lock));
        t.start();
      }
    }
    Thread.sleep(5000);
    lock.releaseLock(LockMode.EX_SH, true, id);
    try {
      lock.releaseLock(LockMode.EX_SH, false, id);
      fail("write share release clears the write bits, so second attempt "
          + "should fail");
    } catch (IllegalMonitorStateException e) {
      // ignore the exception as expected
    }
    assertFalse(failed);
  }

  // multi-threaded tests with perf check below

  private static final SharedStruct sharedVal = new SharedStruct();

  private static final AtomicInteger currentReaders = new AtomicInteger();

  private static final AtomicInteger currentWriters = new AtomicInteger();

  private static AcquireReleaseLocks currentLock;

  private static final AtomicInteger globalId = new AtomicInteger(0);

  public void testReadWriteLockWithPerf() throws Exception {
    final int numReaders = 100;
    final int numWriters = 10;
    final int totalRuns = 2500000;
    currentReaders.set(0);
    currentWriters.set(0);
    final Object startObject = new Object();

    // cache just used for cancelInProgress checks by implementations
    new CacheFactory().create();

    // create some reader and writer threads
    final Thread[] readers = new Thread[numReaders];
    final Thread[] writers = new Thread[numWriters];

    // runnable for the reader threads
    final Runnable readRun = new Runnable() {

      public void run() {
        try {
          synchronized (startObject) {
            startObject.wait();
          }
          for (int count = 1; count <= totalRuns / numReaders; ++count) {
            currentLock.acquireReadLock();
            try {
              final long longVal = sharedVal.longVal;
              currentReaders.incrementAndGet();
              assertEquals(0, currentWriters.get());
              final double doubleVal = Double.longBitsToDouble(longVal);
              final double actualDouble = sharedVal.doubleVal;
              final long actualDoubleLong = Double
                  .doubleToLongBits(actualDouble);
              assertEquals(longVal, actualDoubleLong);
              assertEquals(doubleVal, actualDouble);
              assertTrue(currentReaders.decrementAndGet() >= 0);
            } finally {
              currentLock.releaseReadLock();
            }
          }
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
        }
      }
    };

    // runnable for the writer threads
    final Runnable writeRun = new Runnable() {

      public void run() {
        try {
          synchronized (startObject) {
            startObject.wait();
          }
          for (int count = 1; count <= totalRuns / numReaders; ++count) {
            currentLock.acquireWriteLock();
            assertEquals(1, currentWriters.incrementAndGet());
            try {
              for (int iters = 1; iters <= numReaders / numWriters; ++iters) {
                final long longVal = sharedVal.longVal;
                sharedVal.longVal += 10;
                assertEquals(longVal, Double
                    .doubleToLongBits(sharedVal.doubleVal));
                assertEquals(0, currentReaders.get());
                assertEquals(1, currentWriters.get());
                sharedVal.doubleVal = Double
                    .longBitsToDouble(sharedVal.longVal);
              }
              currentWriters.decrementAndGet();
            } finally {
              currentLock.releaseWriteLock();
            }
          }
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
        }
      }
    };

    // test for different ReadWriteLock implementations
    final AcquireReleaseLocks[] allLocks = new AcquireReleaseLocks[] {
        new NonReentrantRWAcquireReleaseLocks(),
        new NonReentrantAcquireReleaseLocks(),
        new ReentrantRWAcquireReleaseLocks(),
        new ReentrantRW2AcquireReleaseLocks(),
        new ReentrantRW3AcquireReleaseLocks(),
    };

    // start the threads in parallel
    for (int times = 1; times <= 3; ++times) {
      for (AcquireReleaseLocks lock : allLocks) {
        currentLock = lock;
        for (int index = 0; index < numReaders; ++index) {
          readers[index] = new Thread(readRun);
          readers[index].start();
        }
        for (int index = 0; index < numWriters; ++index) {
          writers[index] = new Thread(writeRun);
          writers[index].start();
        }
        Thread.sleep(500);

        sharedVal.init(0);
        final long start = System.currentTimeMillis();
        synchronized (startObject) {
          startObject.notifyAll();
        }
        for (int index = 0; index < numReaders; ++index) {
          readers[index].join();
        }
        for (int index = 0; index < numWriters; ++index) {
          writers[index].join();
        }
        final long end = System.currentTimeMillis();
        assertEquals(totalRuns * 10, sharedVal.longVal);
        assertEquals(totalRuns * 10, Double
            .doubleToLongBits(sharedVal.doubleVal));
        assertEquals(0, currentReaders.get());
        assertEquals(0, currentWriters.get());
        System.out.println("Total time taken with " + currentLock
            + " in iteration " + times + ": " + (end - start) + "ms");
      }
    }
  }

  public void testReadWriteLockWithPerf2() throws Exception {
    final int numReaders = 20;
    final int numWriters = 2;
    final int totalRuns = 2500000;
    final int writerInterval = 1000;
    currentReaders.set(0);
    currentWriters.set(0);
    final Object startObject = new Object();

    // cache just used for cancelInProgress checks by implementations
    new CacheFactory().create();

    // create some reader and writer threads
    final Thread[] readers = new Thread[numReaders];
    final Thread[] writers = new Thread[numWriters];

    // runnable for the reader threads
    final Runnable readRun = new Runnable() {

      public void run() {
        try {
          synchronized (startObject) {
            startObject.wait();
          }
          for (int count = 1; count <= totalRuns / numReaders; ++count) {
            currentLock.acquireReadLock();
            try {
              final long longVal = sharedVal.longVal;
              currentReaders.incrementAndGet();
              assertEquals(0, currentWriters.get());
              final double doubleVal = Double.longBitsToDouble(longVal);
              final double actualDouble = sharedVal.doubleVal;
              final long actualDoubleLong = Double
                  .doubleToLongBits(actualDouble);
              assertEquals(longVal, actualDoubleLong);
              assertEquals(doubleVal, actualDouble);
              assertTrue(currentReaders.decrementAndGet() >= 0);
            } finally {
              currentLock.releaseReadLock();
            }
          }
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
        }
      }
    };

    // runnable for the writer threads
    final Runnable writeRun = new Runnable() {

      public void run() {
        try {
          final int myOffset = globalId.incrementAndGet() * writerInterval;
          synchronized (startObject) {
            startObject.wait();
          }
          for (int count = 1; count <= totalRuns / numWriters; ++count) {
            // this is just a busy wait to simulate the case where writers
            // appear after some intervals rather than trying to acquire
            // write locks continuously
            if (count < myOffset || ((count - myOffset) % writerInterval) != 0) {
              // do some checks and loop back
              assertTrue(currentReaders.get() >= 0);
              assertTrue(currentWriters.get() >= 0
                  && currentWriters.get() < numWriters);
              continue;
            }
            currentLock.acquireWriteLock();
            assertEquals(1, currentWriters.incrementAndGet());
            try {
              final long longVal = sharedVal.longVal;
              sharedVal.longVal += 10;
              assertEquals(longVal, Double
                  .doubleToLongBits(sharedVal.doubleVal));
              assertEquals(0, currentReaders.get());
              assertEquals(1, currentWriters.get());
              sharedVal.doubleVal = Double.longBitsToDouble(sharedVal.longVal);
              currentWriters.decrementAndGet();
            } finally {
              currentLock.releaseWriteLock();
            }
          }
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
        }
      }
    };

    // test for different ReadWriteLock implementations
    final AcquireReleaseLocks[] allLocks = new AcquireReleaseLocks[] {
        new NonReentrantRWAcquireReleaseLocks(),
        new NonReentrantAcquireReleaseLocks(),
        new ReentrantRWAcquireReleaseLocks(),
        new ReentrantRW2AcquireReleaseLocks(),
        new ReentrantRW3AcquireReleaseLocks(),
    };

    // start the threads in parallel
    for (int times = 1; times <= 3; ++times) {
      for (AcquireReleaseLocks lock : allLocks) {
        currentLock = lock;
        globalId.set(0);
        for (int index = 0; index < numReaders; ++index) {
          readers[index] = new Thread(readRun);
          readers[index].start();
        }
        for (int index = 0; index < numWriters; ++index) {
          writers[index] = new Thread(writeRun);
          writers[index].start();
        }
        Thread.sleep(500);

        sharedVal.init(10);
        final long start = System.currentTimeMillis();
        synchronized (startObject) {
          startObject.notifyAll();
        }
        for (int index = 0; index < numReaders; ++index) {
          readers[index].join();
        }
        for (int index = 0; index < numWriters; ++index) {
          writers[index].join();
        }
        final long end = System.currentTimeMillis();
        assertEquals(totalRuns * 10 / writerInterval, sharedVal.longVal);
        assertEquals(totalRuns * 10 / writerInterval, Double
            .doubleToLongBits(sharedVal.doubleVal));
        assertEquals(0, currentReaders.get());
        assertEquals(0, currentWriters.get());
        System.out.println("Total time taken with " + currentLock
            + " in iteration " + times + ": " + (end - start) + "ms");
      }
    }
  }

  private static final class SharedStruct {

    long longVal;

    double doubleVal;

    void init(long val) {
      this.longVal = val;
      this.doubleVal = Double.longBitsToDouble(val);
    }
  }

  private static interface AcquireReleaseLocks {

    void acquireReadLock() throws InterruptedException;

    void releaseReadLock();

    void acquireWriteLock() throws InterruptedException;

    void releaseWriteLock();
  }

  private static final class NonReentrantRWAcquireReleaseLocks implements
      AcquireReleaseLocks {

    private final NonReentrantReadWriteLock lock =
      new NonReentrantReadWriteLock();

    public void acquireReadLock() throws InterruptedException {
      this.lock.attemptReadLock(-1, null);
    }

    public void acquireWriteLock() throws InterruptedException {
      this.lock.attemptWriteLock(-1, null);
    }

    public void releaseReadLock() {
      this.lock.releaseReadLock();
    }

    public void releaseWriteLock() {
      this.lock.releaseWriteLock(null);
    }

    @Override
    public String toString() {
      return this.lock.toString();
    }
  }

  private static final class NonReentrantAcquireReleaseLocks implements
      AcquireReleaseLocks {

    private final NonReentrantLock lock = new NonReentrantLock(true);

    public void acquireReadLock() throws InterruptedException {
      this.lock.lock();
    }

    public void acquireWriteLock() throws InterruptedException {
      this.lock.lock();
    }

    public void releaseReadLock() {
      this.lock.unlock();
    }

    public void releaseWriteLock() {
      this.lock.unlock();
    }

    @Override
    public String toString() {
      return this.lock.toString();
    }
  }

  private static final class ReentrantRWAcquireReleaseLocks implements
      AcquireReleaseLocks {

    private final ReentrantReadWriteWriteShareLock lock =
      new ReentrantReadWriteWriteShareLock();

    public void acquireReadLock() throws InterruptedException {
      this.lock.attemptLock(LockMode.SH, -1, null);
    }

    public void acquireWriteLock() throws InterruptedException {
      this.lock.attemptLock(LockMode.EX, -1, null);
    }

    public void releaseReadLock() {
      this.lock.releaseLock(LockMode.SH, false, null);
    }

    public void releaseWriteLock() {
      this.lock.releaseLock(LockMode.EX, false, null);
    }

    @Override
    public String toString() {
      return this.lock.toString();
    }
  }

  private static final class ReentrantRW2AcquireReleaseLocks implements
      AcquireReleaseLocks {

    private final ReentrantReadWriteWriteShareLock lock =
      new ReentrantReadWriteWriteShareLock();

    public void acquireReadLock() throws InterruptedException {
      this.lock.attemptLock(LockMode.READ_ONLY, -1, null);
    }

    public void acquireWriteLock() throws InterruptedException {
      this.lock.attemptLock(LockMode.EX, -1, null);
    }

    public void releaseReadLock() {
      this.lock.releaseLock(LockMode.READ_ONLY, false, null);
    }

    public void releaseWriteLock() {
      this.lock.releaseLock(LockMode.EX, false, null);
    }

    @Override
    public String toString() {
      return this.lock.toString();
    }
  }

  private static final class ReentrantRW3AcquireReleaseLocks implements
      AcquireReleaseLocks {

    private final ReentrantReadWriteWriteShareLock lock =
      new ReentrantReadWriteWriteShareLock();

    public void acquireReadLock() throws InterruptedException {
      this.lock.attemptLock(LockMode.READ_ONLY, -1, null);
    }

    public void acquireWriteLock() throws InterruptedException {
      this.lock.attemptLock(LockMode.EX_SH, -1, null);
    }

    public void releaseReadLock() {
      this.lock.releaseLock(LockMode.READ_ONLY, false, null);
    }

    public void releaseWriteLock() {
      this.lock.releaseLock(LockMode.EX_SH, false, null);
    }

    @Override
    public String toString() {
      return this.lock.toString();
    }
  }

  private static class ReadLocker implements Runnable {

    private final long sleepMillisAfterTakingLock = 2;

    private final long waitMillisForTakingLock = 10;

    private final ReentrantReadWriteWriteShareLock l;

    public ReadLocker(ReentrantReadWriteWriteShareLock lock) {
      this.l = lock;
    }

    public void run() {
      try {
        if (this.l.attemptLock(LockMode.SH, waitMillisForTakingLock, null)) {
          Thread.sleep(this.sleepMillisAfterTakingLock);
        }
        else {
          failed = true;
        }
      } catch (InterruptedException e) {
        fail("did not expect exception in taking read locks");
      }
      this.l.releaseLock(LockMode.SH, false, null);
    }
  }

  /*
  private static class WriteShareLocker implements Runnable {

    private long sleepMillisAfterTakingLock = 3;

    private long waitMillisForTakingLock = 6;

    private ReentrantReadWriteWriteShareLock l;

    private Object id;

    public WriteShareLocker(ReentrantReadWriteWriteShareLock lock, Object id) {
      this.l = lock;
      this.id = id;
    }

    public void run() {
      try {
        if (this.l.attemptLock(LockMode.EX_SH, waitMillisForTakingLock, this.id)) {
          failed = true;
        }
      } catch (InterruptedException e) {
        fail("did not expect exception in taking read locks");
      }
      // this.l.releaseLock(LockMode.SH, );
    }
  }
  */

  private static class Locker implements Runnable {

    private final ReentrantReadWriteWriteShareLock l;

    private final Object id;

    private final boolean tryWithSameId;

    private final String[] opsArr;

    private final String[] resArr;

    private final boolean releasing;

    public Locker(ReentrantReadWriteWriteShareLock lock, Object id,
        boolean sameId, boolean rel, String ops, String results) {
      this.l = lock;
      this.id = id;
      this.tryWithSameId = sameId;
      opsArr = ops.split(":");
      this.releasing = rel;
      this.resArr = results.split(":");
    }

    public void run() {
      try {
        for (int i = 0; i < this.opsArr.length; i++) {
          char ch = this.opsArr[i].charAt(0);
          boolean shouldPass = this.resArr[i].equals("P") ? true : false;
          switch (ch) {
            case 'R':
              if (shouldPass) {
                assertTrue(this.l.attemptLock(LockMode.SH, 0, null));
              }
              else {
                assertFalse(this.l.attemptLock(LockMode.SH, 0, null));
              }
              if (this.releasing && shouldPass) {
                this.l.releaseLock(LockMode.SH, false, null);
              }
              break;

            case 'S':

              if (shouldPass) {
                if (this.tryWithSameId) {
                  assertTrue(this.l.attemptLock(LockMode.EX_SH, 0, this.id));
                }
                else {
                  assertTrue(this.l.attemptLock(LockMode.EX_SH, 0, "someId"));
                }
              }
              else {
                assertFalse(this.l.attemptLock(LockMode.EX_SH, 0, "someId"));
              }
              if (this.releasing && shouldPass) {
                this.l.releaseLock(LockMode.EX_SH, true, id);
              }
              break;

            case 'E':

              if (shouldPass) {
                if (this.tryWithSameId) {
                  assertTrue(this.l.attemptLock(LockMode.EX, 0, this.id));
                }
                else {
                  assertTrue(this.l.attemptLock(LockMode.EX, 0, "someid"));
                }
              }
              else {
                assertFalse(this.l.attemptLock(LockMode.EX, 0, "someid"));
              }
              if (this.releasing && shouldPass) {
                this.l.releaseLock(LockMode.EX, true, id);
              }
              break;

            default:
              fail("not expected to come to default case");
          }
        }
      } catch (Exception e) {
        fail("not expected to get exception");
      }
    }
  }

  /*
  private static final class SharedStruct {

    long longVal;

    void init(long val) {
      this.longVal = val;
    }
  }

  private static final SharedStruct value = new SharedStruct();

  private static AtomicInteger tx1success = new AtomicInteger(0);

  public void _testTransactionalBehaviour() throws Exception {
    final ReentrantReadWriteWriteShareLock txlock =
      new ReentrantReadWriteWriteShareLock();
    final ReadWriteLock readFailLock = new ReentrantReadWriteLock();
    boolean readFails = false;

    value.init(0);

    // 5 increments
    Runnable Tx1 = new Runnable() {
      private long startVal;

      private long thisTxVal;

      private Object id;

      public void run() {
        this.start();
        if (this.commit()) {
          tx1success.incrementAndGet();
        }
      }

      private boolean commit() {
        for (int i = 0; i < 10; i++) {

        }
        return false;
      }

      private void start() {
        this.id = new Object();
        this.startVal = this.thisTxVal = value.longVal;
      }
    };

    Runnable writeTask = new Runnable() {
      public void run() {
        Object id = new Object();
      }
    };

    Runnable writeShareTask = new Runnable() {
      public void run() {
      }
    };
  }
  */
}
