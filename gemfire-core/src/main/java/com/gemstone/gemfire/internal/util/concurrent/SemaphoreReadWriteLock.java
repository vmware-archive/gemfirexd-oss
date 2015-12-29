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
package com.gemstone.gemfire.internal.util.concurrent;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * This ReadWriteLock is useful when different threads need to lock
 * and unlock the read lock.
 * @author sbawaska
 */
public class SemaphoreReadWriteLock implements ReadWriteLock {

  private SemaphoreReadLock readLock;
  private SemaphoreWriteLock writeLock;

  public SemaphoreReadWriteLock() {
    this(false);
  }

  /**
   * @param fair
   *          true if this lock will provide FIFO locking, else false
   */
  public SemaphoreReadWriteLock(boolean fair) {
    Semaphore writerSemaphore = new Semaphore(1, fair);
    Semaphore readerSemaphore = new Semaphore(1, fair);
    readLock = new SemaphoreReadLock(readerSemaphore, writerSemaphore);
    writeLock = new SemaphoreWriteLock(writerSemaphore);
  }
  
  @Override
  public Lock readLock() {
    return readLock;
  }

  @Override
  public Lock writeLock() {
    return writeLock;
  }

  public static class SemaphoreReadLock implements Lock {
    private int numReaders = 0;
    private final Semaphore readerSemaphore;
    private final Semaphore writerSemaphore;
    private final Set<Thread> readLockHolders = new HashSet<Thread>();

    public SemaphoreReadLock(Semaphore readerSemaphore,
        Semaphore writerSemaphore) {
      this.readerSemaphore = readerSemaphore;
      this.writerSemaphore = writerSemaphore;
    }

    @Override
    public void lock() {
      boolean interrupted = false;
      try {
        for (;;) {
          try {
            lockInterruptibly();
            break;
          } catch (InterruptedException e) {
            interrupted = true;
          }
        }
      } finally {
        if (interrupted) Thread.currentThread().interrupt();
      }
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
      readerSemaphore.acquire();
      try {
        if (readLockHolders.contains(Thread.currentThread())) {
          return;
        }
        numReaders++;
        if (numReaders == 1) {
          writerSemaphore.acquire();
        }
        readLockHolders.add(Thread.currentThread());
      } finally {
        // in case writeSemaphore.acquire throws Exception
        readerSemaphore.release();
      }
    }

    @Override
    public boolean tryLock() {
      boolean interrupted = false;
      try {
        for (;;) {
          try {
            return tryLock(0, TimeUnit.MILLISECONDS);
          } catch (InterruptedException e) {
            interrupted = true;
          }
        }
      } finally {
        if (interrupted) Thread.currentThread().interrupt();
      }
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit)
        throws InterruptedException {
      if (readerSemaphore.tryAcquire(time, unit)) {
        if (readLockHolders.contains(Thread.currentThread())) {
          readerSemaphore.release();
          return true;
        }
        int oldNumReaders = numReaders;
        numReaders++;
        if (numReaders == 1) {
          if (writerSemaphore.tryAcquire(time, unit)) {
            readLockHolders.add(Thread.currentThread());
            readerSemaphore.release();
            return true;
          } else {
            numReaders = oldNumReaders;
            readerSemaphore.release();
            return false;
          }
        } else {
          readLockHolders.add(Thread.currentThread());
          readerSemaphore.release();
          return true;
        }
      }
      return false;
    }

    @Override
    public void unlock() {
      for (;;) {
        boolean interrupted = false;
        try {
          readerSemaphore.acquire();
        } catch (InterruptedException e) {
          interrupted = true;
          continue;
        } finally {
          if (interrupted) Thread.currentThread().interrupt();
        }
        numReaders--;
        // The unlock method is forgiving
        if (numReaders <= 0) {
          numReaders = 0;
          if (writerSemaphore.availablePermits() == 0) {
            writerSemaphore.release();
          }
        }
        readLockHolders.remove(Thread.currentThread());
        readerSemaphore.release();
        break;
      }
    }

    @Override
    public Condition newCondition() {
      throw new UnsupportedOperationException();
    }
  }
  
  public static class SemaphoreWriteLock implements Lock {

    private final Semaphore writerSemaphore;
    private volatile Thread writeLockOwner;

    public SemaphoreWriteLock(Semaphore writerSemaphore) {
      this.writerSemaphore = writerSemaphore;
    }

    @Override
    public void lock() {
      boolean interrupted = false;
      try {
        for(;;) {
          try {
            lockInterruptibly();
            break;
          } catch (InterruptedException e) {
            interrupted = true;
          }
        }
      } finally {
        if (interrupted) Thread.currentThread().interrupt();
      }
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
      if (Thread.currentThread().equals(writeLockOwner)) {
        return;
      }
      writerSemaphore.acquire();
      writeLockOwner = Thread.currentThread();
    }

    @Override
    public boolean tryLock() {
      if (Thread.currentThread().equals(writeLockOwner)) {
        return true;
      }
      boolean result = writerSemaphore.tryAcquire();
      if (result) {
        writeLockOwner = Thread.currentThread();
      }
      return result;
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit)
        throws InterruptedException {
      if (Thread.currentThread().equals(writeLockOwner)) {
        return true;
      }
      boolean result = writerSemaphore.tryAcquire(time, unit);
      if (result) {
        writeLockOwner = Thread.currentThread();
      }
      return result;
    }

    @Override
    public void unlock() {
      writeLockOwner = null;
      writerSemaphore.release();
    }

    @Override
    public Condition newCondition() {
      throw new UnsupportedOperationException();
    }
    
  }
}
