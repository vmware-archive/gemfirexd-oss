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
package com.gemstone.gemfire.internal.concurrent;

import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.TimeUnit;

/**
 * ReentrantReadWriteLock implementation for JDK 5.
 * @author darrel
 */
class RWL5 extends ReentrantReadWriteLock
  implements RWL {
  private static final long serialVersionUID = 6144484929425711210L;
  private final L readLock;
  private final L writeLock;
  public RWL5() {
    super();
    this.readLock = new InnerLock5(readLock());
    this.writeLock = new InnerLock5(writeLock());
  }
  public L getReadLock() {
    return this.readLock;
  }
  public L getWriteLock() {
    return this.writeLock;
  }
  private static class InnerLock5 implements L {
    private final Lock delegate;
    InnerLock5(Lock delegate) {
      this.delegate = delegate;
    }
    public void lock() {
      this.delegate.lock();
    }
    public void lockInterruptibly() throws InterruptedException {
      this.delegate.lockInterruptibly();
    }
    public boolean tryLock() {
      return this.delegate.tryLock();
    }
    public boolean tryLock(long msTime) throws InterruptedException {
      return this.delegate.tryLock(msTime, TimeUnit.MILLISECONDS);
    }
    public void unlock() {
      this.delegate.unlock();
    }

    public C getNewCondition() {
      return new ReentrantLock5.C5(this.delegate.newCondition());
    }
  }
}
