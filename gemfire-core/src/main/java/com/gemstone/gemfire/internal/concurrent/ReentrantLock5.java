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

import java.util.concurrent.TimeUnit;

/**
 * ReentrantLock implementation for JDK 5.
 * @author darrel
 */
class ReentrantLock5
  extends java.util.concurrent.locks.ReentrantLock
  implements RL {
  private static final long serialVersionUID = 297994531282083277L;
  public ReentrantLock5() {
    super();
  }
  public ReentrantLock5(boolean fair) {
    super(fair);
  }
  public C getNewCondition() {
    return new C5(super.newCondition());
  }
  public boolean tryLock(long msTime) throws InterruptedException {
    return super.tryLock(msTime, TimeUnit.MILLISECONDS);
  }

  static class C5 implements C {
    private final java.util.concurrent.locks.Condition delegate;
    C5(java.util.concurrent.locks.Condition delegate) {
      this.delegate = delegate;
    }
    public void await() throws InterruptedException {
      this.delegate.await();
    }
    public void awaitUninterruptibly() {
      this.delegate.awaitUninterruptibly();
    }
    public boolean await(long msTime) throws InterruptedException {
      return this.delegate.await(msTime, TimeUnit.MILLISECONDS);
    }
    public boolean awaitUntil(java.util.Date deadline) throws InterruptedException {
      return this.delegate.awaitUntil(deadline);
    }
    public void signal() {
      this.delegate.signal();
    }
    public void signalAll() {
      this.delegate.signalAll();
    }
  }
}
