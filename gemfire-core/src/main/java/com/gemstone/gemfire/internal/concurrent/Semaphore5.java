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
 * S implementation for JDK 5.
 * @author darrel
 */
class Semaphore5
  extends java.util.concurrent.Semaphore
  implements S {
  private static final long serialVersionUID = -2646034174142919439L;
  public Semaphore5(int permits) {
    super(permits);
  }
  public Semaphore5(int permits, boolean fair) {
    super(permits, fair);
  }

  public boolean tryAcquireMs(long timeout)
    throws InterruptedException
  {
    return tryAcquire(timeout, TimeUnit.MILLISECONDS);
  }

  public boolean tryAcquireMs(int permits, long timeout)
    throws InterruptedException
  {
    return tryAcquire(permits, timeout, TimeUnit.MILLISECONDS);
  }
//   public void reducePermits(int reduction) {
//     super.reducePermits(reduction);
//   }
}
