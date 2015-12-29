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

import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.i18n.LogWriterI18n;

/**
 * TXSynchronizationThread manages beforeCompletion and afterCompletion
 * calls on behalf of a client cache.  The thread should be instantiated
 * with a Runnable that invokes beforeCompletion behavior.  Then you
 * must invoke runSecondRunnable() with another Runnable that invokes
 * afterCompletion behavior. 
 * 
 * @author Bruce Schuchardt
 * @since 6.6
 *
 */
public class TXSynchronizationRunnable implements Runnable {

  private Runnable firstRunnable;
  private final Object firstRunnableSync = new Object();
  private boolean firstRunnableCompleted;
  
  private Runnable secondRunnable;
  private final Object secondRunnableSync = new Object();
  private boolean secondRunnableCompleted;
  
  private boolean abort;

  public TXSynchronizationRunnable(Runnable beforeCompletion) {
    this.firstRunnable = beforeCompletion;
  }

  public void run() {
    LogWriterI18n log = InternalDistributedSystem.getLoggerI18n();
    synchronized(this.firstRunnableSync) {
      try {
        this.firstRunnable.run();
      } finally {
        if (log.finerEnabled()) {
          log.finer("beforeCompletion notification completed");
        }
        this.firstRunnableCompleted = true;
        this.firstRunnable = null;
        this.firstRunnableSync.notify();
      }
    }
    synchronized(this.secondRunnableSync){ 
      // TODO there should be a transaction timeout that keeps this thread
      // from sitting around forever in the event the client goes away
      while (this.secondRunnable == null && !this.abort) {
        try {
          if (log.finerEnabled()) {
            log.finer("waiting for afterCompletion notification");
          }
          this.secondRunnableSync.wait(1000);
        } catch (InterruptedException e) {
          // eat the interrupt and check for exit conditions
        }
        if (this.secondRunnable == null) {
          GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
          if (cache == null || cache.getCancelCriterion().cancelInProgress() != null) {
            return;
          }
        }
      }
      if (log.finerEnabled()) {
        log.finer("executing afterCompletion notification");
      }
      try {
        if (!this.abort) {
          this.secondRunnable.run();
        }
      } finally {
        if (log.finerEnabled()) {
          log.finer("afterCompletion notification completed");
        }
        this.secondRunnableCompleted = true;
        this.secondRunnable = null;
        this.secondRunnableSync.notify();
      }
    }
  }
  
  /**
   * wait for the initial beforeCompletion step to finish
   */
  public void waitForFirstExecution() {
    synchronized(this.firstRunnableSync) {
      while(!this.firstRunnableCompleted) {
        try {
          this.firstRunnableSync.wait(1000);
        } catch (InterruptedException e) {
          // eat the interrupt and check for exit conditions
        }
        // we really need the Cache Server's cancel criterion here, not the cache's
        // but who knows how to get it?
        GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
        if (cache == null) {
          return;
        }
        cache.getCancelCriterion().checkCancelInProgress(null);
      }
    }
  }

  /**
   * run the afterCompletion portion of synchronization.  This method
   * schedules execution of the given runnable and then waits for it to
   * finish running
   * @param r
   */
  public void runSecondRunnable(Runnable r) {
    synchronized(this.secondRunnableSync){ 
      this.secondRunnable = r;
      this.secondRunnableSync.notify();
      while(!this.secondRunnableCompleted && !this.abort) {
        try {
          this.secondRunnableSync.wait(1000);
        } catch (InterruptedException e) {
          // eat the interrupt and check for exit conditions
        }
        // we really need the Cache Server's cancel criterion here, not the cache's
        // but who knows how to get it?
        GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
        if (cache == null) {
          return;
        }
        cache.getCancelCriterion().checkCancelInProgress(null);
      }
    }
  }
  
  /**
   * stop waiting for an afterCompletion to arrive and just exit
   */
  public void abort() {
    synchronized(this.secondRunnableSync) {
      this.abort = true;
    }
  }
}
