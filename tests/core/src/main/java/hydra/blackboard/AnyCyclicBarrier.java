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
package hydra.blackboard;

import hydra.HydraRuntimeException;

/**
 * A cyclic barrier for use in synchronizing between hydra threads or VMs. Each
 * thread calling await will wait for <code> parties </code> threads to call
 * await, at which point all parties will proceed.
 * 
 * If the barrier is broken, a HydraRunTimeException will be thrown from all
 * waiting threads. Future attempts to use the cyclic barrier in any VM will
 * also throw an exception
 * 
 * @author dsmith
 * 
 */
public class AnyCyclicBarrier {
  private final SharedLock lock;
  private final SharedCondition condition;
  private final int parties;
  private BarrierBB bb;
  
  protected AnyCyclicBarrier(int parties, String lockName) {
    bb = new BarrierBB(lockName);
    bb.setParties(parties);
    this.parties = parties;
    
    lock = bb.getSharedLock();
    condition = lock.getCondition(lockName);
  }
  
  public static AnyCyclicBarrier lookup(int parties, String name) {
    return new AnyCyclicBarrier(parties, name);
  }
  
  public void await() {
    // a little optimization for a useless barrier. This is helpful if the
    // number of parties is determined programmatically
    if(parties == 1) {
      return;
    }
    
    lock.lock();
    try {
      long waiters = bb.incrementAndReadWaiters();
      if(waiters < 0) {
        throw new HydraRuntimeException("Barrier broken");
      }
      
      if(waiters % parties == 0) {
        condition.signalAll();
      }
      else {
        try {
          long iteration = waiters / parties;
          do {
            condition.await();
            waiters = bb.readWaiters() ;
          } while(iteration == waiters / parties);
          
        } catch (InterruptedException e) {
          bb.setBarrierBroken();
          waiters = Long.MIN_VALUE;
          condition.signalAll();
        }
        if(waiters < 0) {
          throw new HydraRuntimeException("Barrier broken");
        }
      }
    } finally {
      lock.unlock();
    }
  }
  
  private static class BarrierBB extends Blackboard {
    
    public static int parties;
    public static int waiters;
    
    /**
     *  Zero-arg constructor for remote method invocations.
     */
    public BarrierBB() {
    }
    
    public BarrierBB(String name) {
      super(name, Blackboard.RMI, BarrierBB.class);
    }
    

    public void setParties(long newValue) {
      long oldValue = getSharedCounters().read(parties);
      if(oldValue != 0) {
        if(oldValue != newValue) {
          throw new HydraRuntimeException("Barrier already initialized with size " + oldValue + ", trying to intialize with size " + newValue);
        }
      }
      else {
        getSharedCounters().setIfLarger(parties, newValue);
      }
    }
    
    public long incrementAndReadWaiters() {
      long result = getSharedCounters().incrementAndRead(waiters);
      return result;
    }
    
    public long readWaiters() {
      long result = getSharedCounters().read(waiters);
      return result;
    }
    
    public void setBarrierBroken() {
      getSharedCounters().setIfSmaller(waiters, Long.MIN_VALUE);
    }
  }
}
