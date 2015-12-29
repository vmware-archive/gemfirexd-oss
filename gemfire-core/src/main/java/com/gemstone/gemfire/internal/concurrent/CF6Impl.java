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

import java.io.File;
import java.lang.management.LockInfo;
import java.lang.management.MonitorInfo;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;

import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.StatisticsType;
import com.gemstone.gemfire.internal.LocalStatisticsImpl;
import com.gemstone.gemfire.internal.StatisticsManager;
import com.gemstone.gemfire.internal.StatisticsTypeImpl;
import com.gemstone.gemfire.internal.stats50.Atomic50StatisticsImpl;
import com.gemstone.gemfire.internal.stats50.Atomic60StatisticsImpl;

/**
 * CF implementation for JDK 6.
 * @author darrel
 */
public class CF6Impl implements CF {
  final static private boolean isIBM =
    "IBM Corporation".equals(System.getProperty("java.vm.vendor"));

  public CF6Impl() {
    // need an explicit public constructor so it can be called through reflection.
  }

  public AB createAB() {
    return new AtomicBoolean5();
  }
  
  public AB createAB(boolean initialValue) {
    return new AtomicBoolean5(initialValue);
  }
  
  public AI createAI() {
    return new AtomicInteger5();
  }
  
  public AI createAI(int initialValue) {
    return new AtomicInteger5(initialValue);
  }
  
  public AL createAL() {
    return new AtomicLong5();
  }
  
  public AL createAL(long initialValue) {
    return new AtomicLong5(initialValue);
  }
  
  public AR createAR() {
    return new AtomicReference5();
  }
  
  public AIArray createAIArray(int length) {
    return new AtomicIntegerArray5(length);
  }
  
  public ARArray createARArray(int length) {
    return new AtomicReferenceArray5(length);
  }
  
  public CLQ createCLQ() {
    return new ConcurrentLinkedQueue5();
  }
  
  public CLQ createCLQ(java.util.Collection c) {
    return new ConcurrentLinkedQueue5(c);
  }
  
  public CM createCM() {
    return new ConcurrentHashMap5();
  }
  
  public CM createCM(int initialCapacity) {
    return new ConcurrentHashMap5(initialCapacity);
  }
  
  public CM createCM(int initialCapacity, float loadFactor, int concurrencyLevel) {
    return new ConcurrentHashMap5(initialCapacity, loadFactor, concurrencyLevel);
  }
  
  public CM createCM(java.util.Map m) {
    return new ConcurrentHashMap5(m);
  }

  public S createS(int permits) {
    return new Semaphore5(permits);
  }
  
  public S createS(int permits, boolean fair) {
    return new Semaphore5(permits, fair);
  }
  
  public BQ createLBQ() {
    return new LinkedBlockingQueue5();
  }
  
  public BQ createLBQ(java.util.Collection c) {
    return new LinkedBlockingQueue5(c);
  }
  
  public BQ createLBQ(int capacity) {
    return new LinkedBlockingQueue5(capacity);
  }
  
  public BQ createSQ() {
    return new SynchronousQueue5();
  }
  
  public BQ createSQ(boolean fair) {
    return new SynchronousQueue5(fair);
  }
  
  public BQ createABQ(int capacity) {
    return new ArrayBlockingQueue5(capacity);
  }
  
  public BQ createABQ(int capacity, boolean fair) {
    return new ArrayBlockingQueue5(capacity, fair);
  }
  
  public BQ createABQ(int capacity, boolean fair, java.util.Collection c) {
    return new ArrayBlockingQueue5(capacity, fair, c);
  }
  
  public CDL createCDL(int count) {
    return new CountDownLatch5(count);
  }
  
  public RL createRL() {
    return new ReentrantLock5();
  }
  
  public RL createRL(boolean fair) {
    return new ReentrantLock5(fair);
  }
  
  public RWL createRWL() {
    return new RWL5();
  }

  
  public java.util.List createCOWAL() {
    return new CopyOnWriteArrayList5();
  }
  
  public java.util.Set createCOWAS() {
    return new CopyOnWriteArraySet5();
  }
  
  
  public long nanoTime() {
    return java.lang.System.nanoTime();
  }
  
  public boolean nativeConcurrencyAvailable() {
    return true;
  }
  
  public int timerPurge(java.util.Timer t) {
    // Fix 39585, IBM's java.util.timer's purge() has stack overflow issue
    if (isIBM) {
      return 0;
    }
    return t.purge();
  }
  
  public java.util.Timer createTimer(String name, boolean isDaemon) {
    return new java.util.Timer(name, isDaemon);
  }

  /**
   * Whether per-thread stats are used.  Striping is disabled for the
   * IBM JVM due to bug 38226.
   * <p>
   * Now disabled by default.
   */
  public static boolean STRIPED_STATS_ENABLED =
    Boolean.getBoolean("gemfire.STRIPED_STATS_ENABLED");
    //|| "IBM Corporation".equals(System.getProperty("java.vm.vendor", "unknown"));

  public Statistics createAtomicStatistics(StatisticsType type, String textId,
                                           long nId, long uId,
                                           StatisticsManager mgr) {
    if (!STRIPED_STATS_ENABLED) {
      return new Atomic60StatisticsImpl(type, textId, nId, uId, mgr);
    }

    Statistics result = null;
    if (((StatisticsTypeImpl)type).getDoubleStatCount() == 0) {
      result = new Atomic50StatisticsImpl(type, textId, nId, uId, mgr);
    }
    else {
      result = new LocalStatisticsImpl(type, textId, nId, uId, true, 0, mgr);
    }
    return result;
  }

  public long getThreadId() {
    return Thread.currentThread().getId();
  }

  public java.net.InetSocketAddress createInetSocketAddress(String host, int port) {
    return java.net.InetSocketAddress.createUnresolved(host, port);
  }

  public boolean setExecutable(File file, boolean executable, boolean ownerOnly) {
    return file.setExecutable(executable, ownerOnly);
  }

  public boolean canExecute(File file) {
    return file.canExecute();
  }

  @Override
  public LI[] getLockedMonitors(ThreadInfo info) {
    MonitorInfo[] monitors = info.getLockedMonitors();
    if(monitors == null) {
      return null;
    }
    LI[] results = new LI[monitors.length];
    for(int i = 0; i < monitors.length; i++) {
      results[i] = new LI(monitors[i].getClassName(), monitors[i].getIdentityHashCode(), monitors[i].getLockedStackFrame());
    }
    return results;
  }
  @Override
  public LI[] getLockedSynchronizers(ThreadInfo info) {
    LockInfo[] monitors = info.getLockedSynchronizers();
    if(monitors == null) {
      return null;
    }
    LI[] results = new LI[monitors.length];
    for(int i = 0; i < monitors.length; i++) {
      results[i] = new LI(monitors[i].getClassName(), monitors[i].getIdentityHashCode());
    }
    return results;
  }
  @Override
  public LI getLockInfo(ThreadInfo threadInfo) {
    LockInfo lockInfo= threadInfo.getLockInfo();
    if(lockInfo == null) {
      return null;
    }
    
    return new LI(lockInfo.getClassName(), lockInfo.getIdentityHashCode());
  }
  
  @Override
  public ThreadInfo[] dumpAllThreads(ThreadMXBean bean, boolean lockedMonitors,
      boolean lockedSynchronizers) {
    return bean.dumpAllThreads(lockedMonitors, lockedSynchronizers);
  }
}
