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
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import com.gemstone.gemfire.*;
import com.gemstone.gemfire.internal.*;

/**
 * This interface describes what a factory implementation must implement
 * for this package's factory.
 * @see CFactory
 * @author darrel
 */
interface CF {
  public AB createAB();
  public AB createAB(boolean initialValue);
  public AI createAI();
  public AI createAI(int initialValue);
  public AL createAL();
  public AL createAL(long initialValue);
  public AR createAR();
  public AIArray createAIArray(int length);
  public ARArray createARArray(int length);
  public CLQ createCLQ();
  public CLQ createCLQ(java.util.Collection c);
  public CM createCM();
  public CM createCM(int initialCapacity);
  public CM createCM(int initialCapacity, float loadFactor, int concurrencyLevel);
  public CM createCM(java.util.Map m);
  public S createS(int permits);
  public S createS(int permits, boolean fair);
  public BQ createLBQ();
  public BQ createLBQ(java.util.Collection c);
  public BQ createLBQ(int capacity);
  public BQ createSQ();
  public BQ createSQ(boolean fair);
  public BQ createABQ(int capacity);
  public BQ createABQ(int capacity, boolean fair);
  public BQ createABQ(int capacity, boolean fair, java.util.Collection c);
  public CDL createCDL(int count);
  public RL createRL();
  public RL createRL(boolean fair);
  public RWL createRWL();
  public java.util.List createCOWAL();
  public java.util.Set createCOWAS();
  public long nanoTime();
  public boolean nativeConcurrencyAvailable();
  public int timerPurge(java.util.Timer t);
  public java.util.Timer createTimer(String name, boolean isDaemon);
  public Statistics createAtomicStatistics(StatisticsType type, String textId,
                                           long numericId, long uniqueId,
                                           StatisticsManager mgr);
  public long getThreadId();
  public java.net.InetSocketAddress createInetSocketAddress(String host, int port);
  public boolean setExecutable(File file, boolean executable, boolean ownerOnly);
  public boolean canExecute(File file);
  public LI[] getLockedMonitors(ThreadInfo info);
  public LI[] getLockedSynchronizers(ThreadInfo info);
  public LI getLockInfo(ThreadInfo info);
  public ThreadInfo[] dumpAllThreads(ThreadMXBean bean, boolean lockedMonitors,
      boolean lockedSynchronizers);
}
