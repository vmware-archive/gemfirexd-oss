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
import java.util.Collection;
import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.StatisticsType;
import com.gemstone.gemfire.internal.StatisticsManager;

/**
 * This is the <code>factory</code> for this package.
 * It is the only public class in this package. All users of this package
 * should only use this factory along with the public interfaces it produces.
 * It now only supports jdk 6 and later and should no longer be used.
 *
 * @author darrel
 * @author Eric Zoerner
 * @deprecated as of 7.0, use the JDK classes directly instead of CFactory.
 */
@Deprecated
public class CFactory  {
  /**
   * The factory impl to use.
   */
  private static final CF factoryImpl = new CF6Impl();

  private static CF getCF() {
    return factoryImpl;
  }
  /**
   * Create and return a C.
   */
  public static AB createAB() {
    return getCF().createAB();
  }

  /**
   * Create and return a AB given its initial value.
   */
  public static AB createAB(boolean initialValue) {
    return getCF().createAB(initialValue);
  }
  
  /**
   * Create and return a AI.
   */
  public static AI createAI() {
    return getCF().createAI();
  }

  /**
   * Create and return a AI given its initial value.
   */
  public static AI createAI(int initialValue) {
    return getCF().createAI(initialValue);
  }

  /**
   * Create and return a AL.
   */
  public static AL createAL() {
    return getCF().createAL();
  }

  /**
   * Create and return a AL given its initial value.
   */
  public static AL createAL(long initialValue) {
    return getCF().createAL(initialValue);
  }

  /**
   * Create and return an AR (AtomicReference).
   * @since 5.7
   */
  public static AR createAR() {
    return getCF().createAR();
  }

  /**
   * Create and return an AIArray given its length
   */
  public static AIArray createAIArray(int length) {
    return getCF().createAIArray(length);
  }
  /**
   * Create and return an ARArray given its length
   */
  public static ARArray createARArray(int length) {
    return getCF().createARArray(length);
  }

  /**
   * Create and return a CLQ.
   */
  public static CLQ createCLQ() {
    return getCF().createCLQ();
  }
  /**
   * Create and return a CLQ initialized to contain the given collection.
   */
  public static CLQ createCLQ(Collection c) {
    return getCF().createCLQ(c);
  }

  /**
   * Create and return a hash-based CM.
   */
  public static CM createCM() {
    return getCF().createCM();
  }
  /**
   * Create and return a hash-based CM.
   */
  public static CM createCM(int initialCapacity) {
    return getCF().createCM(initialCapacity);
  }

  public static CM createCM(java.util.Map m) {
    return getCF().createCM(m);
  }

  /**
   * Create and return a hash-based CM.
   */
  public static CM createCM(int initialCapacity, float loadFactor, int concurrencyLevel) {
    return getCF().createCM(initialCapacity, loadFactor, concurrencyLevel);
  }

  /**
   * Create and return a nonfair S given the number of permits
   */
  public static S createS(int permits) {
    return getCF().createS(permits);
  }
  /**
   * Create and return a S given the number of permits and fairness setting.
   */
  public static S createS(int permits, boolean fair) {
    return getCF().createS(permits, fair);
  }

  /**
   * Create and return a LinkedBlockingQueue
   */
  public static BQ createLBQ() {
    return getCF().createLBQ();
  }
  /**
   * Create and return a LinkedBlockingQueue
   * given the collection to initialize it with.
   */
  public static BQ createLBQ(Collection c) {
    return getCF().createLBQ();
  }
  /**
   * Create and return a LinkedBlockingQueue
   * given its maximum capacity.
   */
  public static BQ createLBQ(int capacity) {
    return getCF().createLBQ();
  }
  /**
   * Create and return a SynchronousQueue
   */
  public static BQ createSQ() {
    return getCF().createSQ();
  }
  /**
   * Create and return a SynchronousQueue
   * given its fairness policy.
   */
  public static BQ createSQ(boolean fair) {
    return getCF().createSQ();
  }
  /**
   * Create and return an ArrayBlockingQueue
   * given its fixed capacity.
   */
  public static BQ createABQ(int capacity) {
    return getCF().createABQ(capacity);
  }
  /**
   * Create and return an ArrayBlockingQueue
   * given its fixed capacity and fairness policy.
   */
  public static BQ createABQ(int capacity, boolean fair) {
    return getCF().createABQ(capacity, fair);
  }
  /**
   * Create and return an ArrayBlockingQueue
   * given its fixed capacity, fairness policy,
   * and collection of intial object to add.
   */
  public static BQ createABQ(int capacity, boolean fair, Collection c) {
    return getCF().createABQ(capacity, fair, c);
  }

  /**
   * Create and return a CDL (CountDownLatch)
   * given its initial count.
   */
  public static CDL createCDL(int count) {
    return getCF().createCDL(count);
  }
  /**
   * Create and return a CopyOnWriteArrayList
   */
  public static java.util.List createCOWAL() {
    return getCF().createCOWAL();
  }
  /**
   * Create and return a CopyOnWriteArraySet
   */
  public static java.util.Set createCOWAS() {
    return getCF().createCOWAS();
  }

  /**
   * Create and return an RL (ReentrantLock)
   */
  public static RL createRL() {
    return getCF().createRL();
  }
  /**
   * Create and return an RL (ReentrantLock)
   * with the given fairness policy.
   */
  public static RL createRL(boolean fair) {
    return getCF().createRL(fair);
  }

  public static RWL createRWL() {
    return getCF().createRWL();
  }
  /**
   * Returns the current value of the most precise available system timer, in nanoseconds.
   */
  public static long nanoTime() {
    return getCF().nanoTime();
  }
  /**
   * Returns true if VM has "native concurrency" capabilities.
   */
  public static boolean nativeConcurrencyAvailable() {
    return getCF().nativeConcurrencyAvailable();
  }
  /**
   * Removes all cancelled tasks from this timer's task queue. Calling
   * this method has no effect on the behavior of the timer, but
   * eliminates the references to the cancelled tasks from the
   * queue. If there are no external references to these tasks, they
   * become eligible for garbage collection.
   * Most programs will have
   * no need to call this method. It is designed for use by the rare
   * application that cancels a large number of tasks. Calling this
   * method trades time for space: the runtime of the method may be
   * proportional to n + c log n, where n is the number of tasks in the
   * queue and c is the number of cancelled tasks.
   * Note that it is
   * permissible to call this method from within a a task scheduled on
   * this timer.
   * @param t the timer to call purge on
   * @return the number of tasks removed from the queue.
   */
  public static int timerPurge(java.util.Timer t) {
    return getCF().timerPurge(t);
  }
  /**
   * Creates a Timer with the given name and daemon attribute.
   * @since 5.7
   */
  public static java.util.Timer createTimer(String name, boolean isDaemon) {
    return getCF().createTimer(name, isDaemon);
  }
  
  /**
   * Returns the id of the calling thread.
   * @since 5.5
   */
  public static long getThreadId() {
    return getCF().getThreadId();
  }

  /**
   * Create an unresolved InetSocketAddress, if possible.
   * If it is not possible then create a resolved one.
   * @since 5.7
   */
  public static java.net.InetSocketAddress createInetSocketAddress(String host,
                                                                   int port) {
    return getCF().createInetSocketAddress(host, port);
  }
  
  public static Statistics createAtomicStatistics(StatisticsType type, String textId,
                                                  long numericId, long uniqueId,
                                                  StatisticsManager mgr) {
    return getCF().createAtomicStatistics(type, textId, numericId, uniqueId, mgr);
  }

  public static boolean setExecutable(File file, boolean executable, boolean ownerOnly) {
    return getCF().setExecutable(file, executable, ownerOnly);
  }
  
  public static boolean canExecute(File file) {
    return getCF().canExecute(file);
  }

  /**
   * Get the locked monitors associated with this thread, if possible.
   * Otherwise, returns an empty array.
   * @since 6.6
   */
  public static LI[] getLockedMonitors(ThreadInfo info) {
    return getCF().getLockedMonitors(info);
  }
  
  /**
   * Get the locked synchronizers associated with this thread, if possible.
   * Otherwise, returns an empty array.
   * @since 6.6
   */
  public static LI[] getLockedSynchronizers(ThreadInfo info) {
    return getCF().getLockedSynchronizers(info);
  }
  
  /**
   * Get the lock this thread is blocked waiting on, if possible.
   * Otherwise, returns null.
   * @since 6.6
   */
  public static LI getLockInfo(ThreadInfo info) {
    return getCF().getLockInfo(info);
  }
  
  /**
   * This method dumps the thread info, optionally
   * including lockedMonitors and lockedSynchronizers, if such information
   * is available from the JVM. 
   * @param lockedMonitors
   * @param lockedSynchronizers
   */
  public static ThreadInfo[] dumpAllThreads(ThreadMXBean bean,
      boolean lockedMonitors, boolean lockedSynchronizers) {
    return getCF().dumpAllThreads(bean, lockedMonitors, lockedSynchronizers);
  }
  
  private CFactory() {
    // private so no instances allowed. static methods only
  }
}
