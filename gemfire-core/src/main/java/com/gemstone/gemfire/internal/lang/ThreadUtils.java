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

package com.gemstone.gemfire.internal.lang;

import java.lang.Thread.State;

/**
 * The ThreadUtils class is an abstract utility class for working with and invoking methods on Threads.
 * <p/>
 * @author John Blum
 * @see java.lang.Thread
 * @since 7.0
 */
public abstract class ThreadUtils {

  /**
   * Gets the name of the particular Thread or null if the Thread object reference is null.
   * <p/>
   * @param thread the Thread object whose name is returned.
   * @return a String value indicating the name of the Thread or null if the Thread object reference is null.
   * @see java.lang.Thread#getName()
   */
  public static String getThreadName(final Thread thread) {
    return (thread == null ? null : thread.getName());
  }

  /**
   * Interrupts the specified Thread, guarding against null.
   * <p/>
   * @param thread the Thread to interrupt.
   * @see java.lang.Thread#interrupt()
   */
  public static void interrupt(final Thread thread) {
    if (thread != null) {
      thread.interrupt();
    }
  }

  /**
   * Determines whether the specified Thread is alive, guarding against null Object references.
   * <p/>
   * @param thread the Thread to determine for aliveness.
   * @return a boolean value indicating whether the specified Thread is alive.  Will return false if the Thread Object
   * references is null.
   * @see java.lang.Thread#isAlive()
   */
  public static boolean isAlive(final Thread thread) {
    return (thread != null && thread.isAlive());
  }

  /**
   * Determines whether the specified Thread is in a waiting state, guarding against null Object references
   * <p/>
   * @param thread the Thread to access it's state.
   * @return a boolean value indicating whether the Thread is in a waiting state.  If the Thread Object reference
   * is null, then this method return false, as no Thread is clearly not waiting for anything.
   * @see java.lang.Thread#getState()
   * @see java.lang.Thread.State#WAITING
   */
  public static boolean isWaiting(final Thread thread) {
    return (thread != null && thread.getState().equals(State.WAITING));
  }

  /**
   * Causes the current Thread to sleep for the specified number of milliseconds.  If the current Thread is interrupted
   * during sleep, the interrupt is ignore and the duration, in milliseconds, of completed sleep is returned.
   * <p/>
   * @param milliseconds an integer value specifying the number of milliseconds the current Thread should sleep.
   * @return a long value indicating duration in milliseconds of completed sleep by the current Thread.
   * @see java.lang.System#currentTimeMillis()
   * @see java.lang.Thread#sleep(long)
   */
  public static long sleep(final long milliseconds) {
    final long t0 = System.currentTimeMillis();

    try {
      Thread.sleep(milliseconds);
    }
    catch (InterruptedException ignore) {
    }

    return (System.currentTimeMillis() - t0);
  }

}
