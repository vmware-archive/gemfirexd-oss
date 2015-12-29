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

/**
 * These methods are the same ones on
 * the JDK 5 version java.util.concurrent.atomic.AtomicInteger.
 * Note that unlike AtomicInteger this interface does not support
 * <code>java.lang.Number</code>.
 * @see CFactory
 * @author darrel
 * @deprecated used AtomicInteger instead
 */
public interface AI {
  /**
   * Gets the current value.
   *
   * @return the current value
   */
  public int get();

  /**
   * Sets to the given value.
   *
   * @param newValue the new value
   */
  public void set(int newValue);

  /**
   * Atomically sets to the given value and returns the old value.
   *
   * @param newValue the new value
   * @return the previous value
   */
  public int getAndSet(int newValue);

  /**
   * Atomically sets the value to the given updated value
   * if the current value {@code ==} the expected value.
   *
   * @param expect the expected value
   * @param update the new value
   * @return true if successful. False return indicates that
   * the actual value was not equal to the expected value.
   */
  public boolean compareAndSet(int expect, int update);

  /**
   * Atomically sets the value to the given updated value
   * if the current value {@code ==} the expected value.
   *
   * <p>May <a href="package-summary.html#Spurious">fail spuriously</a>
   * and does not provide ordering guarantees, so is only rarely an
   * appropriate alternative to {@code compareAndSet}.
   *
   * @param expect the expected value
   * @param update the new value
   * @return true if successful.
   */
  public boolean weakCompareAndSet(int expect, int update);

  /**
   * Atomically increments by one the current value.
   *
   * @return the previous value
   */
  public int getAndIncrement();

  /**
   * Atomically decrements by one the current value.
   *
   * @return the previous value
   */
  public int getAndDecrement();

  /**
   * Atomically adds the given value to the current value.
   *
   * @param delta the value to add
   * @return the previous value
   */
  public int getAndAdd(int delta);

  /**
   * Atomically increments by one the current value.
   *
   * @return the updated value
   */
  public int incrementAndGet();

  /**
   * Atomically decrements by one the current value.
   *
   * @return the updated value
   */
  public int decrementAndGet();

  /**
   * Atomically adds the given value to the current value.
   *
   * @param delta the value to add
   * @return the updated value
   */
  public int addAndGet(int delta);
}
