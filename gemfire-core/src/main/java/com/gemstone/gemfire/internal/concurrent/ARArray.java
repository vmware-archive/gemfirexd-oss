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
 * the JDK 5 version java.util.concurrent.atomic.AtomicReferenceArray.
 * @see CFactory
 * @author darrel
 * @since 5.7
 * @deprecated used AtomicReferenceArray instead
 */
public interface ARArray {
  /**
   * Returns the length of the array.
   *
   * @return the length of the array
   */
  public int length();

  /**
   * Gets the current value at position {@code i}.
   *
   * @param i the index
   * @return the current value
   */
  public Object get(int i);

  /**
   * Sets the element at position {@code i} to the given value.
   *
   * @param i the index
   * @param newValue the new value
   */
  public void set(int i, Object newValue);

  /**
   * Atomically sets the element at position {@code i} to the given
   * value and returns the old value.
   *
   * @param i the index
   * @param newValue the new value
   * @return the previous value
   */
  public Object getAndSet(int i, Object newValue);

  /**
   * Atomically sets the element at position {@code i} to the given
   * updated value if the current value {@code ==} the expected value.
   *
   * @param i the index
   * @param expect the expected value
   * @param update the new value
   * @return true if successful. False return indicates that
   * the actual value was not equal to the expected value.
   */
  public boolean compareAndSet(int i, Object expect, Object update);

  /**
   * Atomically sets the element at position {@code i} to the given
   * updated value if the current value {@code ==} the expected value.
   *
   * <p>May fail spuriously
   * and does not provide ordering guarantees, so is only rarely an
   * appropriate alternative to {@code compareAndSet}.
   *
   * @param i the index
   * @param expect the expected value
   * @param update the new value
   * @return true if successful.
   */
  public boolean weakCompareAndSet(int i, Object expect, Object update);
}
