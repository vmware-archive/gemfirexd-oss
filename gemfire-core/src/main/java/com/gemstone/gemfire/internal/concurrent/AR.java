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
 * the JDK 5 version java.util.concurrent.atomic.AtomicReference.
 * @see CFactory
 * @author darrel
 * @deprecated used AtomicReference instead
 */
public interface AR {
  /**
   * Gets the current value.
   *
   * @return the current value
   */
  public Object get();

  /**
   * Sets to the given value.
   *
   * @param newValue the new value
   */
  public void set(Object newValue);

  /**
   * Atomically sets to the given value and returns the old value.
   *
   * @param newValue the new value
   * @return the previous value
   */
  public Object getAndSet(Object newValue);

  /**
   * Atomically sets the value to the given updated value
   * if the current value {@code ==} the expected value.
   *
   * @param expect the expected value
   * @param update the new value
   * @return true if successful. False return indicates that
   * the actual value was not equal to the expected value.
   */
  public boolean compareAndSet(Object expect, Object update);

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
  public boolean weakCompareAndSet(Object expect, Object update);
}
