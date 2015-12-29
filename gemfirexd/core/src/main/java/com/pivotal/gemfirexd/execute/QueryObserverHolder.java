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

package com.pivotal.gemfirexd.execute;

import java.util.Collection;

import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverHolder;

/**
 * A holder for {@link QueryObserver}s to allow for adding removing observers.
 * 
 * @author swale
 */
public class QueryObserverHolder {

  /**
   * Add a new {@link QueryObserver} to the list of observers for the VM. Note
   * that any number of observers of a given Class can be present at a time.
   * Restriction of only one observer of a give Class is removed.
   */
  public static void putInstance(QueryObserver observer) {
    GemFireXDQueryObserverHolder.putInstance(observer);
  }

  /**
   * Add a new {@link QueryObserver} to the list of observers for the VM if no
   * existing observer of the same Class is found.
   */
  public static QueryObserver putInstanceIfAbsent(QueryObserver observer) {
    return GemFireXDQueryObserverHolder.putInstanceIfAbsent(observer);
  }

  /** Return the current QueryObserver instance */
  public static QueryObserver getInstance() {
    return GemFireXDQueryObserverHolder.getInstance();
  }

  /**
   * Returns the first observer of the given class (registered using
   * {@link #putInstance(QueryObserver)} or
   * {@link #putInstanceIfAbsent(QueryObserver)}, or null if none found.
   */
  public static <T extends QueryObserver> T getObserver(Class<T> c) {
    return GemFireXDQueryObserverHolder.getObserver(c);
  }

  /**
   * Returns all the observers of the given class (registered using
   * {@link #putInstance(QueryObserver)} or
   * {@link #putInstanceIfAbsent(QueryObserver)}, or null if none found.
   */
  public static <T extends QueryObserver> Collection<T> getObservers(
      final Class<T> c) {
    return GemFireXDQueryObserverHolder.getObservers(c);
  }

  /**
   * Returns all the registered observers.
   */
  public static Collection<QueryObserver> getAllObservers() {
    return GemFireXDQueryObserverHolder.getAllObservers();
  }

  /**
   * Removes all observers of the given class (registered using
   * {@link #putInstance(QueryObserver)} or
   * {@link #putInstanceIfAbsent(QueryObserver)} and returns true if any
   * observer was removed else false.
   */
  public static boolean removeObserver(final Class<? extends QueryObserver> c) {
    return GemFireXDQueryObserverHolder.removeObserver(c);
  }

  /** remove the observer of given observer instance */
  public static boolean removeObserver(QueryObserver observer) {
    return GemFireXDQueryObserverHolder.removeObserver(observer);
  }
}
