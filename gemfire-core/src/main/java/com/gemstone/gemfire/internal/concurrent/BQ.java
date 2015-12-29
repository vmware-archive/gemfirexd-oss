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

import java.util.Collection;

/**
 * Proxy for
 * the JDK 5 version java.util.concurrent.BlockingQueue interface.
 * @see CFactory
 * @author darrel
 * @deprecated use BlockingQueue
 */
public interface BQ extends Q {
  /**
   * Inserts the specified element at the tail of this queue,
   * waiting if necessary up to the specified msTimeout milliseconds
   * for space to become available.
   *
   * @param e the object to insert
   * @param msTimeout milliseconds to wait
   * @return true if it was possible to add to the queue; false otherwise
   * @throws NullPointerException if the specified element is null
   * @throws InterruptedException if interrupted while waiting
   */
  public boolean offer(Object e, long msTimeout) throws InterruptedException;
  /**
   * Inserts the specified element at the tail of this queue,
   * waiting if necessary for space to become available.
   *
   * @param e the object to insert
   * @throws NullPointerException if the specified element is null
   * @throws InterruptedException if interrupted while waiting
   */
  public void put(Object e) throws InterruptedException;

  /**
   * Retrieves and removes the head of this queue,
   * waiting if necessary up to the specified msTimeout milliseconds
   * if no elements are present on this queue.
   * @param msTimeout milliseconds to wait
   * @return the head of the queue or null
   * @throws InterruptedException if interrupted while waiting
   */
  public Object poll(long msTimeout) throws InterruptedException;
  /**
   * Retrieves and removes the head of this queue,
   * waiting if no elements are present.
   * @return the head of the queue
   * @throws InterruptedException if interrupted while waiting
   */
  public Object take() throws InterruptedException;
  /**
   * Returns the number of elements that this queue can ideally (in
   * the absence of memory or resource constraints) accept without
   * blocking, or Integer.MAX_VALUE if there is no intrinsic limit.

   * <p>Note that you cannot always tell if an attempt to add an element
   * will succeed by inspecting remainingCapacity because it may be
   * the case that another thread is about to put or take an element.

   * @return the remaining capacity
   */
  public int remainingCapacity();
  /**
   * Removes all available elements from this queue and adds them into
   * the given collection. This operation may be more efficient than
   * repeatedly polling this queue. A failure encountered while
   * attempting to add elements to collection c may result in elements
   * being in neither, either or both collections when the associated
   * exception is thrown. Attempts to drain a queue to itself result
   * in IllegalArgumentException. Further, the behavior of this
   * operation is undefined if the specified collection is modified
   * while the operation is in progress.
   * @param c the collection to transfer elements into
   * @return the number of elements transferred
   * @throws NullPointerException if c is null
   * @throws IllegalArgumentException if c is this queue
   */
  public int drainTo(Collection c);
  /**
   * Removes at most the given number of available
   * elements from this queue and adds them into
   * the given collection. This operation may be more efficient than
   * repeatedly polling this queue. A failure encountered while
   * attempting to add elements to collection c may result in elements
   * being in neither, either or both collections when the associated
   * exception is thrown. Attempts to drain a queue to itself result
   * in IllegalArgumentException. Further, the behavior of this
   * operation is undefined if the specified collection is modified
   * while the operation is in progress.
   * @param c the collection to transfer elements into
   * @param maxElements the maximum number of elements to transfer
   * @return the number of elements transferred
   * @throws NullPointerException if c is null
   * @throws IllegalArgumentException if c is this queue
   */
  public int drainTo(Collection c, int maxElements);
}
