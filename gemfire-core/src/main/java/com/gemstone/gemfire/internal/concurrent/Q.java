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
 * Proxy for
 * the JDK 5 version java.util.Queue interface.
 * @see CFactory
 * @author darrel
 * @deprecated use Queue
 */
public interface Q extends java.util.Collection {
  /**
   * Inserts the specified element at the tail of this queue.
   *
   * @return true if it was possible to add to the queue; false otherwise
   * @throws NullPointerException if the specified element is null
   */
  public boolean offer(Object e);

  /**
   * Retrieves and removes the head of this queue,
   * or <code>null</code> if this queue is empty.
   */
  public Object poll();
  /**
   * Retrieves and removes the head of this queue.
   * This method differs from the poll method in that
   * it throws an exception if this queue is empty.
   */
  public Object remove() throws java.util.NoSuchElementException;
  /**
   * Retrieves but does not remove the head of this queue, returning
   * <code>null</code> if this queue is empty.
   */
  public Object peek();
  /**
   * Retrieves, but does not remove, the head of this queue.
   * This method differs from the peek method only in that it
   * throws an exception if this queue is empty.
   */
  public Object element() throws java.util.NoSuchElementException;
}
