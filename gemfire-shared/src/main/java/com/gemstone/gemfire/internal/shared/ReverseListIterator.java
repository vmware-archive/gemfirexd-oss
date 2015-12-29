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

package com.gemstone.gemfire.internal.shared;

import java.util.List;
import java.util.ListIterator;

/**
 * A reverse iterator on a list.
 * 
 * @author swale
 * @since gfxd 1.1
 */
public final class ReverseListIterator<E> implements ListIterator<E> {

  private final ListIterator<E> fwdIter;

  public ReverseListIterator(final List<E> list, int initialPosition) {
    this.fwdIter = list.listIterator(initialPosition);
  }

  /**
   * {@inheritDoc}
   */
  public boolean hasNext() {
    return this.fwdIter.hasPrevious();
  }

  /**
   * {@inheritDoc}
   */
  public E next() {
    return this.fwdIter.previous();
  }

  /**
   * {@inheritDoc}
   */
  public boolean hasPrevious() {
    return this.fwdIter.hasNext();
  }

  /**
   * {@inheritDoc}
   */
  public E previous() {
    return this.fwdIter.next();
  }

  /**
   * {@inheritDoc}
   */
  public int nextIndex() {
    return this.fwdIter.previousIndex();
  }

  /**
   * {@inheritDoc}
   */
  public int previousIndex() {
    return this.fwdIter.nextIndex();
  }

  /**
   * {@inheritDoc}
   */
  public void remove() {
    this.fwdIter.remove();
  }

  /**
   * {@inheritDoc}
   */
  public void set(E e) {
    this.fwdIter.set(e);
  }

  /**
   * {@inheritDoc}
   */
  public void add(E e) {
    // need to add before current position and since no straightforward way of
    // doing it, so unsupported for now
    throw new UnsupportedOperationException("ReverseListIterator.add");
  }
}
