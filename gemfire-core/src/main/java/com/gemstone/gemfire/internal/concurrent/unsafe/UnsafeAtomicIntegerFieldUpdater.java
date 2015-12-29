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

package com.gemstone.gemfire.internal.concurrent.unsafe;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import com.gemstone.gemfire.internal.shared.unsafe.UnsafeHolder;

import sun.misc.Unsafe;

/**
 * Optimized implementation of {@link AtomicIntegerFieldUpdater} using the
 * internal Unsafe class if possible avoiding the various class checks.
 * 
 * @author swale
 * @since gfxd 1.0
 */
public final class UnsafeAtomicIntegerFieldUpdater<T> extends
    AtomicIntegerFieldUpdater<T> {

  static final Unsafe unsafe = UnsafeHolder.getUnsafe();

  private final long offset;

  public UnsafeAtomicIntegerFieldUpdater(Class<T> tclass, String fieldName) {
    this.offset = UnsafeAtomicReferenceUpdater.getFieldOffet(tclass, int.class,
        fieldName);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean compareAndSet(T obj, int expect, int update) {
    return unsafe.compareAndSwapInt(obj, this.offset, expect, update);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean weakCompareAndSet(T obj, int expect, int update) {
    // same as strong version
    return unsafe.compareAndSwapInt(obj, this.offset, expect, update);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void set(T obj, int newValue) {
    unsafe.putIntVolatile(obj, this.offset, newValue);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void lazySet(T obj, int newValue) {
    unsafe.putOrderedInt(obj, this.offset, newValue);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int get(T obj) {
    return unsafe.getIntVolatile(obj, this.offset);
  }
}
