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

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import com.gemstone.gemfire.internal.concurrent.unsafe.UnsafeAtomicIntegerFieldUpdater;
import com.gemstone.gemfire.internal.concurrent.unsafe.UnsafeAtomicLongFieldUpdater;
import com.gemstone.gemfire.internal.concurrent.unsafe.UnsafeAtomicReferenceUpdater;
import com.gemstone.gemfire.internal.shared.unsafe.UnsafeHolder;

/**
 * Factory to get Atomic*Updater classes including the Unsafe* versions if
 * possible.
 * 
 * @author swale
 * @since gfxd 1.0
 */
public class AtomicUpdaterFactory {

  //uses reflection on AtomicLong to determine if platform supports 64bit CAS
  static final boolean hasLongCAS;

  static {
    // assume LONG_CAS is available by default
    boolean longCAS;
    try {
      Field field = AtomicLong.class.getDeclaredField("VM_SUPPORTS_LONG_CAS");
      field.setAccessible(true);
      Boolean result = (Boolean)field.get(null);
      longCAS = result == null || result.booleanValue();
    } catch (Throwable t) {
      // assume LONG_CAS is available by default
      longCAS = true;
    }
    hasLongCAS = longCAS;
  }

  /**
   * Creates and returns an updater for objects with the given reference field.
   */
  public static <T, V> AtomicReferenceFieldUpdater<T, V> newReferenceFieldUpdater(
      Class<T> tclass, Class<V> vclass, String fieldName) {
    if (UnsafeHolder.hasUnsafe()) {
      return new UnsafeAtomicReferenceUpdater<T, V>(tclass, vclass, fieldName);
    }
    else {
      return AtomicReferenceFieldUpdater.newUpdater(tclass, vclass, fieldName);
    }
  }

  /**
   * Creates and returns an updater for objects with the given integer field.
   */
  public static <T> AtomicIntegerFieldUpdater<T> newIntegerFieldUpdater(
      Class<T> tclass, String fieldName) {
    if (UnsafeHolder.hasUnsafe()) {
      return new UnsafeAtomicIntegerFieldUpdater<T>(tclass, fieldName);
    }
    else {
      return AtomicIntegerFieldUpdater.newUpdater(tclass, fieldName);
    }
  }

  /**
   * Creates and returns an updater for objects with the given long field.
   */
  public static <T> AtomicLongFieldUpdater<T> newLongFieldUpdater(
      Class<T> tclass, String fieldName) {
    if (hasLongCAS && UnsafeHolder.hasUnsafe()) {
      return new UnsafeAtomicLongFieldUpdater<T>(tclass, fieldName);
    }
    else {
      return AtomicLongFieldUpdater.newUpdater(tclass, fieldName);
    }
  }
}
