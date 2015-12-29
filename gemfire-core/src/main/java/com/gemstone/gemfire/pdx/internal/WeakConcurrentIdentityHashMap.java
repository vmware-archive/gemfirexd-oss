/*
 * Adapted from Apache Avro's org.apache.avro.util.WeakIdentityHashMap.
 * Original license below.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Changes for GemFireXD distributed data platform.
 *
 * Portions Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
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
package com.gemstone.gemfire.pdx.internal;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implements a combination of WeakHashMap and IdentityHashMap.
 * Useful for caches that need to key off of a == comparison
 * instead of a .equals.
 * <p/>
 * <b>
 * This class is not a general-purpose Map implementation! While
 * this class implements the Map interface, it intentionally violates
 * Map's general contract, which mandates the use of the equals method
 * when comparing objects. This class is designed for use only in the
 * rare cases wherein reference-equality semantics are required.
 * <p/>
 * Note: this code came from the Apache Avro from package org.apache.avro.util.
 * Modified to use a ConcurrentMap and few other changes.
 * </b>
 *
 * @since 6.6
 */
public final class WeakConcurrentIdentityHashMap<K, V> {

  private final ReferenceQueue<K> queue = new ReferenceQueue<>();
  private final ConcurrentHashMap<IdentityWeakReference<K>, V> backingStore =
      new ConcurrentHashMap<>();

  public WeakConcurrentIdentityHashMap() {
  }

  public void clear() {
    backingStore.clear();
    reap();
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean equals(Object o) {
    return (o == this) || ((o instanceof WeakConcurrentIdentityHashMap) &&
        backingStore.equals(((WeakConcurrentIdentityHashMap)o).backingStore));
  }

  public V get(K key) {
    reap();
    return backingStore.get(new IdentityWeakReference<>(key, queue));
  }

  public V put(K key, V value) {
    reap();
    return backingStore.put(new IdentityWeakReference<>(key, queue), value);
  }

  @Override
  public int hashCode() {
    reap();
    return backingStore.hashCode();
  }

  public boolean isEmpty() {
    reap();
    return backingStore.isEmpty();
  }

  public V remove(K key) {
    reap();
    return backingStore.remove(new IdentityWeakReference<>(key, queue));
  }

  public int size() {
    reap();
    return backingStore.size();
  }

  private void reap() {
    Reference<? extends K> zombie = queue.poll();

    while (zombie != null) {
      @SuppressWarnings("unchecked")
      IdentityWeakReference<K> victim = (IdentityWeakReference<K>)zombie;
      backingStore.remove(victim);
      zombie = queue.poll();
    }
  }

  private static class IdentityWeakReference<K> extends WeakReference<K> {
    private final int hash;

    @SuppressWarnings("unchecked")
    IdentityWeakReference(K obj, ReferenceQueue<K> queue) {
      super(obj, queue);
      hash = obj != null ? System.identityHashCode(obj) : 0;
    }

    @Override
    public int hashCode() {
      return hash;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o instanceof IdentityWeakReference) {
        IdentityWeakReference ref = (IdentityWeakReference)o;
        return this.get() == ref.get();
      } else {
        return false;
      }
    }
  }
}
