/*
 * Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import com.gemstone.gnu.trove.TObjectProcedure;
import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.apache.commons.pool2.impl.GenericObjectPool;

/**
 * A configurable <code>KeyedObjectPool</code> implementation.
 * <p>
 * This is extends common-pool2 GenericKeyedObjectPool and differs only
 * in additional methods to query objects by key.
 *
 * @param <K> The type of keys maintained by this pool.
 * @param <T> Type of element pooled in this pool.
 * @see GenericObjectPool
 * @see GenericKeyedObjectPool
 */
public final class QueryKeyedObjectPool<K, T>
    extends GenericKeyedObjectPool<K, T> {

  /*
   * The "poolMap" field from the parent class obtained using reflection.
   */
  private final ConcurrentHashMap<?, ?> subPoolMap;

  /**
   * The "keyLock" field from the parent class obtained using reflection.
   */
  private final ReadWriteLock globalKeyLock;

  /**
   * The "allObjects" field from ObjectDequeue class obtained using reflection.
   */
  private volatile Field allObjectsField;

  /**
   * Create a new <code>QueryKeyedObjectPool</code> using defaults from
   * {@link GenericKeyedObjectPoolConfig}.
   *
   * @param factory the factory to be used to create entries
   */
  public QueryKeyedObjectPool(KeyedPooledObjectFactory<K, T> factory) {
    this(factory, new GenericKeyedObjectPoolConfig());
  }

  /**
   * Create a new <code>QueryKeyedObjectPool</code> using a specific
   * configuration.
   *
   * @param factory the factory to be used to create entries
   * @param config  The configuration to use for this pool instance. The
   *                configuration is used by value. Subsequent changes to
   *                the configuration object will not be reflected in the
   *                pool.
   */
  public QueryKeyedObjectPool(KeyedPooledObjectFactory<K, T> factory,
      GenericKeyedObjectPoolConfig config) {
    super(factory, config);

    try {
      final Class<?> superClass = getClass().getSuperclass();
      Field f = superClass.getDeclaredField("poolMap");
      f.setAccessible(true);
      this.subPoolMap = (ConcurrentHashMap<?, ?>)f.get(this);

      f = superClass.getDeclaredField("keyLock");
      f.setAccessible(true);
      this.globalKeyLock = (ReadWriteLock)f.get(this);
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to initialize QueryKeyedObjectPool", e);
    }
  }

  private Map<?, ?> getAllObjectsFromDeque(Object deque) {
    try {
      Field allObjects = this.allObjectsField;
      if (allObjects == null) {
        synchronized (this) {
          allObjects = this.allObjectsField;
          if (allObjects == null) {
            Field f = deque.getClass().getDeclaredField("allObjects");
            f.setAccessible(true);
            this.allObjectsField = allObjects = f;
          }
        }
      }
      return (Map<?, ?>)allObjects.get(deque);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to read pool queue", e);
    }
  }

  public int getNumTotal(K key) {
    final Object objectDeque = subPoolMap.get(key);
    if (objectDeque != null) {
      return getAllObjectsFromDeque(objectDeque).size();
    } else {
      return 0;
    }
  }

  /**
   * Apply the given function on each object for a key both idle (waiting
   * to be borrowed) and active (currently borrowed).
   */
  public void foreachObject(K key, TObjectProcedure proc) {
    Lock readLock = globalKeyLock.readLock();
    readLock.lock();
    try {
      Object queue = subPoolMap.get(key);
      if (queue != null) {
        for (Object p : getAllObjectsFromDeque(queue).values()) {
          if (!proc.execute(((PooledObject<?>)p).getObject())) {
            break;
          }
        }
      }
    } finally {
      readLock.unlock();
    }
  }
}
