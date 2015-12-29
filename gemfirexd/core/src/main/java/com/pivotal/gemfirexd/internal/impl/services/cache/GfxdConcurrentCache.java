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

package com.pivotal.gemfirexd.internal.impl.services.cache;

import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.services.cache.Cacheable;
import com.pivotal.gemfirexd.internal.iapi.services.cache.CacheableFactory;

/**
 * An extension to {@link ConcurrentCache} for GemFireXD that sets the identity
 * on a {@link CacheEntry} before inserting into the cache. This is to avoid
 * deadlock scenario with DDL read-write locks:
 * 
 * distributed write lock (other VM) -> local write lock -> cache hit with
 * existing entry -> {@link CacheEntry#waitUntilIdentityIsSet()}
 * 
 * cache miss -> cache put -> {@link Cacheable#setIdentity(Object)} -> read from
 * SYSTABLES -> local read lock
 * 
 * See bug #40683 for more details.
 * 
 * Currently this is only used for <code>TDCacheble</code>s while for other
 * {@link Cacheable}s the normal {@link ConcurrentCache} is used.
 * 
 * @see ConcurrentCache
 * 
 * @author swale
 */
final class GfxdConcurrentCache extends ConcurrentCache {

  /**
   * Creates a new cache manager.
   * 
   * @param holderFactory
   *          factory which creates <code>Cacheable</code>s
   * @param name
   *          the name of the cache
   * @param initialSize
   *          the initial capacity of the cache
   * @param maxSize
   *          maximum number of elements in the cache
   */
  GfxdConcurrentCache(CacheableFactory holderFactory, String name,
      int initialSize, int maxSize) {
    super(holderFactory, name, initialSize, maxSize);
  }

  // Overrides of ConcurrentCache

  /**
   * Find an object in the cache. If it is not present, add it to the cache. The
   * returned object is kept until <code>release()</code> is called.
   * 
   * @param key
   *          identity of the object to find
   * @return the cached object, or <code>null</code> if it cannot be found
   */
  @Override
  public Cacheable find(Object key) throws StandardException {

    if (stopped) {
      return null;
    }

    Cacheable item;
    CacheEntry entry = cache.get(key);
    while (true) {
      if (entry != null) {
        // Found an entry in the cache, lock it.
        entry.lock();
        if (entry.isValid()) {
          try {
            // Entry is still valid. Return it.
            item = entry.getCacheable();
            // The object is already cached. Increase the use count and
            // return it.
            entry.keep(true);
            return item;
          } finally {
            entry.unlock();
          }
        }
        else {
          // This entry has been removed from the cache while we were
          // waiting for the lock. Unlock it and try again.
          entry.unlock();
          entry = cache.get(key);
        }
      }
      else {
        entry = new CacheEntry(true);
        // Lock the entry before it's inserted in free slot.
        entry.lock();
        try {
          // The object is not cached. Insert the entry into a free
          // slot and retrieve a reusable Cacheable.
          item = insertIntoFreeSlot(key, entry);
        } finally {
          entry.unlock();
        }

        // Set the identity without holding the lock on the entry. If we
        // hold the lock, we may run into a deadlock if the user code in
        // setIdentity() re-enters the buffer manager.
        Cacheable itemWithIdentity = item.setIdentity(key);
        if (itemWithIdentity != null) {
          entry.setCacheable(itemWithIdentity);
          // add the entry to cache
          CacheEntry oldEntry = cache.putIfAbsent(key, entry);
          if (oldEntry != null) {
            // Someone inserted the entry while we created a new
            // one. Retry with the entry currently in the cache.
            entry = oldEntry;
          }
          else {
            // We successfully inserted a new entry.
            return itemWithIdentity;
          }
        }
        else {
          return null;
        }
      }
    }
  }

  /**
   * Create an object in the cache. The object is kept until
   * <code>release()</code> is called.
   * 
   * @param key
   *          identity of the object to create
   * @param createParameter
   *          parameters passed to <code>Cacheable.createIdentity()</code>
   * @return a reference to the cached object, or <code>null</code> if the
   *         object cannot be created
   * @exception StandardException
   *              if the object is already in the cache, or if some other error
   *              occurs
   * @see Cacheable#createIdentity(Object,Object)
   */
  @Override
  public Cacheable create(Object key, Object createParameter)
      throws StandardException {

    if (stopped) {
      return null;
    }

    Cacheable item;
    CacheEntry entry = new CacheEntry(true);
    // Lock the entry before it's inserted in free slot.
    entry.lock();
    try {
      // The object is not cached. Insert the entry into a free
      // slot and retrieve a reusable Cacheable.
      item = insertIntoFreeSlot(key, entry);
    } finally {
      entry.unlock();
    }

    // Create the identity without holding the lock on the entry.
    // Otherwise, we may run into a deadlock if the user code in
    // createIdentity() re-enters the buffer manager.
    Cacheable itemWithIdentity = item.createIdentity(key, createParameter);
    if (itemWithIdentity != null) {
      entry.setCacheable(itemWithIdentity);
      if (cache.putIfAbsent(key, entry) != null) {
        // We can't create the object if it's already in the cache.
        throw StandardException.newException(SQLState.OBJECT_EXISTS_IN_CACHE,
            name, key);
      }
    }
    return itemWithIdentity;
  }
}
