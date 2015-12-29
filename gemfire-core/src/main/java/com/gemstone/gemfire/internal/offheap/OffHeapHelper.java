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
package com.gemstone.gemfire.internal.offheap;

import com.gemstone.gemfire.internal.cache.CachedDeserializableFactory;
import com.gemstone.gemfire.internal.offheap.SimpleMemoryAllocatorImpl.Chunk;
import com.gemstone.gemfire.internal.offheap.annotations.Released;
import com.gemstone.gemfire.internal.offheap.annotations.Retained;
import com.gemstone.gemfire.internal.offheap.annotations.Unretained;

/**
 * Utility class that provides static method to do some common tasks for off-heap references.
 * 
 * @author darrel
 *
 */
public class OffHeapHelper {
  private OffHeapHelper() {
    // no instances allowed
  }
  
  /**
   * If o is off-heap then return its heap form; otherwise return o since it is already on the heap.
   * Note even if o is gfxd off-heap byte[] or byte[][] the heap form will be created.
   */
  public static Object getHeapForm(Object o) {
    if (o instanceof OffHeapReference) {
      return ((OffHeapReference) o).getValueAsDeserializedHeapObject();
    } else {
      return o;
    }
  }

  /**
   * Just like {@link #copyIfNeeded(Object)} except that if off-heap is copied it is also released.
   * @param v If this value is off-heap then the caller must have already retained it.
   * @return the heap copy to use in place of v; will be a retained off-heap reference in some gfxd use cases
   */
  @Retained
  public static Object copyAndReleaseIfNeeded(@Released Object v) {
    /**
     * Only retained when result == v because a copy was not made
     */
    @Retained Object result = null;
    try {
      result = OffHeapHelper.copyIfNeeded(v);
    } finally {
      if (v != result) {
        release(v);
      }
    }
    return result;
  }

  /**
   * If v is on heap then just return v; no copy needed.
   * Else if v is an off-heap gfxd byte[] or byte[][] then just return v; no copy needed.
   * Else v is off-heap so copy it to heap and return a reference to the heap copy.
   * Note that unlike {@link #getHeapForm(Object)} if v is a serialized off-heap object it will be copied to the heap as a CachedDeserializable.
   * If you prefer to have the serialized object also deserialized and copied to the heap use {@link #getHeapForm(Object)} but it will also copy gfxd byte[] and byte[][] to the heap.
   * 
   * @param v possible OFF_HEAP_REFERENCE
   * @return possible OFF_HEAP_REFERENCE if gfxd.
   */
  @Unretained
  public static Object copyIfNeeded(@Unretained Object v) {
    if (v instanceof StoredObject) {
      @Unretained StoredObject ohv = (StoredObject) v;
      if (CachedDeserializableFactory.preferObject()) {
        // in some cases getDeserializeValue just returns ohv
        v = ohv.getDeserializedValue(null, null);
      } else if (ohv.isSerialized()) {
        v = CachedDeserializableFactory.create(ohv.getSerializedValue());
      } else {
        // it is a byte[]
        v = ohv.getDeserializedForReading();
      }
    }
    return v;
  }

  /**
   * @return true if release was done
   */
  public static boolean release(@Released Object o) {
    if (o != null) {
      final Class<?> c = o.getClass();
      if (c == byte[].class || c == byte[][].class) {
        return false;
      }
      else if (Chunk.class.isAssignableFrom(c)) {
        ((Chunk)o).release();
        return true;
      }
      else {
        return false;
      }
    }
    else {
      return false;
    }
    /*
    if (o instanceof Chunk) {
      ((Chunk) o).release();
      return true;
    } else {
      return false;
    }
    */
  }

  /**
   * Just like {@link #release(Object)} but also disable debug tracking of the release.
   * @return true if release was done
   */
  public static boolean releaseWithNoTracking(@Released Object o) {
    if (o instanceof Chunk) {
      SimpleMemoryAllocatorImpl.skipRefCountTracking();
      ((Chunk) o).release();
      SimpleMemoryAllocatorImpl.unskipRefCountTracking();
      return true;
    } else {
      return false;
    }
  }
  
  /**
   * Just like {@link #release(Object)} but also set the owner for debug tracking of the release.
   * @return true if release was done
   */
  public static boolean releaseAndTrackOwner(@Released final Object o, final Object owner) {
    if (o instanceof Chunk) {
      SimpleMemoryAllocatorImpl.setReferenceCountOwner(owner);
      ((Chunk) o).release();
      SimpleMemoryAllocatorImpl.setReferenceCountOwner(null);
      return true;
    } else {
      return false;
    }
  }

}
