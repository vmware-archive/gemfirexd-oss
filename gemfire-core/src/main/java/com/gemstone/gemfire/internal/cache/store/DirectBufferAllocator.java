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
package com.gemstone.gemfire.internal.cache.store;

import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Set;
import java.util.function.Consumer;

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.LowMemoryException;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.shared.unsafe.UnsafeHolder;
import com.gemstone.gemfire.internal.snappy.CallbackFactoryProvider;
import com.gemstone.gemfire.internal.snappy.StoreCallbacks;
import org.apache.spark.unsafe.Platform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Direct ByteBuffer implementation of {@link BufferAllocator}.
 */
public final class DirectBufferAllocator extends BufferAllocator {

  private static final DirectBufferAllocator instance =
      new DirectBufferAllocator();

  private static final UnsafeHolder.FreeMemoryFactory freeBufferFactory =
      FreeBuffer::new;

  private static final UnsafeHolder.FreeMemoryFactory freeStoreBufferFactory =
      FreeStoreBuffer::new;

  /**
   * Overhead of allocation on off-heap memory is kept fixed at 8 even though
   * actual overhead will be dependent on the malloc implementation.
   */
  public static final int DIRECT_OBJECT_OVERHEAD = 8;

  /**
   * The default owner of direct buffers tracked in UMM.
   */
  public static final String DIRECT_OBJECT_OWNER = "SNAPPYDATA_DIRECT_OBJECTS";

  /**
   * The owner of direct buffers that are stored in Regions and tracked in UMM.
   */
  public static final String DIRECT_STORE_OBJECT_OWNER =
      "SNAPPYDATA_DIRECT_STORE_OBJECTS";

  public static DirectBufferAllocator instance() {
    return instance;
  }

  private Logger logger = initLogger();

  private DirectBufferAllocator() {
  }

  public static long defaultMaxMemory() {
    // set default maxMemory as 80% of available RAM
    return (((((com.sun.management.OperatingSystemMXBean)
        ManagementFactory.getOperatingSystemMXBean())
        .getTotalPhysicalMemorySize() << 2) / 5 + 7) >>> 3) << 3;
  }

  public DirectBufferAllocator initialize() {
    this.logger = initLogger();
    return this;
  }

  private Logger initLogger() {
    return LoggerFactory.getLogger(getClass().getName());
  }

  private boolean reserveMemory(String objectName, long requiredSize,
      boolean shouldEvict) {
    // always allocate with a fixed owner
    final long occupiedSize = requiredSize + DIRECT_OBJECT_OVERHEAD;
    return CallbackFactoryProvider.getStoreCallbacks().acquireStorageMemory(
        objectName, occupiedSize, null, shouldEvict, true);
  }

  private boolean tryEvictData(String objectName, long requiredSpace) {
    UnsafeHolder.releasePendingReferences();
    return reserveMemory(objectName, requiredSpace, true);
  }

  public LowMemoryException lowMemoryException(String op, int required) {
    Set<DistributedMember> m = Collections.singleton(
        GemFireCacheImpl.getExisting().getMyId());
    StoreCallbacks callbacks = CallbackFactoryProvider.getStoreCallbacks();
    LowMemoryException lowMemory = new LowMemoryException(LocalizedStrings
        .ResourceManager_LOW_MEMORY_FOR_0_FUNCEXEC_MEMBERS_1
        .toLocalizedString("DirectBufferAllocator." + op + " (" +
            "maxStorage=" + callbacks.getStoragePoolSize(true) +
            " used=" + callbacks.getStoragePoolUsedMemory(true) +
            " required=" + required + ')', m), m);
    logger.warn(lowMemory.toString());
    return lowMemory;
  }

  @Override
  public ByteBuffer allocate(int size) {
    return allocate(DIRECT_OBJECT_OWNER, size, freeBufferFactory);
  }

  @Override
  public ByteBuffer allocateForStorage(int size) {
    return allocate(DIRECT_STORE_OBJECT_OWNER, size, freeStoreBufferFactory);
  }

  private ByteBuffer allocate(String objectName, int size,
      UnsafeHolder.FreeMemoryFactory factory) {
    final int allocSize = UnsafeHolder.getAllocationSize(size);
    if (reserveMemory(objectName, allocSize, false) ||
        tryEvictData(objectName, allocSize)) {
      return UnsafeHolder.allocateDirectBuffer(allocSize, factory);
    } else {
      throw lowMemoryException("allocate", allocSize);
    }
  }

  @Override
  public void clearPostAllocate(ByteBuffer buffer) {
    // clear till the capacity and not limit since former will be a factor
    // of 8 and hence more efficient in Unsafe.setMemory
    clearBuffer(buffer, 0, buffer.capacity());
  }

  @Override
  public Object baseObject(ByteBuffer buffer) {
    return null;
  }

  @Override
  public long baseOffset(ByteBuffer buffer) {
    return UnsafeHolder.getDirectBufferAddress(buffer);
  }

  @Override
  public ByteBuffer expand(ByteBuffer buffer, int required) {
    assert required > 0 : "expand: unexpected required = " + required;

    final int currentUsed = buffer.capacity();
    final int newLength = UnsafeHolder.getAllocationSize(
        BufferAllocator.expandedSize(currentUsed, required));
    final int delta = newLength - currentUsed;
    // store is never the owner here rather will happen in changeOwnerToStorage
    final String objectName = DIRECT_OBJECT_OWNER;
    // TODO: SW: this delta reserve is incorrect since original owner may not
    // be DIRECT_OBJECT_OWNER but outsider like UnsafeHolder or allocateDirect
    if (reserveMemory(objectName, delta, false) ||
        tryEvictData(objectName, delta)) {
      return UnsafeHolder.reallocateDirectBuffer(buffer, newLength, freeBufferFactory);
    } else {
      throw lowMemoryException("expand", delta);
    }
  }

  public void changeOwnerToStorage(ByteBuffer buffer, int capacity,
      Consumer<Boolean> changeOwner) {
    try {
      UnsafeHolder.changeDirectBufferCleaner(buffer, capacity,
          FreeBuffer.class, FreeStoreBuffer.class,
          freeStoreBufferFactory, changeOwner);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to change the owner of " +
          buffer + " to storage.", e);
    }
  }

  @Override
  public ByteBuffer fromBytes(byte[] bytes, int offset, int length,
      boolean forStorage) {
    final ByteBuffer buffer = forStorage ? allocateForStorage(length)
        : allocate(length);
    buffer.put(bytes, offset, length);
    // move to the start
    buffer.rewind();
    return buffer;
  }

  @Override
  public ByteBuffer transfer(ByteBuffer buffer) {
    if (buffer.isDirect()) {
      return buffer;
    } else {
      return super.transfer(buffer);
    }
  }

  @Override
  public void release(ByteBuffer buffer) {
    // reserved bytes will be decremented via FreeBuffer
    UnsafeHolder.releaseDirectBuffer(buffer);
  }

  @Override
  public boolean isDirect() {
    return true;
  }

  @Override
  public void close() {
    // check that all memory has been released else try to release
    long allocated = CallbackFactoryProvider.getStoreCallbacks()
        .getOffHeapMemory(DIRECT_STORE_OBJECT_OWNER);
    if (allocated > 0) {
      UnsafeHolder.releasePendingReferences();
      allocated = CallbackFactoryProvider.getStoreCallbacks()
          .getOffHeapMemory(DIRECT_STORE_OBJECT_OWNER);
      if (allocated > 0) {
        // TODO: this needs to be observed since its quite possible that
        // unreleased references will remain especially in cache close
        // unless an explicit GC is invoked
        logger.warn("Unreleased memory " + allocated + " bytes in close.");
      }
    }
  }

  @SuppressWarnings("serial")
  static abstract class FreeBufferBase extends UnsafeHolder.FreeMemory {

    protected final int size;

    FreeBufferBase(long address, int size) {
      super(address);
      this.size = size;
    }

    protected abstract String objectName();

    @Override
    public final void run() {
      final long address = tryFree();
      if (address != 0) {
        Platform.freeMemory(address);
        try {
          // decrement the size from pool
          CallbackFactoryProvider.getStoreCallbacks().releaseStorageMemory(
              objectName(), this.size + DIRECT_OBJECT_OVERHEAD, true);
        } catch (Throwable t) {
          // ignore exceptions
          SystemFailure.checkFailure();
          try {
            DirectBufferAllocator.instance().logger.error(
                "FreeBuffer unexpected exception", t);
          } catch (Throwable ignored) {
            // ignore if even logging failed
          }
        }
      }
    }
  }

  @SuppressWarnings("serial")
  static final class FreeBuffer extends FreeBufferBase {

    FreeBuffer(long address, int size) {
      super(address, size);
    }

    protected String objectName() {
      return DIRECT_OBJECT_OWNER;
    }
  }

  @SuppressWarnings("serial")
  static final class FreeStoreBuffer extends FreeBufferBase {

    FreeStoreBuffer(long address, int size) {
      super(address, size);
    }

    protected String objectName() {
      return DIRECT_STORE_OBJECT_OWNER;
    }
  }
}
