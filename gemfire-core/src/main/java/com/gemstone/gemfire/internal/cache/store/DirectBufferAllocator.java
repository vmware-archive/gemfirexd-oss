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
import java.util.concurrent.atomic.AtomicLong;

import com.gemstone.gemfire.cache.LowMemoryException;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.shared.unsafe.UnsafeHolder;
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

  public static DirectBufferAllocator instance() {
    return instance;
  }

  private Logger logger = initLogger();
  private volatile long maxMemory;
  private final AtomicLong reserved = new AtomicLong(0L);

  private DirectBufferAllocator() {
    this.maxMemory = defaultMaxMemory();
    this.maxMemory = Math.max(sun.misc.VM.maxDirectMemory(), this.maxMemory);
  }

  private long defaultMaxMemory() {
    // set default maxMemory as 80% of available RAM
    return (((((com.sun.management.OperatingSystemMXBean)
        ManagementFactory.getOperatingSystemMXBean())
        .getTotalPhysicalMemorySize() << 2) / 5 + 7) >>> 3) << 3;
  }

  public DirectBufferAllocator initialize(long maxMemory) {
    this.maxMemory = maxMemory;
    this.logger = initLogger();
    return this;
  }

  private Logger initLogger() {
    return LoggerFactory.getLogger(getClass().getName());
  }

  private boolean reserveMemory(long requiredSize) {
    final long maxMemory = this.maxMemory;
    if (maxMemory > 0) {
      long used;
      while (maxMemory >= ((used = this.reserved.get()) + requiredSize)) {
        if (this.reserved.compareAndSet(used, used + requiredSize)) {
          return true;
        }
      }
      return false;
    } else {
      // allow unlimited acquisitions
      return true;
    }
  }

  private boolean tryReleasePendingReferences(long requiredSpace) {
    final sun.misc.JavaLangRefAccess refAccess =
        sun.misc.SharedSecrets.getJavaLangRefAccess();
    // retry while helping enqueue pending Cleaner Reference objects
    while (refAccess.tryHandlePendingReference()) {
      if (reserveMemory(requiredSpace)) {
        return true;
      }
    }
    return false;
  }

  private boolean releasePendingReferences(long requiredSpace) {
    if (tryReleasePendingReferences(requiredSpace)) {
      return true;
    }
    System.gc();
    return tryReleasePendingReferences(requiredSpace);
  }

  private LowMemoryException lowMemoryException(String op) {
    Set<DistributedMember> m = Collections.singleton(
        GemFireCacheImpl.getExisting().getMyId());
    LowMemoryException lowMemory = new LowMemoryException(LocalizedStrings
        .ResourceManager_LOW_MEMORY_FOR_0_FUNCEXEC_MEMBERS_1
        .toLocalizedString("DirectBufferAllocator." + op + " (maxMemory=" +
            this.maxMemory + ')', m), m);
    logger.warn(lowMemory.toString());
    return lowMemory;
  }

  @Override
  public ByteBuffer allocate(int size) {
    final int allocSize = UnsafeHolder.getAllocationSize(size);
    if (reserveMemory(allocSize) || releasePendingReferences(allocSize)) {
      return UnsafeHolder.allocateDirectBuffer(allocSize, freeBufferFactory);
    } else {
      throw lowMemoryException("allocate");
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
  public ByteBuffer expand(ByteBuffer buffer, long cursor, long startPosition,
      int required) {
    final int currentUsed = BufferAllocator.checkBufferSize(
        cursor - startPosition);
    final int newLength = UnsafeHolder.getAllocationSize(
        BufferAllocator.expandedSize(currentUsed, required));
    final int delta = newLength - currentUsed;
    if (reserveMemory(delta) || releasePendingReferences(delta)) {
      ByteBuffer newBuffer = UnsafeHolder.reallocateDirectBuffer(buffer,
          newLength, freeBufferFactory);
      // FreeBuffer will decrement the old memory whether reallocate is invoked
      // or Cleaner.clean() is invoked in tryFree, so add back currentUsed.
      // There is slight race condition here that can cause reserved to exceed
      // the maxMemory but we will live with that instead of complicating this.
      this.reserved.addAndGet(currentUsed);
      return newBuffer;
    } else {
      throw lowMemoryException("expand");
    }
  }

  @Override
  public ByteBuffer fromBytes(byte[] bytes, int offset, int length) {
    final ByteBuffer buffer = allocate(length);
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
    // reset maxMemory to default
    this.maxMemory = defaultMaxMemory();
    // check that all memory should have been released else try to release
    if (this.reserved.get() == 0L) {
      return;
    }
    final sun.misc.JavaLangRefAccess refAccess =
        sun.misc.SharedSecrets.getJavaLangRefAccess();
    // retry while helping enqueue pending Cleaner Reference objects
    while (refAccess.tryHandlePendingReference()) {
      if (this.reserved.get() == 0L) {
        return;
      }
    }
    final long used = this.reserved.get();
    if (used != 0) {
      logger.warn("Unreleased memory " + used + " bytes in close.");
      this.reserved.set(0);
    }
  }

  @SuppressWarnings("serial")
  static final class FreeBuffer extends UnsafeHolder.FreeMemory {

    private final int size;

    FreeBuffer(long address, int size) {
      super(address);
      this.size = size;
    }

    @Override
    protected long tryFree() {
      final long address = super.tryFree();
      if (address != 0L) {
        // decrement the size
        DirectBufferAllocator.instance().reserved.addAndGet(-this.size);
        return address;
      } else {
        return 0L;
      }
    }
  }
}
