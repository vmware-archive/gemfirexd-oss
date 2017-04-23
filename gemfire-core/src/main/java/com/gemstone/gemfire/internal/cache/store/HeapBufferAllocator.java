package com.gemstone.gemfire.internal.cache.store;

import java.nio.ByteBuffer;

import org.apache.spark.unsafe.Platform;

/**
 * Heap ByteBuffer implementation of {@link BufferAllocator}.
 */
public final class HeapBufferAllocator extends BufferAllocator {

  private static final HeapBufferAllocator instance =
      new HeapBufferAllocator();

  public static HeapBufferAllocator instance() {
    return instance;
  }

  private HeapBufferAllocator() {
  }

  @Override
  public ByteBuffer allocate(int size) {
    return ByteBuffer.allocate(size);
  }

  @Override
  public void clearPostAllocate(ByteBuffer buffer) {
    // JVM clears the allocated area
  }

  @Override
  public Object baseObject(ByteBuffer buffer) {
    return buffer.array();
  }

  @Override
  public long baseOffset(ByteBuffer buffer) {
    return Platform.BYTE_ARRAY_OFFSET + buffer.arrayOffset();
  }

  @Override
  public ByteBuffer expand(ByteBuffer buffer, long cursor, long startPosition,
      int required) {
    final byte[] bytes = buffer.array();
    final int currentUsed = BufferAllocator.checkBufferSize(
        cursor - startPosition);
    final int newLength = BufferAllocator.expandedSize(currentUsed, required);
    final byte[] newBytes = new byte[newLength];
    System.arraycopy(bytes, buffer.arrayOffset(), newBytes, 0, currentUsed);
    return ByteBuffer.wrap(newBytes);
  }

  @Override
  public void release(ByteBuffer buffer) {
  }

  @Override
  public boolean isDirect() {
    return false;
  }

  @Override
  public void close() {
  }
}
