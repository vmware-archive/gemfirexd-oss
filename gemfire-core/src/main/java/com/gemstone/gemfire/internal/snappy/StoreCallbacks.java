/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
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

package com.gemstone.gemfire.internal.snappy;

import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.gemstone.gemfire.internal.cache.BucketRegion;

public interface StoreCallbacks {

  String SHADOW_SCHEMA_NAME = "SNAPPYSYS_INTERNAL";

  String SHADOW_TABLE_SUFFIX = "_COLUMN_STORE_";

  void registerTypes();

  Set<Object> createColumnBatch(BucketRegion region, UUID batchID,
      int bucketID);

  List<String> getInternalTableSchemas();

  int getHashCodeSnappy(Object dvd, int numPartitions);

  int getHashCodeSnappy(Object dvds[], int numPartitions);

  public String columnBatchTableName(String tableName);

  public String snappyInternalSchemaName();

  void cleanUpCachedObjects(String table, Boolean sentFromExternalCluster);

  void registerRelationDestroyForHiveStore();

  void performConnectorOp(Object ctx);

  int getLastIndexOfRow(Object o);

  /**
   * Heap allocation calls will happen when creating region entries (and other
   *   places as required) and clearing them. Off-heap allocations and release
   * will be controlled by BufferAllocator. Reason being that it can track
   * the precise amount of memory still allocated (e.g. chunks removed from
   *   region still in use by iterators), can trigger a JVM reference collection
   * if memory is reaching threshold and can integrate with Spark off-heap
   * allocations which do not use UnsafeHolder.
   *
   * Bottom-line is that GemFire code that needs to use off-heap need not worry
   * about explicit calls to this layer and GemFireCacheImpl.getBufferAllocator
   * will do the accounting (so GemFire code should *never* be invoking
   *   store/release calls with offHeap=true).
   */
  boolean acquireStorageMemory(String objectName, long numBytes,
      UMMMemoryTracker buffer, boolean shouldEvict, boolean offHeap);

  void releaseStorageMemory(String objectName, long numBytes, boolean offHeap);

  void dropStorageMemory(String objectName, long ignoreBytes);

  boolean isSnappyStore();

  void resetMemoryManager();

  long getStoragePoolUsedMemory(boolean offHeap);
  long getStoragePoolSize(boolean offHeap);
  long getExecutionPoolUsedMemory(boolean offHeap);
  long getExecutionPoolSize(boolean offHeap);

  /**
   * Get the number of bytes used for off-heap storage for given object name.
   */
  long getOffHeapMemory(String objectName);

  /**
   * Do any additional accounting required when an off-heap value is put
   * into a region or removed.
   */
  void accountOffHeapStoreValue(Object newValue, Object oldValue);
}
