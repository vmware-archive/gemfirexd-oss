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
package com.gemstone.gemfire.internal.cache.ha;

import com.gemstone.gemfire.internal.cache.ha.ThreadIdentifier.WanType;

import junit.framework.TestCase;

public class ThreadIdentifierJUnitTest extends TestCase {

  public void testPutAllId() {
    int id = 42;
    int bucketNumber = 113;
    
    long putAll = ThreadIdentifier.createFakeThreadIDForPutAll(bucketNumber, id);
    
    assertTrue(ThreadIdentifier.isPutAllFakeThreadID(putAll));
    assertEquals(42, ThreadIdentifier.getRealThreadID(putAll));
  }
  
  public void testWanId() {
    int id = 42;
    
    long wan1 = ThreadIdentifier.createFakeThreadIDForParallelGSPrimaryBucket(1, id);
    assertEquals(42, ThreadIdentifier.getRealThreadID(wan1));
    assertTrue(ThreadIdentifier.isParallelWANThreadID(wan1));
    {
      long real_tid_with_wan = ThreadIdentifier.getRealThreadIDIncludingWan(wan1);
      assertEquals(42, ThreadIdentifier.getRealThreadID(real_tid_with_wan));
      assertTrue(ThreadIdentifier.isParallelWANThreadID(real_tid_with_wan));
      assertTrue(WanType.PRIMARY.matches(real_tid_with_wan));
    }
    
    long wan2 = ThreadIdentifier.createFakeThreadIDForParallelGSSecondaryBucket(1, id);
    assertEquals(42, ThreadIdentifier.getRealThreadID(wan2));
    assertTrue(ThreadIdentifier.isParallelWANThreadID(wan2));
    {
      long real_tid_with_wan = ThreadIdentifier.getRealThreadIDIncludingWan(wan2);
      assertEquals(42, ThreadIdentifier.getRealThreadID(real_tid_with_wan));
      assertTrue(ThreadIdentifier.isParallelWANThreadID(real_tid_with_wan));
      assertTrue(WanType.SECONDARY.matches(real_tid_with_wan));
    }

    long wan3 = ThreadIdentifier.createFakeThreadIDForParallelGateway(1, id);
    assertEquals(42, ThreadIdentifier.getRealThreadID(wan3));
    assertTrue(ThreadIdentifier.isParallelWANThreadID(wan3));
    {
      long real_tid_with_wan = ThreadIdentifier.getRealThreadIDIncludingWan(wan3);
      assertEquals(42, ThreadIdentifier.getRealThreadID(real_tid_with_wan));
      assertTrue(ThreadIdentifier.isParallelWANThreadID(real_tid_with_wan));
      assertTrue(WanType.PARALLEL.matches(real_tid_with_wan));
    }
  }
  
  public void testWanAndPutAllId() {
    int id = 42;
    int bucketNumber = 113;
    
    long putAll = ThreadIdentifier.createFakeThreadIDForPutAll(bucketNumber, id);

    long wan1 = ThreadIdentifier.createFakeThreadIDForParallelGSPrimaryBucket(1, putAll);
    assertEquals(42, ThreadIdentifier.getRealThreadID(wan1));
    assertTrue(ThreadIdentifier.isParallelWANThreadID(wan1));
    assertTrue(ThreadIdentifier.isPutAllFakeThreadID(wan1));
    {
      long real_tid_with_wan = ThreadIdentifier.getRealThreadIDIncludingWan(wan1);
      assertEquals(42, ThreadIdentifier.getRealThreadID(real_tid_with_wan));
      assertTrue(ThreadIdentifier.isParallelWANThreadID(real_tid_with_wan));
      assertTrue(WanType.PRIMARY.matches(real_tid_with_wan));
    }
    
    long wan2 = ThreadIdentifier.createFakeThreadIDForParallelGSSecondaryBucket(1, putAll);
    assertEquals(42, ThreadIdentifier.getRealThreadID(wan2));
    assertTrue(ThreadIdentifier.isParallelWANThreadID(wan2));
    assertTrue(ThreadIdentifier.isPutAllFakeThreadID(wan2));
    {
      long real_tid_with_wan = ThreadIdentifier.getRealThreadIDIncludingWan(wan2);
      assertEquals(42, ThreadIdentifier.getRealThreadID(real_tid_with_wan));
      assertTrue(ThreadIdentifier.isParallelWANThreadID(real_tid_with_wan));
      assertTrue(WanType.SECONDARY.matches(real_tid_with_wan));
    }

    long wan3 = ThreadIdentifier.createFakeThreadIDForParallelGateway(1, putAll);
    assertEquals(42, ThreadIdentifier.getRealThreadID(wan3));
    assertTrue(ThreadIdentifier.isParallelWANThreadID(wan3));
    assertTrue(ThreadIdentifier.isPutAllFakeThreadID(wan3));
    {
      long real_tid_with_wan = ThreadIdentifier.getRealThreadIDIncludingWan(wan3);
      assertEquals(42, ThreadIdentifier.getRealThreadID(real_tid_with_wan));
      assertTrue(ThreadIdentifier.isParallelWANThreadID(real_tid_with_wan));
      assertTrue(WanType.PARALLEL.matches(real_tid_with_wan));
    }
    
    long tid = 4054000001L;
    assertTrue(ThreadIdentifier.isParallelWANThreadID(tid));
    assertFalse(ThreadIdentifier.isParallelWANThreadID(putAll));
  }
}
