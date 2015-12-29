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
/**
 * 
 */
package memscale;

import com.gemstone.gemfire.internal.offheap.SimpleMemoryAllocatorImpl.Chunk;

/**
 * @author lynng
 *
 */
public class OffHeapHelperVersionHelper {

  public static void checkIsAllocated(Chunk aChunk) {
    // this check was not available in 1.0
  }

  public static void verifyOffHeapMemoryConsistency() {
    // Since we already know that this version of the product has bugs that
    // result in off-heap orphans, there is no point in failing backwards
    // compatibility tests with oprhans. We want the current version of the
    // product to fail with orphans if detected, but not this version.
  }

}

