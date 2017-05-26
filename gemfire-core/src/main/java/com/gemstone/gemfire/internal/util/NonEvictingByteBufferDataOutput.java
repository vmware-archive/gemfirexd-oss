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

package com.gemstone.gemfire.internal.util;

import com.gemstone.gemfire.internal.ByteBufferDataOutput;
import com.gemstone.gemfire.internal.cache.store.ManagedDirectBufferAllocator;
import com.gemstone.gemfire.internal.shared.BufferAllocator;
import com.gemstone.gemfire.internal.shared.Version;

/**
 * Hackish way to not evict off-heap memory data in some cases. Extendning from class
 * to avoid refactroing in a big way.
 */
public class NonEvictingByteBufferDataOutput extends ByteBufferDataOutput {

    protected static String BUFFER_OWNER =
            ManagedDirectBufferAllocator.CACHED_DATA_FRAME_RESULTOUTPUT_OWNER;

    public NonEvictingByteBufferDataOutput(int initialSize,
                                           BufferAllocator allocator,
                                           Version version) {
        super(initialSize, allocator, version);
    }
}
