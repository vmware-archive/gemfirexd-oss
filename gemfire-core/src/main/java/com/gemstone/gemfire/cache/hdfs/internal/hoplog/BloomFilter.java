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
package com.gemstone.gemfire.cache.hdfs.internal.hoplog;

public interface BloomFilter {
  /**
   * Returns true if the bloom filter might contain the supplied key. The nature of the bloom filter
   * is such that false positives are allowed, but false negatives cannot occur.
   */
  boolean mightContain(byte[] key);

  /**
   * Returns true if the bloom filter might contain the supplied key. The nature of the bloom filter
   * is such that false positives are allowed, but false negatives cannot occur.
   */
  boolean mightContain(byte[] key, int keyOffset, int keyLength);

  /**
   * @return Size of the bloom, in bytes
   */
  long getBloomSize();
}