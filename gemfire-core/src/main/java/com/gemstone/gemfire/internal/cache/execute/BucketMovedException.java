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

package com.gemstone.gemfire.internal.cache.execute;

import com.gemstone.gemfire.GemFireException;

public class BucketMovedException extends GemFireException {

  private static final long serialVersionUID = 4893171227542647452L;

  private final int bucketId;

  private final String regionName;

  /**
   * Creates new function exception with given error message.
   * 
   * @param msg
   * @since 6.0
   */
  public BucketMovedException(String msg, int bucketId, String regionName) {
    super(msg + "[buckedId=" + bucketId + ",region=" + regionName + ']');
    this.bucketId = bucketId;
    this.regionName = regionName;
  }

  public int getBucketId() {
    return this.bucketId;
  }

  public String getRegionName() {
    return this.regionName;
  }
}
