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

package com.gemstone.gemfire.cache.partition;

import com.gemstone.gemfire.cache.Region;

/**
 * <p>Utility class that implements all methods in <code>PartitionListener</code>
 * with empty implementations. Applications can subclass this class and only
 * override the methods of interest.<p>
 * 
 * <p>Subclasses declared in a Cache XML file, it must also implement {@link com.gemstone.gemfire.cache.Declarable}
 * </p>
 * 
 * Note : Please contact VMware support before using these APIs
 *
 * @author Barry Oglesby
 * 
 * @since 6.6.2
 */
public class PartitionListenerAdapter implements PartitionListener {

  public void afterPrimary(int bucketId) {
  }

  public void afterRegionCreate(Region<?, ?> region) {
  }
  
  public void afterBucketRemoved(int bucketId, Iterable<?> keys) {
  }
  
  public void afterBucketCreated(int bucketId, Iterable<?> keys) {
  }
}
