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

import java.io.IOException;

/**
 * Defines an observer of asynchronous operations on sorted oplog files associated with a bucket.
 */
public interface HoplogListener {
  /**
   * Notifies creation of new sorted oplog files. A new file will be created after compaction or
   * other bucket maintenance activities
   * 
   * @throws IOException
   */
  void hoplogCreated(String regionFolder, int bucketId, Hoplog... oplogs) throws IOException;

  /**
   * Notifies file deletion. A file becomes redundant after compaction or other bucket maintenance
   * activities
   * @throws IOException 
   */
  void hoplogDeleted(String regionFolder, int bucketId, Hoplog... oplogs) throws IOException;
  
  /**
   * Notifies completion of a hoplog compaction cycle. 
   * @param region Region on which compaction was performed
   * @param bucket bucket id
   * @param isMajor true if major compaction was executed
   */
  void compactionCompleted(String region, int bucket, boolean isMajor);
}
