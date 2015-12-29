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
package com.gemstone.gemfire.cache.hdfs.internal;

import com.gemstone.gemfire.cache.hdfs.HDFSStoreFactory.HDFSCompactionConfigFactory;
import com.gemstone.gemfire.cache.hdfs.internal.HDFSStoreConfigHolder.AbstractHDFSCompactionConfigHolder;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXml;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * Class for authorization and validation of HDFS size Tiered compaction config
 * 
 * @author ashvina
 */
public class SizeTieredHdfsCompactionConfigHolder extends AbstractHDFSCompactionConfigHolder {
  @Override
  public String getCompactionStrategy() {
    return SIZE_ORIENTED;
  }

  @Override
  public HDFSCompactionConfigFactory setMaxInputFileSizeMB(int size) {
    HDFSStoreCreation.assertIsPositive(CacheXml.HDFS_COMPACTION_MAX_INPUT_FILE_SIZE_MB, size);
    this.maxInputFileSizeMB = size;
    return this;
  }

  @Override
  public HDFSCompactionConfigFactory setMinInputFileCount(int count) {
    HDFSStoreCreation.assertIsPositive(CacheXml.HDFS_COMPACTION_MIN_INPUT_FILE_COUNT, count);
    this.minInputFileCount = count;
    return this;
  }

  @Override
  public HDFSCompactionConfigFactory setMaxInputFileCount(int count) {
    HDFSStoreCreation.assertIsPositive(CacheXml.HDFS_COMPACTION_MAX_INPUT_FILE_COUNT, count);
    this.maxInputFileCount = count;
    return this;
  }
  
  @Override
  public HDFSCompactionConfigFactory setMajorCompactionIntervalMins(int count) {
    HDFSStoreCreation.assertIsPositive(CacheXml.HDFS_COMPACTION_MAJOR_COMPACTION_INTERVAL_MINS, count);
    this.majorCompactionIntervalMins = count;
    return this;
  }
  
  @Override
  public HDFSCompactionConfigFactory setMajorCompactionMaxThreads(int count) {
    HDFSStoreCreation.assertIsPositive(CacheXml.HDFS_COMPACTION_MAJOR_COMPACTION_CONCURRENCY, count);
    this.majorCompactionConcurrency = count;
    return this;
  }

  @Override
  protected void validate() {
    if (minInputFileCount > maxInputFileCount) {
      throw new IllegalArgumentException(
          LocalizedStrings.HOPLOG_MIN_IS_MORE_THAN_MAX
          .toLocalizedString(new Object[] {
              CacheXml.HDFS_COMPACTION_MIN_INPUT_FILE_COUNT,
              minInputFileCount,
              CacheXml.HDFS_COMPACTION_MAX_INPUT_FILE_COUNT,
              maxInputFileCount }));
    }
    super.validate();
  }
}
