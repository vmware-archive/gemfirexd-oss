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

import com.gemstone.gemfire.cache.hdfs.HDFSEventQueueAttributes;
import com.gemstone.gemfire.cache.hdfs.HDFSEventQueueAttributesFactory;
import com.gemstone.gemfire.cache.hdfs.HDFSStore;
import com.gemstone.gemfire.cache.hdfs.HDFSStoreMutator;
import com.gemstone.gemfire.cache.hdfs.internal.HDFSStoreConfigHolder.AbstractHDFSCompactionConfigHolder;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

public class HDFSStoreMutatorImpl implements HDFSStoreMutator {
  private HDFSStoreConfigHolder configHolder;
  private HDFSCompactionConfigMutator compactionMutator;
  private HDFSEventQueueAttributesMutator qMutator;

  public HDFSStoreMutatorImpl() {
    configHolder = new HDFSStoreConfigHolder();
    configHolder.resetDefaultValues();
    compactionMutator = new HDFSCompactionConfigMutatorImpl(configHolder.getHDFSCompactionConfig());
    qMutator = new HDFSEventQueueAttributesMutatorImpl(null);
  }

  public HDFSStoreMutatorImpl(HDFSStore store) {
    configHolder = new HDFSStoreConfigHolder(store);
    compactionMutator = new HDFSCompactionConfigMutatorImpl(configHolder.getHDFSCompactionConfig());
    // The following two steps are needed to set the null boolean values in compactionMutator
    compactionMutator.setAutoCompaction(configHolder.getHDFSCompactionConfig().getAutoCompaction());
    compactionMutator.setAutoMajorCompaction(configHolder.getHDFSCompactionConfig().getAutoMajorCompaction());
    qMutator = new HDFSEventQueueAttributesMutatorImpl(configHolder.getHDFSEventQueueAttributes());
  }
  
  public HDFSStoreMutator setMaxFileSize(int maxFileSize) {
    configHolder.setMaxFileSize(maxFileSize);
    return this;
  }
  @Override
  public int getMaxFileSize() {
    return configHolder.getMaxFileSize();
  }

  @Override
  public HDFSStoreMutator setFileRolloverInterval(int count) {
    configHolder.setFileRolloverInterval(count);
    return this;
  }
  @Override
  public int getFileRolloverInterval() {
    return configHolder.getFileRolloverInterval();
  }

  @Override
  public HDFSCompactionConfigMutator getCompactionConfigMutator() {
    return compactionMutator;
  }

  @Override
  public HDFSEventQueueAttributesMutator getHDFSEventQueueAttributesMutator() {
    return qMutator;
  }

  public static class HDFSEventQueueAttributesMutatorImpl implements HDFSEventQueueAttributesMutator {
    private HDFSEventQueueAttributesFactory factory = new HDFSEventQueueAttributesFactory();
    int batchSize = -1;
    int batchInterval = -1;
    
    public HDFSEventQueueAttributesMutatorImpl(HDFSEventQueueAttributes qAttrs) {
      if (qAttrs == null) {
        return;
      }
      
      setBatchSizeMB(qAttrs.getBatchSizeMB());
      setBatchTimeInterval(qAttrs.getBatchTimeInterval());
    }
    
    @Override
    public HDFSEventQueueAttributesMutator setBatchSizeMB(int size) {
      factory.setBatchSizeMB(size);
      batchSize = size;
      // call factory.set to execute attribute value validation
      return this;
    }
    @Override
    public int getBatchSizeMB() {
      return batchSize;
    }

    @Override
    public HDFSEventQueueAttributesMutator setBatchTimeInterval(int interval) {
      batchInterval = interval;
      // call factory.set to execute attribute value validation
      factory.setBatchTimeInterval(interval);
      return this;
    }
    @Override
    public int getBatchTimeInterval() {
      return batchInterval;
    }
    
    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("HDFSEventQueueAttributesMutatorImpl [");
      if (batchSize > -1) {
        builder.append("batchSize=");
        builder.append(batchSize);
        builder.append(", ");
      }
      if (batchInterval > -1) {
        builder.append("batchInterval=");
        builder.append(batchInterval);
      }
      builder.append("]");
      return builder.toString();
    }
  }

  /**
   * @author ashvina
   */
  public static class HDFSCompactionConfigMutatorImpl implements HDFSCompactionConfigMutator {
    private AbstractHDFSCompactionConfigHolder configHolder;
    private Boolean autoCompact;
    private Boolean autoMajorCompact;

    public HDFSCompactionConfigMutatorImpl(AbstractHDFSCompactionConfigHolder configHolder) {
      this.configHolder = configHolder;
    }

    @Override
    public HDFSCompactionConfigMutator setAutoCompaction(boolean auto) {
      autoCompact = Boolean.valueOf(auto);
      configHolder.setAutoCompaction(auto);
      return this;
    }
    @Override
    public Boolean getAutoCompaction() {
      return autoCompact;
    }

    @Override
    public HDFSCompactionConfigMutator setMaxInputFileSizeMB(int size) {
      configHolder.setMaxInputFileSizeMB(size);
      return this;
    }
    @Override
    public int getMaxInputFileSizeMB() {
      return configHolder.getMaxInputFileSizeMB();
    }
    
    @Override
    public HDFSCompactionConfigMutator setMinInputFileCount(int count) {
      configHolder.setMinInputFileCount(count);
      return this;
    }
    @Override
    public int getMinInputFileCount() {
      return configHolder.getMinInputFileCount();
    }

    @Override
    public HDFSCompactionConfigMutator setMaxInputFileCount(int count) {
      configHolder.setMaxInputFileCount(count);
      return this;
    }
    @Override
    public int getMaxInputFileCount() {
      return configHolder.getMaxInputFileCount();
    }

    @Override
    public HDFSCompactionConfigMutator setMaxThreads(int count) {
      configHolder.setMaxThreads(count);
      return this;
    }
    @Override
    public int getMaxThreads() {
      return configHolder.getMaxThreads();
    }
    
    @Override
    public HDFSCompactionConfigMutator setAutoMajorCompaction(boolean auto) {
      autoMajorCompact = Boolean.valueOf(auto);
      configHolder.setAutoMajorCompaction(auto);
      return this;
    }
    @Override
    public Boolean getAutoMajorCompaction() {
      return autoMajorCompact;
    }

    @Override
    public HDFSCompactionConfigMutator setMajorCompactionIntervalMins(int count) {
      configHolder.setMajorCompactionIntervalMins(count);
      return this;
    }
    @Override
    public int getMajorCompactionIntervalMins() {
      return configHolder.getMajorCompactionIntervalMins();
    }

    @Override
    public HDFSCompactionConfigMutator setMajorCompactionMaxThreads(int count) {
      configHolder.setMajorCompactionMaxThreads(count);
      return this;
    }
    @Override
    public int getMajorCompactionMaxThreads() {
      return configHolder.getMajorCompactionMaxThreads();
    }

    @Override
    public HDFSCompactionConfigMutator setOldFilesCleanupIntervalMins(
        int interval) {
      configHolder.setOldFilesCleanupIntervalMins(interval);
      return this;
    }
    @Override
    public int getOldFilesCleanupIntervalMins() {
      return configHolder.getOldFilesCleanupIntervalMins();
    }
  }

  public static void assertIsPositive(String name, int count) {
    if (count < 1) {
      throw new IllegalArgumentException(
          LocalizedStrings.DiskWriteAttributesImpl_0_HAS_TO_BE_POSITIVE_NUMBER_AND_THE_VALUE_GIVEN_1_IS_NOT_ACCEPTABLE
              .toLocalizedString(new Object[] { name, count }));
    }
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("HDFSStoreMutatorImpl [");
    if (configHolder != null) {
      builder.append("configHolder=");
      builder.append(configHolder);
      builder.append(", ");
    }
    if (compactionMutator.getAutoCompaction() != null) {
      builder.append("autoCompaction=");
      builder.append(compactionMutator.getAutoCompaction());
      builder.append(", ");
    }
    if (compactionMutator.getAutoMajorCompaction() != null) {
      builder.append("autoMajorCompaction=");
      builder.append(compactionMutator.getAutoMajorCompaction());
      builder.append(", ");
    }
    if (qMutator != null) {
      builder.append("qMutator=");
      builder.append(qMutator);
    }
    builder.append("]");
    return builder.toString();
  }
}