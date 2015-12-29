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

import java.util.Properties;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.hdfs.HDFSEventQueueAttributesFactory;
import com.gemstone.gemfire.cache.hdfs.HDFSStoreFactory;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.OffHeapTestUtil;

import dunit.SerializableCallable;
import dunit.SerializableRunnable;

public class RegionWithHDFSOffHeapBasicDUnitTest extends
    RegionWithHDFSBasicDUnitTest {
  static {
    System.setProperty("gemfire.trackOffHeapRefCounts", "true");
  }
  
  public RegionWithHDFSOffHeapBasicDUnitTest(String name) {
    super(name);
  }
  
  @Override
  public void tearDown2() throws Exception {
    SerializableRunnable checkOrphans = new SerializableRunnable() {

      @Override
      public void run() {
        if(hasCache()) {
          OffHeapTestUtil.checkOrphans();
        }
      }
    };
    checkOrphans.run();
    invokeInEveryVM(checkOrphans);
    super.tearDown2();
  }

  @Override
  public void testDelta() {
    //do nothing, deltas aren't supported with off heap.
  }

  
  @Override
  protected SerializableCallable getCreateRegionCallable(final int totalnumOfBuckets,
      final int batchSizeMB, final int maximumEntries, final String folderPath,
      final String uniqueName, final int batchInterval, final boolean queuePersistent,
      final boolean writeonly, final long timeForRollover, final long maxFileSize) {
    SerializableCallable createRegion = new SerializableCallable() {
      public Object call() throws Exception {
        AttributesFactory af = new AttributesFactory();
        af.setDataPolicy(DataPolicy.HDFS_PARTITION);
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setTotalNumBuckets(totalnumOfBuckets);
        paf.setRedundantCopies(1);
        
        HDFSEventQueueAttributesFactory hqf= new HDFSEventQueueAttributesFactory();
        hqf.setBatchSizeMB(batchSizeMB);
        hqf.setPersistent(queuePersistent);
        hqf.setMaximumQueueMemory(3);
        hqf.setBatchTimeInterval(batchInterval);
        af.setHDFSStoreName(uniqueName);
        
        af.setPartitionAttributes(paf.create());
        HDFSStoreFactory hsf = getCache().createHDFSStoreFactory();
        String homeDir = tmpDir + "/" + folderPath;
        hsf.setHomeDir(homeDir);
        hsf.setHDFSEventQueueAttributes(hqf.create());
        if (timeForRollover != -1) {
          hsf.setFileRolloverInterval((int)timeForRollover);
          System.setProperty("gemfire.HDFSRegionDirector.FILE_ROLLOVER_TASK_INTERVAL_SECONDS", "1");
        }
        if (maxFileSize != -1)
          hsf.setMaxFileSize((int)maxFileSize);
        hsf.create(uniqueName);
        
        af.setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(maximumEntries, EvictionAction.LOCAL_DESTROY));
        
        af.setHDFSWriteOnly(writeonly);
        af.setEnableOffHeapMemory(true);;
        Region r = createRootRegion(uniqueName, af.create());
        ((LocalRegion)r).setIsTest();
        
        return 0;
      }
    };
    return createRegion;
  }

  @Override
  public Properties getDistributedSystemProperties() {
    Properties props = super.getDistributedSystemProperties();
    props.setProperty("off-heap-memory-size", "50m");
    return props;
  }
}
