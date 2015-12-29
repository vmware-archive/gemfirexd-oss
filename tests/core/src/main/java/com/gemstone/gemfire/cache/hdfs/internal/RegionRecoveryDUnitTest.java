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

import java.io.File;
import java.util.Properties;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.hdfs.HDFSEventQueueAttributesFactory;
import com.gemstone.gemfire.cache.hdfs.HDFSStoreFactory;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.FileUtil;

import dunit.AsyncInvocation;
import dunit.Host;
import dunit.SerializableCallable;
import dunit.VM;

/**
 * A class for testing the recovery after restart for GemFire cluster
 * that has HDFS regions
 * 
 * @author Hemant Bhanawat
 */
public class RegionRecoveryDUnitTest extends CacheTestCase {
  public RegionRecoveryDUnitTest(String name) {
    super(name);
  }
  

  /**
   * Tests a basic restart of the system. Events if in HDFS should be read back. 
   * The async queue is not persisted so we wait
   * until async queue persists the items to HDFS. 
   * @throws Exception
   */
  public void testbasicRestart() throws Exception{
    disconnectFromDS();
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    
    String homeDir = "./testGetFromHDFS";
//    String homeDir = "/tmp";
    
    createServerRegion(vm0, 11, 1, 500, 500, homeDir, "testGetFromHDFS");
    createServerRegion(vm1, 11, 1, 500, 500, homeDir, "testGetFromHDFS");
    createServerRegion(vm2, 11, 1, 500, 500, homeDir, "testGetFromHDFS");
    createServerRegion(vm3, 11, 1, 500, 500, homeDir, "testGetFromHDFS");
    
    
    doPuts(vm0, "testGetFromHDFS", 1, 50);
    doPuts(vm1, "testGetFromHDFS", 40, 100);
    doPuts(vm2, "testGetFromHDFS", 40, 100);
    doPuts(vm3, "testGetFromHDFS", 90, 150);
    
    cacheClose (vm0, true);
    cacheClose (vm1, true);
    cacheClose (vm2, true);
    cacheClose (vm3, true);
    
    createServerRegion(vm0, 11, 1, 500, 500, homeDir, "testGetFromHDFS");
    createServerRegion(vm1, 11, 1, 500, 500, homeDir, "testGetFromHDFS");
    createServerRegion(vm2, 11, 1, 500, 500, homeDir, "testGetFromHDFS");
    createServerRegion(vm3, 11, 1, 500, 500, homeDir, "testGetFromHDFS");
    
    verifyGetsForValue(vm0, "testGetFromHDFS", 1, 50, false);
    verifyGetsForValue(vm1, "testGetFromHDFS", 40, 100, false);
    verifyGetsForValue(vm2, "testGetFromHDFS", 40, 100, false);
    verifyGetsForValue(vm3, "testGetFromHDFS", 90, 150, false);
    
    cacheClose (vm0, false);
    cacheClose (vm1, false);
    cacheClose (vm2, false);
    cacheClose (vm3, false);

    disconnectFromDS();
    
  }
  
  /**
   * Servers are stopped and restarted. 
   * Disabled due to bug 48067.
   */
  public void DISABLED_testPersistedAsyncQueue_Restart() throws Exception{
    disconnectFromDS();
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    
    String homeDir = "./testPersistedAsyncQueue_Restart";
//    String homeDir = "/tmp";
    
    // create cache and region 
    createPersistedServerRegion(vm0, 11, 1, 2000, 5, homeDir, "testPersistedAsyncQueue_Restart");
    createPersistedServerRegion(vm1, 11, 1, 2000, 5, homeDir, "testPersistedAsyncQueue_Restart");
    createPersistedServerRegion(vm2, 11, 1, 2000, 5, homeDir, "testPersistedAsyncQueue_Restart");
    createPersistedServerRegion(vm3, 11, 1, 2000, 5, homeDir, "testPersistedAsyncQueue_Restart");

    // do some puts
    AsyncInvocation a0 = doAsyncPuts(vm0, "testPersistedAsyncQueue_Restart", 1, 50);
    AsyncInvocation a1 = doAsyncPuts(vm1, "testPersistedAsyncQueue_Restart", 40, 100);
    AsyncInvocation a2 = doAsyncPuts(vm2, "testPersistedAsyncQueue_Restart", 40, 100);
    AsyncInvocation a3 = doAsyncPuts(vm3, "testPersistedAsyncQueue_Restart", 90, 150);

    a3.join();
    a2.join();
    a1.join();
    a0.join();
    
    // close the cache 
    cacheClose (vm0, true);
    cacheClose (vm1, true);
    cacheClose (vm2, true);
    cacheClose (vm3, true);
    
    //recreate the cache and regions 
    a3 = createAsyncPersistedServerRegion(vm3, 11, 1, 2000, 5, homeDir, "testPersistedAsyncQueue_Restart");
    a2 = createAsyncPersistedServerRegion(vm2, 11, 1, 2000, 5, homeDir, "testPersistedAsyncQueue_Restart");
    a1 = createAsyncPersistedServerRegion(vm1, 11, 1, 2000, 5, homeDir, "testPersistedAsyncQueue_Restart");
    a0 = createAsyncPersistedServerRegion(vm0, 11, 1, 2000, 5, homeDir, "testPersistedAsyncQueue_Restart");
    
    a3.join();
    a2.join();
    a1.join();
    a0.join();
    
    // these gets should probably fetch the data from async queue 
    verifyGetsForValue(vm0, "testPersistedAsyncQueue_Restart", 1, 50, false);
    verifyGetsForValue(vm1, "testPersistedAsyncQueue_Restart", 40, 100, false);
    verifyGetsForValue(vm2, "testPersistedAsyncQueue_Restart", 40, 100, false);
    verifyGetsForValue(vm3, "testPersistedAsyncQueue_Restart", 90, 150, false);

    // these gets wait for sometime before fetching the data. this will ensure that 
    // the reads are done from HDFS
    verifyGetsForValue(vm0, "testPersistedAsyncQueue_Restart", 1, 50, true);
    verifyGetsForValue(vm1, "testPersistedAsyncQueue_Restart", 40, 100, true);
    verifyGetsForValue(vm2, "testPersistedAsyncQueue_Restart", 40, 100, true);
    verifyGetsForValue(vm3, "testPersistedAsyncQueue_Restart", 90, 150, true);

    cacheClose (vm0, false);
    cacheClose (vm1, false);
    cacheClose (vm2, false);
    cacheClose (vm3, false);

    disconnectFromDS();
  }

  /**
   * Stops a single server. A different node becomes primary for the buckets on 
   * the stopped node. Everything should work fine. 
   * Disabled due to bug 48067
   * 
   */
  public void testPersistedAsyncQueue_ServerRestart() throws Exception{
    disconnectFromDS();
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);

    String homeDir = "testPAQ_ServerRestart";
    
    createPersistedServerRegion(vm0, 11, 1, 2000, 5, homeDir, "testPAQ_ServerRestart");
    createPersistedServerRegion(vm1, 11, 1, 2000, 5, homeDir, "testPAQ_ServerRestart");
    createPersistedServerRegion(vm2, 11, 1, 2000, 5, homeDir, "testPAQ_ServerRestart");
    createPersistedServerRegion(vm3, 11, 1, 2000, 5, homeDir, "testPAQ_ServerRestart");


    AsyncInvocation a0 = doAsyncPuts(vm0, "testPAQ_ServerRestart", 1, 50);
    AsyncInvocation a1 = doAsyncPuts(vm1, "testPAQ_ServerRestart", 50, 75);
    AsyncInvocation a2 = doAsyncPuts(vm2, "testPAQ_ServerRestart", 75, 100);
    AsyncInvocation a3 = doAsyncPuts(vm3, "testPAQ_ServerRestart", 100, 150);

    a3.join();
    a2.join();
    a1.join();
    a0.join();
    
    cacheClose (vm0, false);
    
    // these gets should probably fetch the data from async queue 
    verifyGetsForValue(vm1, "testPAQ_ServerRestart", 1, 50, false);
    verifyGetsForValue(vm2, "testPAQ_ServerRestart", 40, 100, false);
    verifyGetsForValue(vm3, "testPAQ_ServerRestart", 70, 150, false);

    // these gets wait for sometime before fetching the data. this will ensure that 
    // the reads are done from HDFS
    verifyGetsForValue(vm2, "testPAQ_ServerRestart", 1, 100, true);
    verifyGetsForValue(vm3, "testPAQ_ServerRestart", 40, 150, true);

    cacheClose (vm1, false);
    cacheClose (vm2, false);
    cacheClose (vm3, false);

    disconnectFromDS();
  }
  
  
  private int createPersistedServerRegion(final VM vm, final int totalnumOfBuckets,
      final int batchSize, final int batchInterval, final int maximumEntries, 
      final String folderPath, final String uniqueName) {
    
    return (Integer) vm.invoke(new PersistedRegionCreation(vm, totalnumOfBuckets,
      batchSize, batchInterval, maximumEntries, folderPath, uniqueName));
  }
  private AsyncInvocation createAsyncPersistedServerRegion(final VM vm, final int totalnumOfBuckets,
      final int batchSize, final int batchInterval, final int maximumEntries, final String folderPath, 
      final String uniqueName) {
    
    return (AsyncInvocation) vm.invokeAsync(new PersistedRegionCreation(vm, totalnumOfBuckets,
      batchSize, batchInterval, maximumEntries, folderPath, uniqueName));
  }
  
  class PersistedRegionCreation extends SerializableCallable{
    private VM vm;
    private int totalnumOfBuckets;
    private int batchSize; 
    private int maximumEntries; 
    private String folderPath; 
    private String uniqueName; 
    private int  batchInterval;
    PersistedRegionCreation (final VM vm, final int totalnumOfBuckets,
      final int batchSize, final int batchInterval, final int maximumEntries, 
      final String folderPath, final String uniqueName) {
      this.vm = vm;
      this.totalnumOfBuckets = totalnumOfBuckets;
      this.batchSize = batchSize;
      this.maximumEntries = maximumEntries;
      this.folderPath = folderPath;
      this.uniqueName = uniqueName;
      this.batchInterval = batchInterval;
    }
    
    public Object call() throws Exception {
      
      AttributesFactory af = new AttributesFactory();
      af.setDataPolicy(DataPolicy.HDFS_PARTITION);
      PartitionAttributesFactory paf = new PartitionAttributesFactory();
      paf.setTotalNumBuckets(totalnumOfBuckets);
      paf.setRedundantCopies(1);

      af.setPartitionAttributes(paf.create());
      
      HDFSEventQueueAttributesFactory hqf= new HDFSEventQueueAttributesFactory();
      hqf.setBatchSizeMB(batchSize);
      hqf.setBatchTimeInterval(batchInterval);
      hqf.setPersistent(true);
      hqf.setDiskStoreName(uniqueName + vm.getPid());
      
      HDFSStoreFactory hsf = getCache().createHDFSStoreFactory();
      hsf.setHomeDir(folderPath);
      hsf.setHDFSEventQueueAttributes(hqf.create());

      DiskStore ds = getCache().createDiskStoreFactory().create(uniqueName + vm.getPid());

      af.setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(maximumEntries, EvictionAction.LOCAL_DESTROY));
      af.setHDFSStoreName(uniqueName);
      af.setHDFSWriteOnly(false);
        
      hsf.create(uniqueName);
      
      createRootRegion(uniqueName, af.create());

      return 0;
    }
  };

  private Cache createCacheForHDFS(Integer locPort) {
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + locPort + "]");
    InternalDistributedSystem ds = this.getSystem(props);
    return CacheFactory.create(ds);    
  }
  
  private int createServerRegion(final VM vm, final int totalnumOfBuckets, 
      final int batchSize, final int batchInterval, final int maximumEntries, 
      final String folderPath, final String uniqueName) {
    SerializableCallable createRegion = new SerializableCallable() {
      public Object call() throws Exception {
        AttributesFactory af = new AttributesFactory();
        af.setDataPolicy(DataPolicy.HDFS_PARTITION);
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setTotalNumBuckets(totalnumOfBuckets);
        paf.setRedundantCopies(1);
        af.setPartitionAttributes(paf.create());
        
        HDFSEventQueueAttributesFactory hqf= new HDFSEventQueueAttributesFactory();
        hqf.setBatchSizeMB(batchSize);
        hqf.setBatchTimeInterval(batchInterval);
        hqf.setPersistent(false);
        hqf.setMaximumQueueMemory(1);
        
        HDFSStoreFactory hsf = getCache().createHDFSStoreFactory();
        hsf.setHDFSEventQueueAttributes(hqf.create());
        hsf.setHomeDir(folderPath);
        hsf.create(uniqueName);
        af.setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(maximumEntries, EvictionAction.LOCAL_DESTROY));
        
        af.setHDFSWriteOnly(false);
        af.setHDFSStoreName(uniqueName);
        createRootRegion(uniqueName, af.create());
        
        return 0;
      }
    };

    return (Integer) vm.invoke(createRegion);
  }
  
  private void cacheClose(VM vm, final boolean sleep){
    vm.invoke( new SerializableCallable() {
      public Object call() throws Exception {
        if (sleep)
          Thread.sleep(2000);
        getCache().getLogger().info("Cache close in progress "); 
        getCache().close();
        getCache().getDistributedSystem().disconnect();
        getCache().getLogger().info("Cache closed");
        return null;
      }
    });
    
  }
  private AsyncInvocation cacheAsyncClose(VM vm){
    return vm.invokeAsync( new SerializableCallable() {
      public Object call() throws Exception {
        getCache().getLogger().info("Cache close in progress "); 
        getCache().close();
        getCache().getDistributedSystem().disconnect();
        getCache().getLogger().info("Cache closed");
        return null;
      }
    });
    
  }
  
  private Integer createFirstLocatorWithDSId( final int dsId) {
    int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME,"0");
    props.setProperty(DistributionConfig.DISTRIBUTED_SYSTEM_ID_NAME, ""+ dsId);
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + port + "]");
    props.setProperty(DistributionConfig.START_LOCATOR_NAME, "localhost[" + port + "],server=true,peer=true,hostname-for-clients=localhost");
    getSystem(props);
    return port;
  }
  
  private void doPuts(VM vm, final String regionName, final int start, final int end) throws Exception {
    vm.invoke( new SerializableCallable() {
      public Object call() throws Exception {
        Region r = getRootRegion(regionName);
        getCache().getLogger().info("Putting entries ");
        for (int i =start; i< end; i++)
          r.put("K" + i, "V"+ i );
        return null;
      }
      
    });
  }

  private AsyncInvocation doAsyncPuts(VM vm, final String regionName, 
      final int start, final int end) throws Exception {
    return vm.invokeAsync( new SerializableCallable() {
      public Object call() throws Exception {
        Region r = getRootRegion(regionName);
        getCache().getLogger().info("Putting entries ");
        for (int i =start; i< end; i++)
          r.put("K" + i, "V"+ i );
        return null;
      }
      
    });
  }
  private void verifyGetsForValue(VM vm, final String regionName, final int start, final int end, final boolean sleep) throws Exception {
    vm.invoke( new SerializableCallable() {
      public Object call() throws Exception {
        if (sleep) 
          Thread.sleep(2000);
        getCache().getLogger().info("Getting entries ");
        Region r = getRootRegion(regionName);
        for (int i =start; i< end; i++) {
          String k = "K" + i;
          Object s = r.get(k);
          String v = "V"+ i;
          assertTrue( "The expected key " + v+ " didn't match the received value " + s, v.equals(s));
        }
        return null;
      }
      
    });
    
  }
}
