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
package cacheperf.memory;

import cacheperf.memory.CacheSizePrms.KeyType;

import hydra.CacheHelper;
import hydra.HydraThreadLocal;
import hydra.Log;
import hydra.RemoteTestModule;
import hydra.TestConfig;
import hydra.blackboard.AnyCyclicBarrier;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import perffmwk.PerfStatMgr;
import perffmwk.TrimInterval;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.IndexExistsException;
import com.gemstone.gemfire.cache.query.IndexInvalidException;
import com.gemstone.gemfire.cache.query.IndexNameConflictException;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.RegionNotFoundException;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.cache.CachedDeserializable;
import com.gemstone.gemfire.internal.cache.PreferBytesCachedDeserializable;
import com.gemstone.gemfire.internal.size.ObjectGraphSizer;

import distcache.DistCache;
import distcache.DistCacheFactory;
import distcache.gemfire.GemFireCacheTestImpl;

/**
 * Hydra test case to calculate the overhead per entry with various cache
 * configurations. The final overhead is reported in the CacheSizeStats. The
 * overhead is also recorded to a CSV file "cache_size_data.csv" at intervals in
 * case we want to graph it. Because the test runs so quickly, and because we
 * really want to graph entry count vs. mem size, statistics are not a good
 * place to record the changes in memory size as the test is running.
 * 
 * @author dsmith
 * 
 */
public class CacheSizeClient {
  private static final LogWriter LOG = Log.getLogWriter();
  private static final HydraThreadLocal threadLocalInstance = new HydraThreadLocal();
  
  //This filter will exclude our test data from calculations of the cache size. What's left is the pure
  //cache overhead.
  private static final ObjectGraphSizer.ObjectFilter FILTER = new ObjectGraphSizer.ObjectFilter() {
    public boolean accept(Object parent, Object object) {
      boolean exclude = object instanceof TestObject
        || (parent instanceof CachedDeserializable && object instanceof byte[]);
      return !exclude;
    }
  };
  
  private DistCache cache;
  private CacheSizeStats statistics;
  
  //data
  private long emptyCacheMemSize;
  private long warmedUpCacheMemSize;
  private int warmedUpCacheSize;
  private final Writer dataFile;
  
  //<String,TrimInterval>
  private final Map trimIntervals = new HashMap();
  private int cacheSize;
  private long cacheMemSize;
  private long perEntryOverhead;
  private final AnyCyclicBarrier barrier;
  
  public CacheSizeClient() throws IOException {
    trimIntervals.put(CacheSizeStats.CACHE_SIZE, new TrimInterval());
    dataFile = new BufferedWriter(new FileWriter(Thread.currentThread().getName() + "_cache_size_data.csv", false));
    int numThreads = TestConfig.getInstance().getTotalThreads()
                   - TestConfig.getInstance().getThreadGroup("locator").getTotalThreads();
    LOG.info("configuring barrier with " + numThreads + " parties");
    barrier = AnyCyclicBarrier.lookup(numThreads,"CacheSize");
  }
  
  public static CacheSizeClient getInstance() throws IOException {
    CacheSizeClient instance = (CacheSizeClient) threadLocalInstance.get();
    if(instance == null) {
      instance = new CacheSizeClient();
      threadLocalInstance.set(instance);
    }
    
    return instance;
  }
  
  public static void openCacheTask() throws IOException {
    getInstance().openCache();
  }
  
  public static void closeCacheTask() throws IOException {
    getInstance().closeCache();
  }
  
  public static void openStatisticsTask() throws IOException {
    getInstance().openStatistics();
  }
  
  public static void closeStatisticsTask() throws IOException, IllegalArgumentException, InterruptedException, IllegalAccessException {
    getInstance().closeStatistics();
  }
  
  public static void createIndexTask() throws IOException, RegionNotFoundException, IndexInvalidException, IndexNameConflictException, IndexExistsException, UnsupportedOperationException {
    getInstance().createIndex();
  }

  public static void putDataTask() throws IllegalArgumentException, IllegalAccessException, IOException, InterruptedException {
    getInstance().putData();
  }
  
  public static void watchDataTask() throws IllegalArgumentException, IllegalAccessException, IOException, InterruptedException {
    getInstance().watchData();
  }
  
  public synchronized void openCache() {
    if(cache == null) {
      LOG.info("Opening cache");
      cache = DistCacheFactory.createInstance();
      cache.open();
      LOG.info("Opened cache");
    }
  }
  
  public synchronized void openStatistics() throws IOException {
    if ( this.statistics == null ) {
      LOG.info("Opening application statistics");
      statistics = CacheSizeStats.getInstance(RemoteTestModule.getCurrentThread().getThreadGroupName());
      dataFile.write("Time,Num Entries,Cache Mem Size,Per Entry Overhead from Sizer\n");
      LOG.info("Opened application statistics");
    }
  }
  
  public synchronized void closeCache() {
    if ( this.cache != null ) {
      LOG.info("Closing cache");
      cache.close();
      cache = null;
      LOG.info("Cache closed");
    }
  }
  
  public synchronized void closeStatistics() throws IOException, InterruptedException, IllegalArgumentException, IllegalAccessException {
    if ( this.statistics != null ) {
      LOG.info("Recording stats");
      writeStats();
      LOG.info("Closing stats");
      statistics.close();
      statistics = null;
      dataFile.close();
      LOG.info("Stats closed");
    }
  }
  
  private void createIndex() throws RegionNotFoundException, IndexInvalidException, IndexNameConflictException, IndexExistsException, UnsupportedOperationException {
    LOG.info("Creating Index");
    QueryService query = CacheHelper.getCache().getQueryService();
    Region region = ((GemFireCacheTestImpl) cache).getRegion();
    int indexCardinality = CacheSizePrms.getIndexCardinality();
    int numIndexedValues = CacheSizePrms.getNumIndexedValues();
    TestObject.uniqueValues = indexCardinality;
    TestObject.averageCollectionSize = numIndexedValues;
    if(numIndexedValues == 1) {
      query.createIndex("index", CacheSizePrms.getIndexType(), "indexValue", region.getFullPath());
    }
    else {
      query.createIndex("index", CacheSizePrms.getIndexType(), "val", region.getFullPath() + " region1, region1.indexCollection val TYPE int");
    }
    
    LOG.info("Index Created");

  }
  
  public void putData() throws IllegalArgumentException, IllegalAccessException, IOException, InterruptedException {
    int numberOfEntries = CacheSizePrms.getNumberOfEntries();
    int sampleInterval = CacheSizePrms.getSampleInterval();
    int trimStart = CacheSizePrms.getTrimStart();
    KeyType keyType = CacheSizePrms.getKeyType();

    recordInitialSize();
    
    for(int i = 0; i <= numberOfEntries; i++) {
      if(i % sampleInterval == 0) {
        record();
      }
      if(i == trimStart) {
        setWarmedUp();
      }
      
      Object key = null;
      switch (keyType) {
        case testobject:
          key = new TestObject(i);
          break;
        case string:
          key = String.valueOf(i);
          break;
        case integer:
          key = Integer.valueOf(i);
          break;
        default:
          String s = "Should not happen";
          throw new IllegalArgumentException(s);
      }
      Object value = new TestObject(i + numberOfEntries);
      cache.put(key, value);
    }
    
    recordFinalSize();
  }
  
  public void watchData() throws IllegalArgumentException, IllegalAccessException, IOException, InterruptedException {
    int numberOfEntries = CacheSizePrms.getNumberOfEntries();
    int sampleInterval = CacheSizePrms.getSampleInterval();
    int trimStart = CacheSizePrms.getTrimStart();

    recordInitialSize();
    
    for(int i = 0; i <= numberOfEntries / sampleInterval; i++) {
        record();
      if(i == trimStart / sampleInterval) {
        setWarmedUp();
      }
    }
    
    recordFinalSize();
  }
  
  private void recordInitialSize() throws IllegalArgumentException, IllegalAccessException {
    Assert.assertTrue(cache.size() == 0, "Cache size should be 0");
    emptyCacheMemSize= ObjectGraphSizer.size(cache,  FILTER, true);
    LOG.info("Waiting for all threads to size empty cache");
    barrier.await();
    LOG.info("Done Waiting");
  }
  
  private void recordFinalSize() throws IllegalArgumentException, IllegalAccessException, FileNotFoundException {
    LOG.info("Waiting for all threads to complete");
    barrier.await();
    LOG.info("Done Waiting");
  }

  /**
   * Records data to the statistics, to make it easier to report on with the perf reporter
   * @throws InterruptedException
   * @throws IllegalArgumentException
   * @throws IllegalAccessException
   */
  private void writeStats() throws InterruptedException, IllegalArgumentException, IllegalAccessException
  {
    //start trim
    long start = System.currentTimeMillis();
    for(Iterator itr = trimIntervals.values().iterator(); itr.hasNext();) {
      TrimInterval interval = (TrimInterval) itr.next();
      interval.setStart(start);
    }
    
    int objectSize = (int) ObjectGraphSizer.size(new TestObject(0), true);
    statistics.setEmptyCacheMemSize(emptyCacheMemSize);
    statistics.setWarmedUpCacheMemSize(warmedUpCacheMemSize);
    statistics.setObjectSize(objectSize);
    statistics.setCacheSize(cacheSize);
    statistics.setCacheMemSize(cacheMemSize);
    statistics.setPerEntryOverhead(perEntryOverhead);
    Thread.sleep(1000);
    
    //end trim
    long end = System.currentTimeMillis();
    for(Iterator itr = trimIntervals.values().iterator(); itr.hasNext(); ) {
      TrimInterval interval = (TrimInterval) itr.next();
      interval.setEnd(end);
    }
    PerfStatMgr.getInstance().reportTrimIntervals( trimIntervals );
  }
  
  private void setWarmedUp() throws IllegalArgumentException, IllegalAccessException {
    warmedUpCacheSize = cache.size();
    warmedUpCacheMemSize = ObjectGraphSizer.size(cache, FILTER, true);
  }

  private void record() throws IllegalArgumentException, IllegalAccessException, IOException, InterruptedException {
    LOG.info("Waiting for all threads to get to record method");
    barrier.await();
    LOG.info("Done Waiting");
    cacheSize = cache.size();
    
    cacheMemSize = ObjectGraphSizer.size(cache, FILTER, true);
    long cacheMemChange = cacheMemSize - warmedUpCacheMemSize;
    perEntryOverhead = warmedUpCacheSize == 0  || warmedUpCacheMemSize == 0 ? 0 : (cacheMemChange / (cacheSize - warmedUpCacheSize));
    dataFile.write(System.currentTimeMillis() + "," + cacheSize +  ","  + cacheMemSize + "," + perEntryOverhead  + "\n");
    LOG.info("Waiting for all threads to finish record method");
    barrier.await();
    LOG.info("Done Waiting");
  }
}
