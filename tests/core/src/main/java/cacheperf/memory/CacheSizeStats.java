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

import perffmwk.PerformanceStatistics;

import com.gemstone.gemfire.StatisticDescriptor;
import com.gemstone.gemfire.StatisticsType;

/**
 *  Implements statistics related to cache size tests.
 */
public class CacheSizeStats extends PerformanceStatistics {

  /** <code>CacheSizeStats</code> are maintained on a VM Basis*/
  private static final int SCOPE = VM_SCOPE;

  public static final String CACHE_SIZE = "cacheSize";
  public static final String CACHE_MEM_SIZE = "cacheMemSize";
  public static final String EMPTY_CACHE_MEM_SIZE = "emptyCacheMemSize";
  public static final String WARMED_UP_CACHE_MEM_SIZE = "warmedUpCacheMemSize";
  public static final String OBJECT_SIZE = "objectSize";
  public static final String PER_ENTRY_OVERHEAD = "perEntryOverhead";
  ////////////////////////  Static Methods  ////////////////////////

  /**
   * Returns the statistic descriptors for <code>CachePerfStats</code>
   */
  public static StatisticDescriptor[] getStatisticDescriptors() {
    boolean largerIsBetter = true;
    return new StatisticDescriptor[] {              
      factory().createIntGauge
        ( 
         CACHE_SIZE,
         "Final entry count in the cache.",
         "operations",
	 largerIsBetter
         ),
         
      factory().createLongGauge
        ( 
         WARMED_UP_CACHE_MEM_SIZE,
         "memory size of the cache after the trim interval has passed",
         "bytes",
	 !largerIsBetter
         ),
          factory().createLongGauge
          ( 
           CACHE_MEM_SIZE,
           "Final memory size of the cache.",
           "bytes",
           !largerIsBetter
           ),
           factory().createLongGauge
           ( 
            EMPTY_CACHE_MEM_SIZE,
            "Memory Size of the cache before the cache was populated",
            "bytes",
            !largerIsBetter
            ),
      factory().createLongGauge
        ( 
            OBJECT_SIZE,
         "The size of our test object",
         "bytes",
	 !largerIsBetter
         ),
         factory().createLongGauge
         ( 
          PER_ENTRY_OVERHEAD,
          "The change in cache size divided by the number of entries, minus the size of the data",
          "bytes",
          !largerIsBetter
          )
    };
  }

  public static CacheSizeStats getInstance(String name) {
    return (CacheSizeStats) getInstance( CacheSizeStats.class, SCOPE, name );
  }

  /////////////////// Construction / initialization ////////////////

  public CacheSizeStats( Class cls, StatisticsType type, int scope,
                    String instanceName, String trimspecName ) { 
    super( cls, type, scope, instanceName, trimspecName );
  }
  
  /////////////////// Updating stats /////////////////////////
  
  protected void setEmptyCacheMemSize(long memSize ) {
    statistics().setLong(EMPTY_CACHE_MEM_SIZE, memSize);
  }
  
  protected void setObjectSize(int objectSize) {
    statistics().setLong(OBJECT_SIZE,objectSize);
  }
  
  public void setWarmedUpCacheMemSize(long memSize) {
    statistics().setLong(WARMED_UP_CACHE_MEM_SIZE, memSize);
    
  }

  public void setCacheSize(int cacheSize) {
    statistics().setInt(CACHE_SIZE, cacheSize);
    
  }

  public void setCacheMemSize(long cacheMemSize) {
    statistics().setLong(CACHE_MEM_SIZE, cacheMemSize);
    
  }

  public void setPerEntryOverhead(long perEntryOverhead) {
    statistics().setLong(PER_ENTRY_OVERHEAD, perEntryOverhead);
    
  }
}
