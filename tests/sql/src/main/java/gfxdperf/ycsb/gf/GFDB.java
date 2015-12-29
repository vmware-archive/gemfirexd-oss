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
package gfxdperf.ycsb.gf;

import com.gemstone.gemfire.cache.GemFireCache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.distributed.DistributedSystem;

import gfxdperf.PerfTestException;
import gfxdperf.ycsb.core.ByteIterator;
import gfxdperf.ycsb.core.ByteArrayByteIterator;
import gfxdperf.ycsb.core.DB;
import gfxdperf.ycsb.core.DBException;
import gfxdperf.ycsb.core.workloads.CoreWorkloadPrms;
import gfxdperf.ycsb.core.workloads.CoreWorkloadStats;

import hydra.CacheHelper;
import hydra.ConfigPrms;
import hydra.DistributedSystemHelper;
import hydra.Log;
import hydra.RegionHelper;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Client for measuring YCSB performance with GemFire.
 * <p> 
 * This class expects a schema <key> <field1> <field2> <field3> ...
 * All attributes are of type VARCHAR. All accesses are through the primary key.
 */
public class GFDB extends DB {

  public static final boolean logOps = GFPrms.logOps();

  /** Prefix for each column in the table */
  public static String COLUMN_PREFIX = "field";

  /** Primary key column in the table */
  public static final String PRIMARY_KEY = "YCSB_KEY";

  private GemFireCache cache;
  private DistributedSystem distributedSystem;
  private CoreWorkloadStats statistics;

  @Override
  public void init() throws DBException {
    this.distributedSystem = DistributedSystemHelper.getDistributedSystem();
    this.cache = CacheHelper.createCache(ConfigPrms.getCacheConfig());
    this.statistics = CoreWorkloadStats.getInstance();
  }

  @Override
  public void cleanup() throws DBException {
    if (this.statistics != null) {
      this.statistics.close();
      this.statistics = null;
    }
    if (this.distributedSystem != null) {
      this.distributedSystem.disconnect();
      this.distributedSystem = null;
    }
  }

//------------------------------------------------------------------------------
// DB

  @Override
  public int read(String table, String key, Set<String> fields,
      HashMap<String, ByteIterator> result) {
    if (logOps) {
      Log.getLogWriter().info("Reading key=" + key);
    }
    long start = this.statistics.startRead();
    Region<String, Map<String, byte[]>> r = getRegion(table);
    Map<String, byte[]> val = r.get(key);
    if (val == null) {
      String s = "No results reading key=" + key + " from " + table;
      throw new PerfTestException(s);
    } else {
      if (fields == null) {
        for (String k : val.keySet()) {
          result.put(k, new ByteArrayByteIterator(val.get(k)));
        }
      } else {
        for (String field : fields) {
          result.put(field, new ByteArrayByteIterator(val.get(field)));
        }
      }
    }
    this.statistics.endRead(start, 1);
    if (logOps) {
      Log.getLogWriter().info("Read key=" + key);
    }
    return 0;
  }

  @Override
  public int update(String table, String key, HashMap<String, ByteIterator> values) {
    if (logOps) {
      Log.getLogWriter().info("Updating key=" + key);
    }
    long start = this.statistics.startUpdate();
    getRegion(table).put(key, convertToBytearrayMap(values));
    this.statistics.endUpdate(start, 1);
    if (logOps) {
      Log.getLogWriter().info("Updated key=" + key);
    }
    return 0;
  }

  @Override
  public int insert(String table, String key, HashMap<String, ByteIterator> values) {
    if (logOps) {
      Log.getLogWriter().info("Inserting key=" + key);
    }
    long start = this.statistics.startInsert();
    getRegion(table).create(key, convertToBytearrayMap(values));
    this.statistics.endInsert(start, 1);
    if (logOps) {
      Log.getLogWriter().info("Inserted key=" + key);
    }
    return 0;
  }

  @Override
  public int delete(String table, String key) {
    if (logOps) {
      Log.getLogWriter().info("Deleting key=" + key);
    }
    long start = this.statistics.startDelete();
    Object o = getRegion(table).destroy(key);
    if (o == null) {
      String s = "Failed deleting key=" + key + " from " + table;
      throw new PerfTestException(s);
    }
    this.statistics.endDelete(start, 1);
    if (logOps) {
      Log.getLogWriter().info("Deleted key=" + key);
    }
    return 0;
  }

  private Map<String, byte[]> convertToBytearrayMap(Map<String,ByteIterator> values) {
    Map<String, byte[]> retVal = new HashMap<String, byte[]>();
    for (String key : values.keySet()) {
      retVal.put(key, values.get(key).toArray());
    }
    return retVal;
  }

  private Region<String, Map<String, byte[]>> getRegion(String table) {
    Region<String, Map<String, byte[]>> r = this.cache.getRegion(table);
    if (r == null) {
      r = RegionHelper.createRegion(CoreWorkloadPrms.getTableName(),
                                    ConfigPrms.getRegionConfig());
    }
    return r;
  }
}
