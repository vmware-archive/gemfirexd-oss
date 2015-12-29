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
package query.common.index;

import hydra.CacheHelper;
import hydra.HydraRuntimeException;
import hydra.HydraVector;
import hydra.Log;
import hydra.RegionHelper;
import hydra.TestConfig;


import java.util.Collection;
import java.util.Iterator;

import query.common.QueryTest;
import util.TestException;
import util.TestHelper;

import cacheperf.CachePerfPrms;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.cache.query.IndexExistsException;
import com.gemstone.gemfire.cache.query.IndexNameConflictException;
import com.gemstone.gemfire.cache.query.QueryException;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.internal.Assert;

import distcache.gemfire.GemFireCachePrms;

public class IndexTest {
  static protected IndexTest indexTest;

  /**
   * This method creates various predefined indexes on Portfolio objects Hydra
   * test user can specify the type of index he/she wants to create using
   * IndexPrms-indexType. Supported types are compactRange, range, mapRange User
   * can also specify indexType as 'all' thus creating indexes of all types.
   */
  public static void HydraTask_createIndexes() {
    if (indexTest == null) {
      indexTest = new IndexTest();
    }
    indexTest.createIndexes();
  }

  protected void createIndexes() {
    String []  indexes = null;
    String []  indexFromClauses = null;
    HydraVector hvIndexesExprs = TestConfig.tab()
    .vecAt(CachePerfPrms.queryIndex, null);

    if (hvIndexesExprs != null) {
      indexes = new String[hvIndexesExprs.size()];
      for (int i  = 0; i < hvIndexesExprs.size(); i++) {
        indexes[i] = (String)hvIndexesExprs.get(i);
      }
    }
    
    HydraVector hvIndexesFromClauses = TestConfig.tab()
    .vecAt(CachePerfPrms.queryFromClause, null);
    if (hvIndexesFromClauses != null) {
      indexFromClauses = new String[hvIndexesFromClauses.size()];
      for (int i  = 0; i < hvIndexesFromClauses.size(); i++) {
        indexFromClauses[i] = (String)hvIndexesFromClauses.get(i);
      }
    }
    if (hvIndexesExprs != null) {
      Assert
      .assertTrue(indexFromClauses.length == indexes.length,
          "Number of index expressions and index fromclauses in conf file are not same.");
      for (int i = 0; i < indexes.length; i++) {
        try {
          Log.getLogWriter().info("Creating index: " + "indexedExpression: " + indexes[i] + " fromClause: " + indexFromClauses[i]);
          CacheHelper.getCache().getQueryService()
              .createIndex("index" + i, indexes[i], indexFromClauses[i]);
        }
        catch (IndexExistsException e) {
          Log.getLogWriter().info("index already created");
        }
        catch (IndexNameConflictException e) {
          Log.getLogWriter().info("index already created");
        }
        catch (QueryException e) {
          String s = "Problem creating index: " + indexes[i] + " " + indexFromClauses[i];
          throw new HydraRuntimeException(s, e);
        }
      }
    }
  }
  
  /**
   * This method creates various predefined indexes on Portfolio objects Hydra
   * test user can specify the type of index he/she wants to create using
   * IndexPrms-indexType. Supported types are compactRange, range, mapRange User
   * can also specify indexType as 'all' thus creating indexes of all types.
   * 
   * @param regionName
   */
  public void createIndex(int regionNumber) {
    String RegionName = QueryTest.REGION_NAME + regionNumber;
  }

  /**
   * Hydra task for removing indexes. In most of the tests this method is used
   * as an operation along-with other region operations for testing concurrent
   * behavior.
   */
  public static synchronized void HydraTask_RemoveIndex() {
    if (indexTest == null) {
      indexTest = new IndexTest();
    }
    indexTest.removeIndex(GemFireCachePrms.getRegionName());
  }

  /**
   * Actual instance method for removing indexes. In most of the tests this
   * method is used as an operation along-with other region operations for
   * testing concurrent behavior. Removes the first available index from the
   * region
   */
  public void removeIndex(String RegionName) {
    Region region = RegionHelper.getRegion(RegionName);
    if (region == null) {
      Log.getLogWriter().info("The region is not created properly");
    }else {
      Log.getLogWriter().info("Obtained the region with name :" + RegionName);
      QueryService qs = CacheHelper.getCache().getQueryService();
      if (qs != null) {
        try {
          Collection indexes = qs.getIndexes(region);
          if (indexes == null) {
            return; // no IndexManager defined
          }
          if (indexes.size() == 0) {
            return; // no indexes defined
          }
          Iterator iter = indexes.iterator();
          if (iter.hasNext()) {
            Index idx = (Index)(iter.next());
            String name = idx.getName();
            qs.removeIndex(idx);
            Log.getLogWriter().info("Index " + name + " removed successfully");
          }
        }
        catch (Exception e) {
          throw new TestException("Could not remove Index "
              + TestHelper.getStackTrace(e));
        }
      }
      else {
        Log.getLogWriter().info("Could not obtain QueryService for the cache ");
      }
    }
  }
}
