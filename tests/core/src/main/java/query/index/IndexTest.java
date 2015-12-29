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

package query.index;

import hydra.CacheHelper;
import hydra.HydraRuntimeException;
import hydra.Log;
import hydra.RegionHelper;
import hydra.TestConfig;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import query.QueryTest;
import util.TestException;
import util.TestHelper;

import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.EvictionAlgorithm;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.cache.query.IndexExistsException;
import com.gemstone.gemfire.cache.query.IndexInvalidException;
import com.gemstone.gemfire.cache.query.IndexNameConflictException;
import com.gemstone.gemfire.cache.query.IndexType;
import com.gemstone.gemfire.cache.query.QueryException;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.internal.index.CompactRangeIndex;
import com.gemstone.gemfire.cache.query.internal.index.MapRangeIndex;
import com.gemstone.gemfire.cache.query.internal.index.PartitionedIndex;
import com.gemstone.gemfire.cache.query.internal.index.RangeIndex;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;

import distcache.gemfire.GemFireCachePrms;

/**
 * 
 * Class providing various Index related functionality for Querying and Indexing
 * hydra tests
 * 
 */
public class IndexTest {
  static protected IndexTest indexTest;

  /**
   * This method creates various predefined indexes on Portfolio objects Hydra
   * test user can specify the type of index he/she wants to create using
   * IndexPrms-indexType. Supported types are compactRange, range, mapRange User
   * can also specify indexType as 'all' thus creating indexes of all types.
   */
  // public static synchronized void HydraTask_CreateIndex() {
  // if (indexTest == null) {
  // indexTest = new IndexTest();
  // }
  // indexTest.createIndex(GemFireCachePrms.getRegionName());
  // }

  /**
   * This method creates various predefined indexes on Portfolio objects Hydra
   * test user can specify the type of index he/she wants to create using
   * IndexPrms-indexType. Supported types are compactRange, range, mapRange User
   * can also specify indexType as 'all' thus creating indexes of all types.
   * 
   * @param regionNumber
   */
  public void createIndex(int regionNumber) {
    String indexType = TestConfig.tab().stringAt(IndexPrms.indexType,
        "compactRange");
    if (indexType.equals("compactRange")) {
      createCompactRangeIndex(regionNumber);
    }
    else if (indexType.equals("mapRange")) {
      createMapIndex(regionNumber);
    }
    else if (indexType.equals("rangeIndex")) {
      createRangeIndex(regionNumber);
    }
    else if (indexType.equals("hashIndex")) {
      createHashIndex(regionNumber);
    }
    else if (indexType.equals("all")) {
      createCompactRangeIndex(regionNumber);
      createMapIndex(regionNumber);
      createRangeIndex(regionNumber);
    }
    else if (indexType.equals("allAndHash")) {
      createCompactRangeIndex(regionNumber);
      createMapIndex(regionNumber);
      createRangeIndex(regionNumber);
      createHashIndex(regionNumber);
    }
  }

  public void createIndex(String RegionName) {
    Region region = RegionHelper.getRegion(RegionName);
    if (region == null) {
      Log.getLogWriter().info("The region is not created properly");
    }
    else {
      if (!region.isDestroyed()) {
        Log.getLogWriter().info("Obtained the region with name :" + RegionName);
        QueryService qs = CacheHelper.getCache().getQueryService();
        if (qs != null) {
          try {
            long numOfIndexes = 1;
            numOfIndexes = TestConfig.tab().longAt(IndexPrms.numOfIndexes, 1);
            qs.createIndex("statusIndex", IndexType.FUNCTIONAL, "status", "/"
                + RegionName);
            Log.getLogWriter().info("Index statusIndex Created successfully");

            if (numOfIndexes >= 2) {
              qs.createIndex("idIndex", IndexType.FUNCTIONAL, "ID", "/"
                  + RegionName);
              Log.getLogWriter().info("Index idIndex Created successfully");
            }
            if (numOfIndexes >= 3) {
              qs.createIndex("mktValueIndex", IndexType.FUNCTIONAL,
                  "pVal.mktValue", "/" + RegionName
                      + " pf, pf.positions.values pVal TYPE Position",
                  "import parReg.\"query\".Position;");
              Log.getLogWriter().info(
                  "Index mktValueIndex Created successfully");
            }
            if (numOfIndexes >= 4) {
              qs.createIndex("nameIndex", IndexType.PRIMARY_KEY, "name", "/"
                  + RegionName);
              Log.getLogWriter().info("Index nameIndex Created successfully");
            } // developer needs this to debug
          }
          catch (IndexNameConflictException e) {
            Log.getLogWriter().info(
                "Caught expected IndexNameConflictException, continuing tests");
          }
          catch (IndexExistsException e) {
            Log.getLogWriter().info(
                "Caught expected IndexExistsException, continuing tests");
          }
          catch (QueryException e) {
            throw new HydraRuntimeException("Could not create Index "
                + TestHelper.getStackTrace(e));
          }
        }
        else {
          Log.getLogWriter().info(
              "Hey !!!!! Could not obtain QueryService for the cache ");
        }
      }
      else {
        Log.getLogWriter().info(
            "Hey !!!!! Region.isDestroyed() returned true for region : "
                + RegionName);
      }
    }
  }

  /**
   * Method for creation of compactRangeIndex. This method creates various
   * predefined compact range indexes on Portfolio objects
   * 
   * @param regionNumber
   */
  public void createCompactRangeIndex(int regionNumber) {
    String RegionName = QueryTest.REGION_NAME + regionNumber;
    Log.getLogWriter().info(
        "Creating Compact range index on region: " + RegionName);
    Region region = RegionHelper.getRegion(RegionName);

    if (region == null) {
      Log.getLogWriter().info("The region is not created properly");
    } else {
      if (!region.isDestroyed()) {
        Log.getLogWriter().info("Obtained the region with name :" + RegionName);
        QueryService qs = CacheHelper.getCache().getQueryService();
        if (qs != null) {
          try {

            Index i1 = qs.createIndex(QueryTest.STATUS_COMPACT_RANGE_INDEX
                + regionNumber, "status", "/" + RegionName);

            Index i2 = qs.createIndex(QueryTest.ID_COMPACT_RANGE_INDEX
                + regionNumber, "ID", "/" + RegionName);

            Log.getLogWriter().info(
                "Index idCompactRangeIndex Created successfully");
            if (region instanceof PartitionedRegion) {
              PartitionedIndex pindex = (PartitionedIndex) i1;
              List indexes = pindex.getBucketIndexes();
              for (Object index : indexes) {
                if (!(index instanceof CompactRangeIndex)) {
                  throw new TestException("IndexType was found to be: "
                      + index.getClass()
                      + " but Expected to be CompactRangeIndex.");
                }
              }
              PartitionedIndex pindex2 = (PartitionedIndex) i2;
              indexes = pindex2.getBucketIndexes();
              for (Object index : indexes) {
                if (!(index instanceof CompactRangeIndex)) {
                  throw new TestException("IndexType was found to be: "
                      + index.getClass()
                      + " but Expected to be CompactRangeIndex.");
                }
              }
            } else {
              if (!(i1 instanceof CompactRangeIndex)) {
                throw new TestException("IndexType was found to be: "
                    + i1.getClass() + " but Expected to be CompactRangeIndex.");
              }
              if (!(i2 instanceof CompactRangeIndex)) {
                throw new TestException("IndexType was found to be: "
                    + i2.getClass() + " but Expected to be CompactRangeIndex.");
              }
            }
          } catch (IndexNameConflictException e) {
            Log.getLogWriter().info(
                "Caught expected IndexNameConflictException, continuing tests");
          } catch (QueryException e) {
            throw new TestException("Could not create Index " + e);
          }
        } else {
          Log.getLogWriter().info(
              "Could not obtain QueryService for the cache ");
        }
      } else {
        Log.getLogWriter().info(
            "Region.isDestroyed() returned true for region : " + RegionName);
      }
    }
  }
  
  /**
   * Method for creation of hashIndex. This method creates various
   * predefined hash indexes on Portfolio objects
   * 
   * @param regionNumber
   */
  public void createHashIndex(int regionNumber) {
    String RegionName = QueryTest.REGION_NAME + regionNumber;
    Log.getLogWriter().info(
        "Creating hash index on region: " + RegionName);
    Region region = RegionHelper.getRegion(RegionName);

    if (region == null) {
      Log.getLogWriter().info("The region is not created properly");
    } else {
      if (!region.isDestroyed()) {
        Log.getLogWriter().info("Obtained the region with name :" + RegionName);
        QueryService qs = CacheHelper.getCache().getQueryService();
        if (qs != null) {
          try {

            Index i1 = qs.createHashIndex(QueryTest.STATUS_HASH_INDEX
                + regionNumber, "status", "/" + RegionName);

            Index i2 = qs.createHashIndex(QueryTest.ID_HASH_INDEX
                + regionNumber, "ID", "/" + RegionName);

            Log.getLogWriter().info(
                "Index idHashIndex Created successfully");
            if (region instanceof PartitionedRegion) {
              PartitionedIndex pindex = (PartitionedIndex) i1;
              List indexes = pindex.getBucketIndexes();
              for (Object index : indexes) {
                if (!(index instanceof CompactRangeIndex)) {
                  throw new TestException("IndexType was found to be: "
                      + index.getClass()
                      + " but Expected to be CompactRangeIndex.");
                }
                /*
                if (!(index instanceof HashIndex)) {
                  throw new TestException("IndexType was found to be: "
                      + index.getClass()
                      + " but Expected to be HashIndex.");
                }
                */
              }
              PartitionedIndex pindex2 = (PartitionedIndex) i2;
              indexes = pindex2.getBucketIndexes();
              for (Object index : indexes) {
                if (!(index instanceof CompactRangeIndex)) {
                  throw new TestException("IndexType was found to be: "
                      + index.getClass()
                      + " but Expected to be CompactRangeIndex.");
                }
                /*
                if (!(index instanceof HashIndex)) {
                  throw new TestException("IndexType was found to be: "
                      + index.getClass()
                      + " but Expected to be HashIndex.");
                }
                */
              }
            } else {
              if (!(i1 instanceof CompactRangeIndex)) {
                throw new TestException("IndexType was found to be: "
                    + i1.getClass() + " but Expected to be CompactRangeIndex.");
              }
              if (!(i2 instanceof CompactRangeIndex)) {
                throw new TestException("IndexType was found to be: "
                    + i2.getClass() + " but Expected to be CompactRangeIndex.");
              }
              /*
              if (!(i1 instanceof HashIndex)) {
                throw new TestException("IndexType was found to be: "
                    + i1.getClass() + " but Expected to be HashIndex.");
              }
              if (!(i2 instanceof HashIndex)) {
                throw new TestException("IndexType was found to be: "
                    + i2.getClass() + " but Expected to be HashIndex.");
              }
              */
            }
          } catch (IndexNameConflictException e) {
            Log.getLogWriter().info(
                "Caught expected IndexNameConflictException, continuing tests");
          } catch (QueryException e) {
            throw new TestException("Could not create Index " + e);
          }
        } else {
          Log.getLogWriter().info(
              "Could not obtain QueryService for the cache ");
        }
      } else {
        Log.getLogWriter().info(
            "Region.isDestroyed() returned true for region : " + RegionName);
      }
    }
  }

  /**
   * Method for creation of rangeIndex. This method creates various predefined
   * range indexes on Portfolio objects
   * 
   * @param regionNumber
   */
  public void createRangeIndex(int regionNumber) {
    String RegionName = QueryTest.REGION_NAME + regionNumber;
    Log.getLogWriter().info("Creating range index on region: " + RegionName);
    Region region = RegionHelper.getRegion(RegionName);
    region.getAttributes().getEvictionAttributes().getAction()
        .isOverflowToDisk();
    if (region == null) {
      Log.getLogWriter().info("The region is not created properly");
    }
    else {
      if (!region.isDestroyed()) {
        Log.getLogWriter().info("Obtained the region with name :" + RegionName);
        EvictionAttributes evAttr = region.getAttributes().getEvictionAttributes();
        if (evAttr.getAlgorithm() != null && evAttr.getAction() != null) {
          if (!evAttr.getAlgorithm().equals(EvictionAlgorithm.NONE)
              && !evAttr.getAction().equals(EvictionAction.NONE)) {
            // This is an overflow region
            Log.getLogWriter().info("Region :" + RegionName + " is an overflow region. So no range index creation");
            return;
          }
        }
        QueryService qs = CacheHelper.getCache().getQueryService();
        if (qs != null) {
          try {
            Index i1 = qs.createIndex(QueryTest.STATUS_RANGE_INDEX
                + regionNumber, "status", "/" + RegionName
                + " pf, pf.positions");
            Log.getLogWriter().info(
                "Index statusRangeIndex Created successfully");
            
            Index i2 = qs.createIndex(QueryTest.ID_RANGE_INDEX + regionNumber,
                "ID", "/" + RegionName + " pf, pf.positions");
            Log.getLogWriter().info("Index idRangeIndex Created successfully");
            
            if (region instanceof PartitionedRegion) {
              PartitionedIndex pindex = (PartitionedIndex)i1;
              List indexes = pindex.getBucketIndexes();
              for (Object index : indexes) {
                if (!(index instanceof RangeIndex)) {
                  throw new TestException("IndexType was found to be: "
                      + index.getClass() + "but Expected to be RangeIndex.");
                }
              }
              PartitionedIndex pindex2 = (PartitionedIndex)i2;
              indexes = pindex2.getBucketIndexes();
              for (Object index : indexes) {
                if (!(index instanceof RangeIndex)) {
                  throw new TestException("IndexType was found to be: "
                      + index.getClass() + "but Expected to be RangeIndex.");
                }
              }
            }
            else {
              if (!(i1 instanceof RangeIndex)) {
                throw new TestException("IndexType was found to be: "
                    + i1.getClass() + "but Expected to be RangeIndex.");
              }
              if (!(i2 instanceof RangeIndex)) {
                throw new TestException("IndexType was found to be: "
                    + i2.getClass() + "but Expected to be CompactRangeIndex.");
              }
            }
          } catch (IndexNameConflictException e) {
            Log.getLogWriter().info(
                "Caught expected IndexNameConflictException, continuing tests");
          } catch (QueryException e) {
            throw new TestException("Could not create Index " + e);
          }
        }
        else {
          Log.getLogWriter().info(
              "Could not obtain QueryService for the cache ");
        }
      }
      else {
        Log.getLogWriter().info(
            "Region.isDestroyed() returned true for region : " + RegionName);
      }
    }
  }

  /**
   * Method for creation of mapRangeIndex. This method creates various
   * predefined map range indexes on Portfolio objects
   * 
   * @param regionNumber
   */
  public void createMapIndex(int regionNumber) {
    String RegionName = QueryTest.REGION_NAME + regionNumber;
    Log.getLogWriter()
        .info("Creating Map range index on region: " + RegionName);
    Region region = RegionHelper.getRegion(RegionName);
    if (region == null) {
      Log.getLogWriter().info("The region is not created properly");
    }
    else {
      EvictionAttributes evAttr = region.getAttributes().getEvictionAttributes();
      Log.getLogWriter().info("*************************************************");
      Log.getLogWriter().info("INSIDE CreateMapIndex");
      Log.getLogWriter().info("RegionName: " + RegionName);
      Log.getLogWriter().info("EvAttr: " + evAttr);
      Log.getLogWriter().info("evAttr.getAlgorithm() " + evAttr.getAlgorithm().toString());
      Log.getLogWriter().info("evAttr.getAction() " + evAttr.getAction().toString());
      Log.getLogWriter().info("*************************************************");
      if (evAttr.getAlgorithm() != null && evAttr.getAction() != null) {
        if (!evAttr.getAlgorithm().equals(EvictionAlgorithm.NONE)
            && !evAttr.getAction().equals(EvictionAction.NONE)) {
          // This is an overflow region
          Log.getLogWriter().info("Region :" + RegionName + " is an overflow region. So no map range index creation");
          return;
        }
      }
      if (!region.isDestroyed()) {
        Log.getLogWriter().info("Obtained the region with name :" + RegionName);
        QueryService qs = CacheHelper.getCache().getQueryService();

        if (qs != null) {
          try {
            Index i1 = qs.createIndex(QueryTest.MAP_RANGE_INDEX_1
                + regionNumber, "pf.positions[*]", "/" + RegionName + " pf");
            
            Index i2 = qs.createIndex(QueryTest.MAP_RANGE_INDEX_2
                + regionNumber, "pf.positions['SUN','IBM','YHOO']", "/"+ RegionName + " pf");

            
            if (region instanceof PartitionedRegion) {
              PartitionedIndex pindex = (PartitionedIndex) i1;
              List indexes = pindex.getBucketIndexes();
              for (Object index : indexes) {
                if (!(index instanceof MapRangeIndex)) {
                  throw new TestException("IndexType was found to be: "
                      + index.getClass() + "but Expected to be RangeIndex.");
                }
              }
              PartitionedIndex pindex2 = (PartitionedIndex) i2;
              indexes = pindex2.getBucketIndexes();
              for (Object index : indexes) {
                if (!(index instanceof MapRangeIndex)) {
                  throw new TestException("IndexType was found to be: "
                      + index.getClass() + "but Expected to be RangeIndex.");
                }
              }
            } else {
              if (!(i1 instanceof MapRangeIndex)) {
                throw new TestException("IndexType was found to be: "
                    + i1.getClass() + "but Expected to be MapRangeIndex.");
              }

              if (!(i2 instanceof MapRangeIndex)) {
                throw new TestException("IndexType was found to be: "
                    + i2.getClass() + "but Expected to be MapRangeIndex.");
              }
            }
            Log.getLogWriter().info(
                "Index " + QueryTest.MAP_RANGE_INDEX_1 + regionNumber
                    + " created successfully!!");
            Log.getLogWriter().info(
                "Index " + QueryTest.MAP_RANGE_INDEX_2 + regionNumber
                    + " created successfully!!");
            
          } catch (IndexNameConflictException e) {
            Log.getLogWriter().info(
                "Caught expected IndexNameConflictException, continuing tests");
          }
          catch (IndexInvalidException e) {
            e.printStackTrace();
            throw new TestException("Caught IndexInvalidException: " + e);
          }
          catch (QueryException e) {
            throw new TestException("Could not create Index " + e);
          }
        }
        else {
          Log.getLogWriter().info(
              "Could not obtain QueryService for the cache ");
        }
      }
      else {
        Log.getLogWriter().info(
            "Region.isDestroyed() returned true for region : " + RegionName);
      }
    }
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
    }
    else {
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
