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
/**
 * 
 */
package com.gemstone.gemfire.cache.query.internal.index;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.CacheUtils;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.cache.query.IndexStatistics;
import com.gemstone.gemfire.cache.query.IndexType;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.data.Numbers;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.data.Position;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * @author shobhit
 *
 */
public class IndexStatisticsTest extends TestCase {

  static QueryService qs;
  static boolean isInitDone = false;
  static Region region;
  static IndexProtocol keyIndex1;
  static IndexProtocol keyIndex2;
  static IndexProtocol keyIndex3;

  public IndexStatisticsTest(String testName) {
    super(testName);
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    junit.textui.TestRunner.run(suite());
  }

  public static Test suite() {
    TestSuite suite = new TestSuite(IndexStatisticsTest.class);
    return suite;
  }

  @Override
  protected void setUp() throws Exception {
    try {
      CacheUtils.startCache();
      region = CacheUtils.createRegion("portfolio", Portfolio.class);
      Position.cnt = 0;
      if(region.size() == 0){
        for(int i=0; i<100; i++){
          region.put(Integer.toString(i), new Portfolio(i, i));
        }
      }
      assertEquals(100, region.size());
      qs = CacheUtils.getQueryService();
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  protected void tearDown() throws Exception {
    CacheUtils.closeCache();
  }
  
  /*public static Test suite() {
    TestSuite suite = new TestSuite(IndexMaintenanceTest.class);
    return suite;
  }*/
  /**
   * Test RenageIndex IndexStatistics for keys, values, updates and uses.
   * @throws Exception
   */
  public void testStatsForRangeIndex() throws Exception{
    keyIndex1 = (IndexProtocol) qs.createIndex("multiKeyIndex1",
        IndexType.FUNCTIONAL, "pos.secId", "/portfolio p, p.positions.values pos");
    
    assertTrue(keyIndex1 instanceof RangeIndex);

    IndexStatistics keyIndex1Stats = keyIndex1.getStatistics();
    
    //Initial stats test (keys, values & updates)
    assertEquals(4, keyIndex1Stats.getNumberOfKeys());
    assertEquals(200, keyIndex1Stats.getNumberOfValues());
    assertEquals(200, keyIndex1Stats.getNumUpdates());
    
    for(int i=0; i<100; i++){
      region.put(Integer.toString(i), new Portfolio(i, i));
    }
    
    assertEquals(4, keyIndex1Stats.getNumberOfKeys());
    assertEquals(200, keyIndex1Stats.getNumberOfValues());
    assertEquals(400, keyIndex1Stats.getNumUpdates());
    
    //IndexUsed stats test
    String queryStr = "select * from /portfolio p, p.positions.values pos where pos.secId = 'YHOO'";
    Query query = qs.newQuery(queryStr);

    for(int i=0; i<50; i++){
      query.execute();
    }
 
    assertEquals(50, keyIndex1Stats.getTotalUses());
    
    //NumOfValues should be reduced.
    for(int i=0; i<50; i++){
      region.invalidate(Integer.toString(i));
    }
    
    assertEquals(4, keyIndex1Stats.getNumberOfKeys());
    assertEquals(100, keyIndex1Stats.getNumberOfValues());
    assertEquals(450, keyIndex1Stats.getNumUpdates());
    
    //Should not have any effect as invalidated values are destroyed
    for(int i=0; i<50; i++){
      region.destroy(Integer.toString(i));
    }
    
    assertEquals(4, keyIndex1Stats.getNumberOfKeys());
    assertEquals(100, keyIndex1Stats.getNumberOfValues());
    assertEquals(450, keyIndex1Stats.getNumUpdates());
    
    //NumOfKeys should get zero as all values are destroyed
    for(int i=50; i<100; i++){
      region.destroy(Integer.toString(i));
    }
    
    assertEquals(500, keyIndex1Stats.getNumUpdates());
    
    assertEquals(0, keyIndex1Stats.getNumberOfKeys());
    
    qs.removeIndex(keyIndex1);
  }
  
  /**
   * Test CompactRenageIndex IndexStatistics for keys, values, updates and uses.
   * @throws Exception
   */
  public void testStatsForCompactRangeIndex() throws Exception{

    keyIndex2 = (IndexProtocol) qs.createIndex("multiKeyIndex2",
        IndexType.FUNCTIONAL, "ID", "/portfolio ");
    
    assertTrue(keyIndex2 instanceof CompactRangeIndex);

    IndexStatistics keyIndex1Stats = keyIndex2.getStatistics();
    
    //Initial stats test (keys, values & updates)
    assertEquals(100, keyIndex1Stats.getNumberOfKeys());
    assertEquals(100, keyIndex1Stats.getNumberOfValues());
    assertEquals(100, keyIndex1Stats.getNumUpdates());
    
    for(int i=0; i<100; i++){
      region.put(Integer.toString(i), new Portfolio(i, i));
    }
    
    assertEquals(100, keyIndex1Stats.getNumberOfKeys());
    assertEquals(100, keyIndex1Stats.getNumberOfValues());
    assertEquals(200, keyIndex1Stats.getNumUpdates());
    
    //IndexUsed stats test
    String queryStr = "select * from /portfolio where ID > 0";
    Query query = qs.newQuery(queryStr);

    for(int i=0; i<50; i++){
      query.execute();
    }
 
    assertEquals(50, keyIndex1Stats.getTotalUses());
    
    //NumOfValues should be reduced.
    for(int i=0; i<50; i++){
      region.invalidate(Integer.toString(i));
    }
    
    assertEquals(50, keyIndex1Stats.getNumberOfKeys());
    assertEquals(50, keyIndex1Stats.getNumberOfValues());
    assertEquals(250, keyIndex1Stats.getNumUpdates());
    
    for(int i=0; i<50; i++){
      region.destroy(Integer.toString(i));
    }
    
    assertEquals(50, keyIndex1Stats.getNumberOfKeys());
    assertEquals(50, keyIndex1Stats.getNumberOfValues());
    assertEquals(250, keyIndex1Stats.getNumUpdates());
    
    //NumOfKeys should get zero as all values are destroyed
    for(int i=50; i<100; i++){
      region.destroy(Integer.toString(i));
    }
    
    assertEquals(300, keyIndex1Stats.getNumUpdates());
    
    assertEquals(0, keyIndex1Stats.getNumberOfKeys());
    
    qs.removeIndex(keyIndex2);
  }
  
  /**
   * Test MapRenageIndex IndexStatistics for keys, values, updates and uses.
   * @throws Exception
   */
  public void testStatsForMapRangeIndex() throws Exception{
    
    keyIndex3 = (IndexProtocol) qs.createIndex("multiKeyIndex3",
        IndexType.FUNCTIONAL, "positions['DELL', 'YHOO']", "/portfolio");
    
    assertTrue(keyIndex3 instanceof MapRangeIndex);

    Object[] indexes = ((MapRangeIndex)keyIndex3).getRangeIndexHolderForTesting().values().toArray();
    assertTrue(indexes[0] instanceof RangeIndex);
    assertTrue(indexes[1] instanceof RangeIndex);
    
    IndexStatistics keyIndex1Stats = ((RangeIndex)indexes[0]).getStatistics();
    IndexStatistics keyIndex2Stats = ((RangeIndex)indexes[1]).getStatistics();
    
    assertEquals(50, keyIndex1Stats.getNumberOfKeys());
    assertEquals(50, keyIndex1Stats.getNumberOfValues());
    assertEquals(50, keyIndex1Stats.getNumUpdates());
    assertEquals(50, keyIndex2Stats.getNumberOfKeys());
    assertEquals(50, keyIndex2Stats.getNumberOfValues());
    assertEquals(50, keyIndex2Stats.getNumUpdates());

    Position.cnt = 0;
    for(int i=0; i<100; i++){
      region.put(Integer.toString(i), new Portfolio(i, i));
    }
    
    assertEquals(50, keyIndex1Stats.getNumberOfKeys());
    assertEquals(50, keyIndex1Stats.getNumberOfValues());
    assertEquals(100, keyIndex1Stats.getNumUpdates());
    assertEquals(50, keyIndex2Stats.getNumberOfKeys());
    assertEquals(50, keyIndex2Stats.getNumberOfValues());
    assertEquals(100, keyIndex2Stats.getNumUpdates());
    
    String queryStr = "select * from /portfolio where positions['DELL'] != NULL OR positions['YHOO'] != NULL";
    Query query = qs.newQuery(queryStr);

    for(int i=0; i<50; i++){
      query.execute();
    }

    //Both RangeIndex should be used
    assertEquals(50, keyIndex1Stats.getTotalUses());
    assertEquals(50, keyIndex2Stats.getTotalUses());
    
    for(int i=0; i<50; i++){
      region.invalidate(Integer.toString(i));
    }
    
    assertEquals(25, keyIndex1Stats.getNumberOfKeys());
    assertEquals(25, keyIndex1Stats.getNumberOfValues());
    assertEquals(150, keyIndex1Stats.getNumUpdates());
    assertEquals(25, keyIndex2Stats.getNumberOfKeys());
    assertEquals(25, keyIndex2Stats.getNumberOfValues());
    assertEquals(150, keyIndex2Stats.getNumUpdates());
    
    for(int i=0; i<50; i++){
      region.destroy(Integer.toString(i));
    }
    
    assertEquals(25, keyIndex1Stats.getNumberOfKeys());
    assertEquals(25, keyIndex1Stats.getNumberOfValues());
    assertEquals(150, keyIndex1Stats.getNumUpdates());
    assertEquals(25, keyIndex2Stats.getNumberOfKeys());
    assertEquals(25, keyIndex2Stats.getNumberOfValues());
    assertEquals(150, keyIndex2Stats.getNumUpdates());
    
    for(int i=50; i<100; i++){
      region.destroy(Integer.toString(i));
    }
    
    assertEquals(200, keyIndex1Stats.getNumUpdates());
    assertEquals(200, keyIndex1Stats.getNumUpdates());
    
    assertEquals(0, keyIndex1Stats.getNumberOfKeys());
    assertEquals(0, keyIndex2Stats.getNumberOfKeys());
    
    qs.removeIndex(keyIndex3);
  }
  
  /**
   * Test RenageIndex IndexStatistics for keys, values, updates and uses.
   * @throws Exception
   */
  public void testStatsForRangeIndexBeforeRegionCreation() throws Exception{
    //Destroy region
    region.clear();
    assertEquals(0, region.size());
    Position.cnt = 0;
    
    keyIndex1 = (IndexProtocol) qs.createIndex("multiKeyIndex4",
        IndexType.FUNCTIONAL, "pos.secId", "/portfolio p, p.positions.values pos");
    
    //Recreate all entries in the region
    for(int i=0; i<100; i++){
      region.put(Integer.toString(i), new Portfolio(i, i));
    }
    
    assertTrue(keyIndex1 instanceof RangeIndex);
    
    IndexStatistics keyIndex1Stats = keyIndex1.getStatistics();
    
    //Initial stats test (keys, values & updates)
    assertEquals(4, keyIndex1Stats.getNumberOfKeys());
    assertEquals(200, keyIndex1Stats.getNumberOfValues());
    assertEquals(200, keyIndex1Stats.getNumUpdates());
    
    for(int i=0; i<100; i++){
      region.put(Integer.toString(i), new Portfolio(i, i));
    }
    
    assertEquals(4, keyIndex1Stats.getNumberOfKeys());
    assertEquals(200, keyIndex1Stats.getNumberOfValues());
    assertEquals(400, keyIndex1Stats.getNumUpdates());
    
    //IndexUsed stats test
    String queryStr = "select * from /portfolio p, p.positions.values pos where pos.secId = 'YHOO'";
    Query query = qs.newQuery(queryStr);

    for(int i=0; i<50; i++){
      query.execute();
    }
 
    assertEquals(50, keyIndex1Stats.getTotalUses());
    
    //NumOfValues should be reduced.
    for(int i=0; i<50; i++){
      region.invalidate(Integer.toString(i));
    }
    
    assertEquals(4, keyIndex1Stats.getNumberOfKeys());
    assertEquals(100, keyIndex1Stats.getNumberOfValues());
    assertEquals(450, keyIndex1Stats.getNumUpdates());
    
    //Should not have any effect as invalidated values are destroyed
    for(int i=0; i<50; i++){
      region.destroy(Integer.toString(i));
    }
    
    assertEquals(4, keyIndex1Stats.getNumberOfKeys());
    assertEquals(100, keyIndex1Stats.getNumberOfValues());
    assertEquals(450, keyIndex1Stats.getNumUpdates());
    
    //NumOfKeys should get zero as all values are destroyed
    for(int i=50; i<100; i++){
      region.destroy(Integer.toString(i));
    }
    
    assertEquals(500, keyIndex1Stats.getNumUpdates());
    
    assertEquals(0, keyIndex1Stats.getNumberOfKeys());
    
    qs.removeIndex(keyIndex1);
  }
  
  /**
   * Test CompactRenageIndex IndexStatistics for keys, values, updates and uses.
   * @throws Exception
   */
  public void testStatsForCompactRangeIndexBeforeRegionCreation() throws Exception{
    //Destroy region
    region.clear();
    assertEquals(0, region.size());
    Position.cnt = 0;
    
    keyIndex2 = (IndexProtocol) qs.createIndex("multiKeyIndex5",
        IndexType.FUNCTIONAL, "ID", "/portfolio ");
    
    //Recreate all entries in the region
    for(int i=0; i<100; i++){
      region.put(Integer.toString(i), new Portfolio(i, i));
    }
    
    assertTrue(keyIndex2 instanceof CompactRangeIndex);

    IndexStatistics keyIndex1Stats = keyIndex2.getStatistics();
    
    //Initial stats test (keys, values & updates)
    assertEquals(100, keyIndex1Stats.getNumberOfKeys());
    assertEquals(100, keyIndex1Stats.getNumberOfValues());
    assertEquals(100, keyIndex1Stats.getNumUpdates());
    
    for(int i=0; i<100; i++){
      region.put(Integer.toString(i), new Portfolio(i, i));
    }
    
    assertEquals(100, keyIndex1Stats.getNumberOfKeys());
    assertEquals(100, keyIndex1Stats.getNumberOfValues());
    assertEquals(200, keyIndex1Stats.getNumUpdates());
    
    //IndexUsed stats test
    String queryStr = "select * from /portfolio where ID > 0";
    Query query = qs.newQuery(queryStr);

    for(int i=0; i<50; i++){
      query.execute();
    }
 
    assertEquals(50, keyIndex1Stats.getTotalUses());
    
    //NumOfValues should be reduced.
    for(int i=0; i<50; i++){
      region.invalidate(Integer.toString(i));
    }
    
    assertEquals(50, keyIndex1Stats.getNumberOfKeys());
    assertEquals(50, keyIndex1Stats.getNumberOfValues());
    assertEquals(250, keyIndex1Stats.getNumUpdates());
    
    for(int i=0; i<50; i++){
      region.destroy(Integer.toString(i));
    }
    
    assertEquals(50, keyIndex1Stats.getNumberOfKeys());
    assertEquals(50, keyIndex1Stats.getNumberOfValues());
    assertEquals(250, keyIndex1Stats.getNumUpdates());
    
    //NumOfKeys should get zero as all values are destroyed
    for(int i=50; i<100; i++){
      region.destroy(Integer.toString(i));
    }
    
    assertEquals(300, keyIndex1Stats.getNumUpdates());
    
    assertEquals(0, keyIndex1Stats.getNumberOfKeys());
    
    qs.removeIndex(keyIndex2);
  }
  
  /**
   * Test MapRenageIndex IndexStatistics for keys, values, updates and uses.
   * @throws Exception
   */
  public void testStatsForMapRangeIndexBeforeRegionCreation() throws Exception{
    //Destroy region
    region.clear();
    assertEquals(0, region.size());
    Position.cnt = 0;
    
    keyIndex3 = (IndexProtocol) qs.createIndex("multiKeyIndex6",
        IndexType.FUNCTIONAL, "positions['DELL', 'YHOO']", "/portfolio");
    Object[] indexes = ((MapRangeIndex)keyIndex3).getRangeIndexHolderForTesting().values().toArray();
    assertEquals(indexes.length, 0);

    //Recreate all entries in the region
    Position.cnt = 0;
    for(int i=0; i<100; i++){
      region.put(Integer.toString(i), new Portfolio(i, i));
    }
    assertTrue(keyIndex3 instanceof MapRangeIndex);
    indexes = ((MapRangeIndex)keyIndex3).getRangeIndexHolderForTesting().values().toArray();
    assertTrue(indexes[0] instanceof RangeIndex);
    assertTrue(indexes[1] instanceof RangeIndex);
    
    IndexStatistics keyIndex1Stats = ((RangeIndex)indexes[0]).getStatistics();
    IndexStatistics keyIndex2Stats = ((RangeIndex)indexes[1]).getStatistics();
    
    assertEquals(50, keyIndex1Stats.getNumberOfKeys());
    assertEquals(50, keyIndex1Stats.getNumberOfValues());
    assertEquals(50, keyIndex1Stats.getNumUpdates());
    assertEquals(50, keyIndex2Stats.getNumberOfKeys());
    assertEquals(50, keyIndex2Stats.getNumberOfValues());
    assertEquals(50, keyIndex2Stats.getNumUpdates());

    Position.cnt = 0;
    for(int i=0; i<100; i++){
      region.put(Integer.toString(i), new Portfolio(i, i));
    }
    
    assertEquals(50, keyIndex1Stats.getNumberOfKeys());
    assertEquals(50, keyIndex1Stats.getNumberOfValues());
    assertEquals(100, keyIndex1Stats.getNumUpdates());
    assertEquals(50, keyIndex2Stats.getNumberOfKeys());
    assertEquals(50, keyIndex2Stats.getNumberOfValues());
    assertEquals(100, keyIndex2Stats.getNumUpdates());
    
    String queryStr = "select * from /portfolio where positions['DELL'] != NULL OR positions['YHOO'] != NULL";
    Query query = qs.newQuery(queryStr);

    for(int i=0; i<50; i++){
      query.execute();
    }

    //Both RangeIndex should be used
    assertEquals(50, keyIndex1Stats.getTotalUses());
    assertEquals(50, keyIndex2Stats.getTotalUses());
    
    for(int i=0; i<50; i++){
      region.invalidate(Integer.toString(i));
    }
    
    assertEquals(25, keyIndex1Stats.getNumberOfKeys());
    assertEquals(25, keyIndex1Stats.getNumberOfValues());
    assertEquals(150, keyIndex1Stats.getNumUpdates());
    assertEquals(25, keyIndex2Stats.getNumberOfKeys());
    assertEquals(25, keyIndex2Stats.getNumberOfValues());
    assertEquals(150, keyIndex2Stats.getNumUpdates());
    
    for(int i=0; i<50; i++){
      region.destroy(Integer.toString(i));
    }
    
    assertEquals(25, keyIndex1Stats.getNumberOfKeys());
    assertEquals(25, keyIndex1Stats.getNumberOfValues());
    assertEquals(150, keyIndex1Stats.getNumUpdates());
    assertEquals(25, keyIndex2Stats.getNumberOfKeys());
    assertEquals(25, keyIndex2Stats.getNumberOfValues());
    assertEquals(150, keyIndex2Stats.getNumUpdates());
    
    for(int i=50; i<100; i++){
      region.destroy(Integer.toString(i));
    }
    
    assertEquals(200, keyIndex1Stats.getNumUpdates());
    assertEquals(200, keyIndex1Stats.getNumUpdates());
    
    assertEquals(0, keyIndex1Stats.getNumberOfKeys());
    assertEquals(0, keyIndex2Stats.getNumberOfKeys());
    
    qs.removeIndex(keyIndex3);
  }
  
  
  public void testCompactRangeIndexNumKeysStats() throws Exception {
    String regionName = "testCompactRegionIndexNumKeysStats_region";
    Region region = CacheUtils.createRegion(regionName, Numbers.class);

    Index index = qs.createIndex("idIndexName", "r.max1", "/" + regionName
        + " r");
    IndexStatistics stats = index.getStatistics();

    // Add an object and check stats
    Numbers obj1 = new Numbers(1);
    obj1.max1 = 20;
    region.put(1, obj1);
    assertEquals(1, stats.getNumberOfValues());
    assertEquals(1, stats.getNumberOfKeys());
    // assertEquals(1, stats.getNumberOfValues(20f));
    assertEquals(1, stats.getNumUpdates());

    // add a second object with the same index key
    Numbers obj2 = new Numbers(1);
    obj2.max1 = 20;
    region.put(2, obj2);
    assertEquals(2, stats.getNumberOfValues());
    assertEquals(1, stats.getNumberOfKeys());
    // assertEquals(2, stats.getNumberOfValues(20f));
    assertEquals(2, stats.getNumUpdates());

    // remove the second object and check that keys are 1
    region.remove(2);
    assertEquals(1, stats.getNumberOfValues());
    assertEquals(1, stats.getNumberOfKeys());
    // assertEquals(1, stats.getNumberOfValues(20f));
    assertEquals(3, stats.getNumUpdates());

    // remove the first object and check that keys are 0
    region.remove(1);
    assertEquals(0, stats.getNumberOfValues());
    assertEquals(0, stats.getNumberOfKeys());
    // assertEquals(0, stats.getNumberOfValues(20f));
    assertEquals(4, stats.getNumUpdates());

    // add object with a different key and check results
    obj2.max1 = 21;
    region.put(3, obj2);
    assertEquals(1, stats.getNumberOfValues());
    assertEquals(1, stats.getNumberOfKeys());
    // assertEquals(0, stats.getNumberOfValues(20f));
    assertEquals(5, stats.getNumUpdates());

    // add object with original key and check that num keys are 2
    obj1.max1 = 20;
    region.put(1, obj1);
    assertEquals(2, stats.getNumberOfValues());
    assertEquals(2, stats.getNumberOfKeys());
    // assertEquals(1, stats.getNumberOfValues(20f));
    assertEquals(6, stats.getNumUpdates());
  }
}
