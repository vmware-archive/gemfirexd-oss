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
package com.gemstone.gemfire.cache.query.partitioned;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.CacheUtils;
import com.gemstone.gemfire.cache.query.IndexStatistics;
import com.gemstone.gemfire.cache.query.IndexType;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.data.Position;
import com.gemstone.gemfire.cache.query.internal.index.CompactRangeIndex;
import com.gemstone.gemfire.cache.query.internal.index.IndexProtocol;
import com.gemstone.gemfire.cache.query.internal.index.MapRangeIndex;
import com.gemstone.gemfire.cache.query.internal.index.PartitionedIndex;
import com.gemstone.gemfire.cache.query.internal.index.RangeIndex;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;

import junit.framework.TestCase;

/**
 * @author shobhit
 *
 */
public class PRIndexStatisticsTest extends TestCase {

  static QueryService qs;
  static boolean isInitDone = false;
  static Region region;
  static IndexProtocol keyIndex1;
  static IndexProtocol keyIndex2;
  static IndexProtocol keyIndex3;

  /**
   * @param name
   */
  public PRIndexStatisticsTest(String name) {
    super(name);
  }

  @Override
  protected void setUp() throws Exception {
    try {
      CacheUtils.startCache();
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
  
  private void createAndPopulateRegion() {
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    AttributesFactory af = new AttributesFactory();
    af.setPartitionAttributes(paf.create());
    
    region = CacheUtils.createRegion("portfolio", af.create(), false);
    assertTrue(region instanceof PartitionedRegion);
    Position.cnt = 0;
    if(region.size() == 0){
      for(int i=0; i<100; i++){
        region.put(Integer.toString(i), new Portfolio(i, i));
      }
    }
    assertEquals(100, region.size());
    
  }
  
  private void createRegion() {
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    AttributesFactory af = new AttributesFactory();
    af.setPartitionAttributes(paf.create());
    
    region = CacheUtils.createRegion("portfolio", af.create(), false);
    assertTrue(region instanceof PartitionedRegion);
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
    createAndPopulateRegion();
    keyIndex1 = (IndexProtocol) qs.createIndex("multiKeyIndex1",
        IndexType.FUNCTIONAL, "pos.secId", "/portfolio p, p.positions.values pos");
    
    assertTrue(keyIndex1 instanceof PartitionedIndex);

    IndexStatistics keyIndex1Stats = keyIndex1.getStatistics();
    
    //Initial stats test (keys, values & updates)
    assertEquals(2*100/*Num of values in region*/, keyIndex1Stats.getNumberOfKeys());
    assertEquals(200, keyIndex1Stats.getNumberOfValues());
    assertEquals(200, keyIndex1Stats.getNumUpdates());
    
    for(int i=0; i<100; i++){
      region.put(Integer.toString(i), new Portfolio(i, i));
    }
    
    assertEquals(2*100/*Num of values in region*/, keyIndex1Stats.getNumberOfKeys());
    assertEquals(200, keyIndex1Stats.getNumberOfValues());
    assertEquals(400, keyIndex1Stats.getNumUpdates());
    
    //IndexUsed stats test
    String queryStr = "select * from /portfolio p, p.positions.values pos where pos.secId = 'YHOO'";
    Query query = qs.newQuery(queryStr);

    for(int i=0; i<50; i++){
      query.execute();
    }
 
    assertEquals(50*113, keyIndex1Stats.getTotalUses());
    
    //NumOfValues should be reduced.
    for(int i=0; i<50; i++){
      region.invalidate(Integer.toString(i));
    }
    
    assertEquals(100/*Num of values in region*/, keyIndex1Stats.getNumberOfKeys());
    assertEquals(100, keyIndex1Stats.getNumberOfValues());
    assertEquals(450, keyIndex1Stats.getNumUpdates());
    
    //Should not have any effect as invalidated values are destroyed
    for(int i=0; i<50; i++){
      region.destroy(Integer.toString(i));
    }
    
    assertEquals(100/*Num of values in region*/, keyIndex1Stats.getNumberOfKeys());
    assertEquals(100, keyIndex1Stats.getNumberOfValues());
    assertEquals(450, keyIndex1Stats.getNumUpdates());
    
    //NumOfKeys should get zero as all values are destroyed
    for(int i=50; i<100; i++){
      region.destroy(Integer.toString(i));
    }
    
    assertEquals(500, keyIndex1Stats.getNumUpdates());
    
    assertEquals(0, keyIndex1Stats.getNumberOfKeys());
    
    qs.removeIndex(keyIndex1);
    region.destroyRegion();
  }
  
  /**
   * Test CompactRenageIndex IndexStatistics for keys, values, updates and uses.
   * @throws Exception
   */
  public void testStatsForCompactRangeIndex() throws Exception{
    createAndPopulateRegion();
    keyIndex2 = (IndexProtocol) qs.createIndex("multiKeyIndex2",
        IndexType.FUNCTIONAL, "ID", "/portfolio ");
    
    assertTrue(keyIndex2 instanceof PartitionedIndex);

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
 
    assertEquals(50*113, keyIndex1Stats.getTotalUses());
    
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
    region.destroyRegion();
  }
  
  /**
   * Test MapRenageIndex IndexStatistics for keys, values, updates and uses.
   * @throws Exception
   */
  public void testStatsForMapRangeIndex() throws Exception{
    createAndPopulateRegion();
    keyIndex3 = (IndexProtocol) qs.createIndex("multiKeyIndex3",
        IndexType.FUNCTIONAL, "positions['DELL', 'YHOO']", "/portfolio");
    
    assertTrue(keyIndex3 instanceof PartitionedIndex);

    Object[] indexstats = ((PartitionedIndex)keyIndex3).getMapIndexStats().values().toArray();
    assertTrue(indexstats[0] instanceof IndexStatistics);
    assertTrue(indexstats[1] instanceof IndexStatistics);
    
    IndexStatistics keyIndex1Stats = (IndexStatistics)indexstats[0];
    IndexStatistics keyIndex2Stats = (IndexStatistics)indexstats[1];
        
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
    assertEquals(50 /*Execution time*/ * 50/* Total number of buckets*/, 
        keyIndex1Stats.getTotalUses());
    assertEquals(50 /*Execution time*/ * 50/* Total number of buckets*/, 
        keyIndex2Stats.getTotalUses());
    
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
    region.destroyRegion();
  }
  
  /**
   * Test RenageIndex IndexStatistics for keys, values, updates and uses.
   * @throws Exception
   */
  public void testStatsForRangeIndexBeforeRegionCreation() throws Exception{
    //Destroy region
    createRegion();
    assertEquals(0, region.size());
    
    keyIndex1 = (IndexProtocol) qs.createIndex("multiKeyIndex4",
        IndexType.FUNCTIONAL, "pos.secId", "/portfolio p, p.positions.values pos");
    
    //Recreate all entries in the region
    for(int i=0; i<100; i++){
      region.put(Integer.toString(i), new Portfolio(i, i));
    }
    
    assertTrue(keyIndex1 instanceof PartitionedIndex);
    
    IndexStatistics keyIndex1Stats = keyIndex1.getStatistics();
    
    //Initial stats test (keys, values & updates)
    assertEquals(2*100/*Num of values in region*/, keyIndex1Stats.getNumberOfKeys());
    assertEquals(200, keyIndex1Stats.getNumberOfValues());
    assertEquals(200, keyIndex1Stats.getNumUpdates());
    
    for(int i=0; i<100; i++){
      region.put(Integer.toString(i), new Portfolio(i, i));
    }
    
    assertEquals(2*100/*Num of values in region*/, keyIndex1Stats.getNumberOfKeys());
    assertEquals(200, keyIndex1Stats.getNumberOfValues());
    assertEquals(400, keyIndex1Stats.getNumUpdates());
    
    //IndexUsed stats test
    String queryStr = "select * from /portfolio p, p.positions.values pos where pos.secId = 'YHOO'";
    Query query = qs.newQuery(queryStr);

    for(int i=0; i<50; i++){
      query.execute();
    }
 
    assertEquals(50*113/*Num of buckets*/, keyIndex1Stats.getTotalUses());
    
    //NumOfValues should be reduced.
    for(int i=0; i<50; i++){
      region.invalidate(Integer.toString(i));
    }
    
    assertEquals(100/*Num of values in region*/, keyIndex1Stats.getNumberOfKeys());
    assertEquals(100, keyIndex1Stats.getNumberOfValues());
    assertEquals(450, keyIndex1Stats.getNumUpdates());
    
    //Should not have any effect as invalidated values are destroyed
    for(int i=0; i<50; i++){
      region.destroy(Integer.toString(i));
    }
    
    assertEquals(100/*Num of values in region*/, keyIndex1Stats.getNumberOfKeys());
    assertEquals(100, keyIndex1Stats.getNumberOfValues());
    assertEquals(450, keyIndex1Stats.getNumUpdates());
    
    //NumOfKeys should get zero as all values are destroyed
    for(int i=50; i<100; i++){
      region.destroy(Integer.toString(i));
    }
    
    assertEquals(500, keyIndex1Stats.getNumUpdates());
    
    assertEquals(0, keyIndex1Stats.getNumberOfKeys());
    
    qs.removeIndex(keyIndex1);
    region.destroyRegion();
  }
  
  /**
   * Test CompactRenageIndex IndexStatistics for keys, values, updates and uses.
   * @throws Exception
   */
  public void testStatsForCompactRangeIndexBeforeRegionCreation() throws Exception{
    //Destroy region
    createRegion();
    assertEquals(0, region.size());
    
    keyIndex2 = (IndexProtocol) qs.createIndex("multiKeyIndex5",
        IndexType.FUNCTIONAL, "ID", "/portfolio ");
    
    //Recreate all entries in the region
    for(int i=0; i<100; i++){
      region.put(Integer.toString(i), new Portfolio(i, i));
    }
    
    assertTrue(keyIndex2 instanceof PartitionedIndex);

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
 
    assertEquals(50*113, keyIndex1Stats.getTotalUses());
    
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
    region.destroyRegion();
  }
  
  /**
   * Test MapRenageIndex IndexStatistics for keys, values, updates and uses.
   * @throws Exception
   */
  public void testStatsForMapRangeIndexBeforeRegionCreation() throws Exception{
    //Destroy region
    createRegion();
    assertEquals(0, region.size());
    
    keyIndex3 = (IndexProtocol) qs.createIndex("multiKeyIndex6",
        IndexType.FUNCTIONAL, "positions['DELL', 'YHOO']", "/portfolio");
  
    //Recreate all entries in the region
    Position.cnt = 0;
    for(int i=0; i<100; i++){
      region.put(Integer.toString(i), new Portfolio(i, i));
    }
    assertTrue(keyIndex3 instanceof PartitionedIndex);

    Object[] indexstats = ((PartitionedIndex)keyIndex3).getMapIndexStats().values().toArray();
    assertTrue(indexstats[0] instanceof IndexStatistics);
    assertTrue(indexstats[1] instanceof IndexStatistics);
    
    IndexStatistics keyIndex1Stats = (IndexStatistics)indexstats[0];
    IndexStatistics keyIndex2Stats = (IndexStatistics)indexstats[1];
    
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
    assertEquals((50 /*Execution time*/ * 50/* Total number of buckets*/), 
        keyIndex1Stats.getTotalUses());

    assertEquals(50 /*Execution time*/ * 50/* Total number of buckets*/, 
        keyIndex2Stats.getTotalUses());

    
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
    region.destroyRegion();
  }
}
