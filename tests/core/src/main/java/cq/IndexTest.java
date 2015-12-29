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
package cq;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.IndexExistsException;
import com.gemstone.gemfire.cache.query.IndexNameConflictException;
import com.gemstone.gemfire.cache.query.IndexType;
import com.gemstone.gemfire.cache.query.QueryException;
import com.gemstone.gemfire.cache.query.QueryService;

import query.index.IndexPrms;

import hydra.*;
import util.TestException;
import distcache.gemfire.GemFireCachePrms;

/**
 * @author eshu
 *  This IndexTest will create index on Position object.
 */
public class IndexTest {
  static protected IndexTest indexTest;
  
  public static synchronized void HydraTask_CreateIndex() {
     if(indexTest == null) {
         indexTest = new IndexTest();
     }
     indexTest.createIndex(GemFireCachePrms.getRegionName());
  }
  
  /**
   * To create index for a region.
   * use IndexPrms.numOfIndexes to determine how many indexes to be created, default is 1
   * The first Index is to create primary index. (actually no primary index could be created as
   * Position does not have such attribute as the keys in the region.) 
   * Next index to be created is functional index on qty
   * The last index to be created is functional index on mktValue.
   * @param regionName - to determine the region in which index will be created
   */
  protected void createIndex(String regionName) {

    try{
      long numOfIndexes = 1;
      numOfIndexes = TestConfig.tab().longAt(IndexPrms.numOfIndexes, 1); 
      createNonExistPrimaryIndex(regionName); 
      
      if (numOfIndexes >= 2) {
        createQtyIndex(regionName);
      }
      
      if (numOfIndexes >= 3) {
        createMktValueIndex(regionName);
      }
           
    } catch (IndexNameConflictException e) {
      Log.getLogWriter().info("Caught expected IndexNameConflictException, continuing tests");
    } catch (IndexExistsException e) {
        Log.getLogWriter().info("Caught expected IndexExistsException, continuing tests");
    } catch(QueryException e) {
       throw new TestException("Could not create Index " + e.getStackTrace());
    }  
  }

  //Position does not have any attributes that are the same as keys in the region. 
  //therefore no primary index could be created. However, product should throw exception when 
  //a non existent field is used to create primary index.
  protected void createNonExistPrimaryIndex(String regionName) throws 
  IndexExistsException, IndexNameConflictException, QueryException {    
      QueryService qs = getQueryService (regionName);
      qs.createIndex("secIdIndex", IndexType.PRIMARY_KEY, "secIdIndex", "/" + regionName);
  }
  
  /**
   * create primary index in the specified region
   * @param regionName -- to determine the region in which primaryIndex will be created
   */
  protected void createPrimaryIndex(String regionName){  
    try{
      createSecIdIndex(regionName);
    } catch (IndexNameConflictException e) {
      Log.getLogWriter().info("Caught expected IndexNameConflictException, continuing tests");
    } catch (IndexExistsException e) {
        Log.getLogWriter().info("Caught expected IndexExistsException, continuing tests");
    } catch(QueryException e) {
       throw new TestException("Could not create Index " + e.getStackTrace());
    }  
  }
  
  /* To create primary index on secId. 
   * @param regionName - to determine the region in which index will be created.  
   */ 
   protected void createSecIdIndex(String regionName) throws  
   IndexExistsException, IndexNameConflictException, QueryException { 
      QueryService qs = getQueryService (regionName); 
      qs.createIndex("secIdIndex", IndexType.PRIMARY_KEY, "secId", "/" + regionName); 
      Log.getLogWriter().info("Primary index secIdIndex Created successfully"); 
   } 
   //this is the test issue which causes the test to fail (bug 38025).
   //test will fail if create index on secId which is not key in the region.
  
  
  /**
   * To create functional index on qty.
   * @param regionName - to determine the region in which index will be created.
   * @throws IndexExistsException
   * @throws IndexNameConflictException
   * @throws QueryException
   */
  protected void createQtyIndex(String regionName) throws IndexExistsException, IndexNameConflictException, QueryException {
    QueryService qs = getQueryService (regionName);
    qs.createIndex("qtyIndex", IndexType.FUNCTIONAL, "qty", "/" + regionName);
    Log.getLogWriter().info("Functinal index qtyIndex Created successfully");
  }
  
  /**
   * To create functional index on mktValue
   * @param regionName - to determine the region in which index will be created.
   * @throws IndexExistsException
   * @throws IndexNameConflictException
   * @throws QueryException
   */
  protected void createMktValueIndex(String regionName) throws IndexExistsException, IndexNameConflictException, QueryException {
    QueryService qs = getQueryService (regionName);
    qs.createIndex("mktValueIndex", IndexType.FUNCTIONAL, "mktValue", "/" + regionName);
    Log.getLogWriter().info("Functinal index mktValueIndex Created successfully");
  }
  
  /**
   * method to get the QueryService
   * @param regionName - to determine the region in which index will be created is created correctly.
   * @return QuerySerive to be used to create index
   */
  protected QueryService getQueryService(String regionName) {
    QueryService qs = null;
    Region region = RegionHelper.getRegion(regionName);
    if(region == null) {
       Log.getLogWriter().info("The region is not created properly");
    } else {
      Log.getLogWriter().info("Obtained the region with name :" + regionName);
      qs = CacheHelper.getCache().getQueryService();      
    }
    return qs;
  }

}
