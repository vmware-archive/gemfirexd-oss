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
package objects.query.largeobject;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.FunctionDomainException;
import com.gemstone.gemfire.cache.query.IndexExistsException;
import com.gemstone.gemfire.cache.query.IndexNameConflictException;
import com.gemstone.gemfire.cache.query.IndexType;
import com.gemstone.gemfire.cache.query.NameResolutionException;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryInvocationTargetException;
import com.gemstone.gemfire.cache.query.RegionNotFoundException;
import com.gemstone.gemfire.cache.query.TypeMismatchException;

import hydra.CacheHelper;
import hydra.GsRandom;
import hydra.HydraConfigException;
import hydra.Log;
//import hydra.PoolHelper;
import hydra.RegionHelper;
import hydra.TestConfig;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import objects.query.BaseGFEQueryFactory;
import objects.query.QueryObjectException;
import objects.query.QueryPrms;

public class GFELargeObjectQueryFactory extends BaseGFEQueryFactory {

  protected static final GsRandom rng = TestConfig.tab().getRandGen();
  /*
   * for performance purposes, used to store total positions so 
   * that we don't need to recalculate for every query.
   */

  private int totalLargeObjects;
  private static Region LargeObjectRegion;

  public GFELargeObjectQueryFactory() {
   
  }

  protected static synchronized void setRegion(Region r) {
    LargeObjectRegion = r;
  }
  
  public String getPreparedQuery(int queryType) {
    return null; 
   }
  
  public String getQuery(int queryType, int i) {
    return null; 
   }
  //--------------------------------------------------------------------------
  // Inserts
  //--------------------------------------------------------------------------

  public List getPreparedInsertObjects() {
    List pobjs = new ArrayList();
    LargeObject pobj= new LargeObject();
    pobjs.add(pobj);
    
    return pobjs;
  }

  // @todo Logging (either move it or implement it here)
  /**
   * @throw QueryObjectException if bid has already been inserted.
   */
  private Class misc;
  private Method getGemFireKeyMethod;
  private Method getLargeObjectMethod;
  
  public Map fillAndExecutePreparedInsertObjects(List pobjs, int i) {
    Map map = new HashMap();
    Object key = null;
    Object pobj = null;
  
    int apiType = QueryPrms.getAPI();
    if ( apiType == QueryPrms.GFE_GFK_DVD || apiType == QueryPrms.GFE_GFK) {
      //Use a gemfirekey
      //GemFireKey   
      if (getGemFireKeyMethod == null) {
        try {
          misc = Class.forName("com.pivotal.gemfirexd.internal.engine.Misc");
          getGemFireKeyMethod = misc.getMethod("getGemFireKey", new Class[] {int.class});
        }
        catch(Exception e) {
          Log.getLogWriter().info(e);
        }
      }
      
      try {
        Object[] idArray = {i};
        key = getGemFireKeyMethod.invoke(null, idArray);   
      }
      catch (Exception e) {
        Log.getLogWriter().info(e);
      }
    }
    else {
      //use a simple object key
      key = new Integer(i);
    }
    
    if (apiType == QueryPrms.GFE_GFK_DVD ) {
      if (getLargeObjectMethod == null) {
        try {
          misc = Class.forName("com.pivotal.gemfirexd.internal.engine.Misc");
          getLargeObjectMethod = misc.getMethod("getLargeObject", new Class[] {int.class});
        }
        catch(Exception e) {
          Log.getLogWriter().info(e);
        }
      }
      try {
        Object[] idArray = {i};
        pobj = getLargeObjectMethod.invoke(null, idArray);   
      }
      catch (Exception e) {
        Log.getLogWriter().info(e);
      }
    }
    else {
      LargeObject lo = (LargeObject) pobjs.get(0);   
      lo.init(i);
      pobj = lo;
    }

    if (logUpdates) {
     Log.getLogWriter().info("Executing insert: " + pobj);
    }
    map.put(key, pobj);
    LargeObjectRegion.put(key, pobj);
    if (logUpdates) {
      Log.getLogWriter().info("Executed insert: " + pobj + ":" + i);
    }
    return map;
  }

  
  //--------------------------------------------------------------------------
  // QueryFactory : constraints
  //--------------------------------------------------------------------------
  public List getConstraintStatements() {
    //DO SOMETHING
    return new ArrayList();
  }

  //--------------------------------------------------------------------------
  // QueryFactory : Query Types
  //--------------------------------------------------------------------------
  public int getQueryType() {
    return LargeObjectPrms.getQueryType(QueryPrms.GFE);
  }
  
  public int getUpdateQueryType() {
    return LargeObjectPrms.getUpdateQueryType(QueryPrms.GFE);
  }
  
  public int getDeleteQueryType() {
    return LargeObjectPrms.getDeleteQueryType(QueryPrms.GFE);
  }
  //--------------------------------------------------------------------------
  // QueryFactory : primary keys
  //--------------------------------------------------------------------------
  public void createPrimaryKeyIndexOnLargeObjectId() throws RegionNotFoundException,
      IndexExistsException, IndexNameConflictException {
    Cache cache = CacheHelper.getCache();
    cache.getQueryService()
        .createIndex("largeobjectpk", IndexType.PRIMARY_KEY,
            LargeObject.getTableShortName() + ".id",
            "/" + LargeObject.getTableAndShortName());
    //return "alter table " + LargeObject.getTableName() + " add primary key(id)";
  }

  //--------------------------------------------------------------------------
  // QueryFactory : indexes
  //--------------------------------------------------------------------------
  public void createIndexes() throws RegionNotFoundException,
      IndexExistsException, IndexNameConflictException {
    Vector indexTypes = LargeObjectPrms.getIndexTypes();
    for (Iterator i = indexTypes.iterator(); i.hasNext();) {
      String indexTypeString = (String) i.next();
      Log.getLogWriter().info(
          "GFELargeObjectQueryFactory: creating index:" + indexTypeString);
      int indexType = LargeObjectPrms.getIndexType(indexTypeString);
      createIndex(indexType);
    }
  }

  public void createIndex(int type) throws RegionNotFoundException,
      IndexExistsException, IndexNameConflictException {
    switch (type) {
      case LargeObjectPrms.PRIMARY_KEY_INDEX_ON_LARGE_OBJECT_ID_QUERY:
        createPrimaryKeyIndexOnLargeObjectId();
        break;
      case LargeObjectPrms.UNIQUE_INDEX_ON_LARGE_OBJECT_ID_QUERY:
        getUniqueIndexOnLargeObjectId();
        break;
      case LargeObjectPrms.NO_QUERY:
        break;
      default:
        throw new UnsupportedOperationException("Should not happen");
    }
  }

  public void getUniqueIndexOnLargeObjectId() throws RegionNotFoundException,
      IndexExistsException, IndexNameConflictException {
    Cache cache = CacheHelper.getCache();
    cache.getQueryService().createIndex("largeobject_unique_id_idx",
        IndexType.FUNCTIONAL, LargeObject.getTableShortName() + ".id",
        "/" + LargeObject.getTableAndShortName());
  }
  
  //--------------------------------------------------------------------------
  // QueryFactory : Table statements
  //--------------------------------------------------------------------------
  public void createRegions() {
    setRegion(RegionHelper.createRegion(LargeObject.getTableName(), LargeObjectPrms.getLargeObjectRegionConfig()));
    /*
    try {
    cacheperf.CachePerfClientVersion.assignBucketsToPartitions(LargeObjectRegion);
    } catch (InterruptedException e) {
      Log.getLogWriter().severe("Interrupted during bucket creation");
    }
    */
  }

  //--------------------------------------------------------------------------
  // Read result sets
  //--------------------------------------------------------------------------
  public void readResultSet(int queryType, Object rs) {
    switch (queryType) {
      case LargeObjectPrms.RANDOM_EQUALITY_ON_LARGE_OBJECT_ID_QUERY:
        readResultSet(queryType, rs, LargeObject.getFields(LargeObjectPrms.getLargeObjectFieldsAsVector()));
        break;
      default:
        readResultSet(queryType, rs, LargeObject.getFields(LargeObjectPrms.getLargeObjectFieldsAsVector()));
        break;
    }
  }

  public Object[] deadArray = new Object[41];
  public void readResultSet(int queryType, Object rs, List fields) {
    int rsSize = 1;
    LargeObject lo = (LargeObject) rs;
    deadArray[0] = lo.getId();
    deadArray[1] = lo.getStringField1();
    deadArray[2] = lo.getStringField2();
    deadArray[3] = lo.getStringField3();
    deadArray[4] = lo.getStringField4();
    deadArray[5] = lo.getStringField5();
    deadArray[6] = lo.getStringField6();
    deadArray[7] = lo.getStringField7();
    deadArray[8] = lo.getStringField8();
    deadArray[9] = lo.getStringField9();
    deadArray[10] = lo.getStringField10();
    deadArray[11] = lo.getStringField11();
    deadArray[12] = lo.getStringField12();
    deadArray[13] = lo.getStringField13();
    deadArray[14] = lo.getStringField14();
    deadArray[15] = lo.getStringField15();
    deadArray[16] = lo.getStringField16();
    deadArray[17] = lo.getStringField17();
    deadArray[18] = lo.getStringField18();
    deadArray[19] = lo.getStringField19();
    deadArray[20] = lo.getStringField20();
    
    deadArray[21] = lo.getIntField1();
    deadArray[22] = lo.getIntField2();
    deadArray[23] = lo.getIntField3();
    deadArray[24] = lo.getIntField4();
    deadArray[25] = lo.getIntField5();
    deadArray[26] = lo.getIntField6();
    deadArray[27] = lo.getIntField7();
    deadArray[28] = lo.getIntField8();
    deadArray[29] = lo.getIntField9();
    deadArray[30] = lo.getIntField10();
    deadArray[31] = lo.getIntField11();
    deadArray[32] = lo.getIntField12();
    deadArray[33] = lo.getIntField13();
    deadArray[34] = lo.getIntField14();
    deadArray[35] = lo.getIntField15();
    deadArray[36] = lo.getIntField16();
    deadArray[37] = lo.getIntField17();
    deadArray[38] = lo.getIntField18();
    deadArray[39] = lo.getIntField19();
    deadArray[40] = lo.getIntField20();
    
    if (logQueryResultSize) {
      Log.getLogWriter().info("Returned " + rsSize + " rows");
    }
    if (validateResults) {
      int expectedSize = getResultSetSize(queryType);
      if (rsSize != expectedSize) {
        throw new HydraConfigException("ResultSetSize expected("
            + expectedSize + ") did not match actual("
            + rsSize + ")");
      }
    }
  }

  /**
   * Returns the expected size of the result set for the specified query type.
   */
  private int getResultSetSize(int queryType) {
    switch (queryType) {
      case LargeObjectPrms.RANDOM_EQUALITY_ON_LARGE_OBJECT_ID_QUERY:
        return 1;
      default:
        String s = "Query type not supported yet: " + queryType;
        throw new UnsupportedOperationException("");
    }
  }

  //direct update
  public void directUpdate(int i, int queryType) {
    //get query type
    if (queryType == LargeObjectPrms.GET_AND_PUT_LARGE_OBJECT_BY_LARGE_OBJECT_ID_QUERY) {
      getObjectAndPut(i, queryType);
    }
    else if (queryType == LargeObjectPrms.PUT_NEW_LARGE_OBJECT_BY_LARGE_OBJECT_ID_QUERY) {
      newObjectPut(i, queryType);
    }
   
  }
  
  /**
   * does a get, modifies the object, and puts it back (a delta in this case)
   */
  private void getObjectAndPut(int i, int queryType) {
    Region region = getRegionForQuery(queryType);
    Integer key = Integer.valueOf(i);
    LargeObject lo = (LargeObject)region.get(key);
    lo.update(rng.nextInt(1,20));
    region.put(key, lo);
  }

  /**
   * puts new objects into the region instead of doing a get followed by a put
   */
  private void newObjectPut(int i, int queryType ) {
    Region region = getRegionForQuery(queryType);
    LargeObject lo = new LargeObject();
    Integer key = new Integer(i);
    lo.init(i);
    region.put(key, lo);
  }

  //We know all queries will be on LargeObject so we return our own region
  public Region getRegionForQuery(int queryType) {
    return LargeObjectRegion;
  }
}
