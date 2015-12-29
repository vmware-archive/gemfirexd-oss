
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

package parReg.query;

import java.util.*;
//import java.rmi.*;

//import query.QueryPrms;
import hydra.*;
import util.*;
import hct.*;

import com.gemstone.gemfire.cache.*;
//import com.gemstone.gemfire.cache.util.*;
//import com.gemstone.gemfire.distributed.*;
import com.gemstone.gemfire.cache.query.*;   // SelectResults
import com.gemstone.gemfire.cache.util.BridgeWriterException;

/**
 * Contains Hydra tasks and supporting methods for testing the GemFire
 * hierarchical cache (cache server) on query in partitioned region.
 */
public class ConcBridgeNotify extends BridgeNotify {

  private int numOfId = 100;    
  private int numOfRegions = 1;         //only one region for this test
  /**
   * Creates and initializes the singleton instance of BridgeNotify
   * in this VM.
   */
  public synchronized static void HydraTask_initialize() {
    if (bridgeClient == null) {
      bridgeClient = new ConcBridgeNotify();
      bridgeClient.initialize();
    }
  }
 
  /**
   * @see #HydraTask_getEntriesFromServer
   */
  protected void populateRegion(Region aRegion) {
    for (int i=1; i <= numKeysInTest; i++) {
       String name = NameFactory.getNextPositiveObjectName();
       Object anObj = getObjectToAdd(name);
       try {
           aRegion.put(name, anObj);
       } catch (Exception e) {
           throw new TestException(TestHelper.getStackTrace(e));
       }
    }
    // sleep for 60 secs to allow distribution
    MasterController.sleepForMs(60000);
  }
  
  protected void query(Region aRegion, Object key, NewPortfolio expectedValue) {

    SelectResults results = query(aRegion, key);
                                                                                              
    if (isSerialExecution) {
       if (results.size() == 0) {
          if (expectedValue != null) {
            throw new TestException("ExpectedValue = <" + expectedValue + ">, but query ResultSet was empty");
          } else {
            Log.getLogWriter().info("Successful validation of empty result set (null)");
          }
                                                                                              
       } else {
          NewPortfolio result = (NewPortfolio)(results.asList().get(0));
          if (!result.equals(expectedValue)) {
             throw new TestException("Expected value of "  + key + " to be <" + expectedValue + "> but query returned <" + result + ">");
          } else {
             Log.getLogWriter().info("Successful validation of query results");
          }
       }
    }
 }
  protected void addObject(Region aRegion, String name, boolean logAddition) {
    Object anObj = getObjectToAdd(name);
    String callback = createCallbackPrefix + ProcessMgr.getProcessId();
    if (logAddition)
        Log.getLogWriter().info("addObject: calling put for name " + name + ", object " + TestHelper.toString(anObj) + " callback is " + callback + ", region is " + aRegion.getFullPath());
    try {
        aRegion.put(name, anObj, callback);
    } catch (Exception e) {
        throw new TestException(TestHelper.getStackTrace(e));
    }
    BridgeNotifyBB.incrementCounter("BridgeNotifyBB.NUM_CREATE", BridgeNotifyBB.NUM_CREATE);

    // serialBridgeNotifyMixedInterests needs help knowing how many events to expect
    int numExpectedEvents = 1;               // for ALL_KEYS client
    if (originalKeyList.contains(name)) {    // 1 for odd/even keylist client
       numExpectedEvents++;
    }
    List singleKeyList = (List)BridgeNotifyBB.getBB().getSharedMap().get( BridgeNotifyBB.SINGLE_KEYS_REGISTERED );
    if (singleKeyList.contains(name)) {      // 1 if we selected the singleKey clients key
       numExpectedEvents++;
    }
    BridgeNotifyBB.getBB().getSharedMap().put( BridgeNotifyBB.numListeners, new Integer(numExpectedEvents) );
    //query (aRegion, name, (NewPortfolio)anObj);
  }
  
  /**
   * Creates a new object with the given <code>name</code> to add to a
   * region.
   */
  protected Object getObjectToAdd(String name) {
    
    long i = NameFactory.getPositiveNameCounter();
    int index = (int)(i % numOfId);       //to have same id in different portfolio

    NewPortfolio val = new NewPortfolio(name, index);
        
    Log.getLogWriter().info("set portfolio done");
    return val;
  }
  
  protected void invalidateObject(Region aRegion, boolean isLocalInvalidate) {
    Set aSet = aRegion.keys();
    if (aSet.size() == 0) {
        Log.getLogWriter().info("invalidateObject: No names in region");
        return;
    }

    Object name = null;
    for (Iterator it = aSet.iterator(); it.hasNext();) {
      Object potentialKey = it.next();
      if (aRegion.containsValueForKey(potentialKey)) {
         name = potentialKey;
         break;
      }
    }
    if (name == null) {
       Log.getLogWriter().info("invalidateObject: No entries with value in region");
        return;
    }

    boolean containsValue = aRegion.containsValueForKey(name);
    boolean alreadyInvalidated = !containsValue;
    Log.getLogWriter().info("containsValue for " + name + ": " + containsValue);
    Log.getLogWriter().info("alreadyInvalidated for " + name + ": " + alreadyInvalidated);
    try {
        String callback = invalidateCallbackPrefix + ProcessMgr.getProcessId();
        if (isLocalInvalidate) {
            Log.getLogWriter().info("invalidateObject: local invalidate for " + name + " callback is " + callback);
            aRegion.localInvalidate(name, callback);
            Log.getLogWriter().info("invalidateObject: done with local invalidate for " + name);
            if (!alreadyInvalidated) {
                BridgeNotifyBB.incrementCounter("BridgeNotifyBB.NUM_LOCAL_INVALIDATE", BridgeNotifyBB.NUM_LOCAL_INVALIDATE);
            }
        } else {
            Log.getLogWriter().info("invalidateObject: invalidating name " + name + " callback is " + callback);
            aRegion.invalidate(name, callback);
            Log.getLogWriter().info("invalidateObject: done invalidating name " + name);
            if (!alreadyInvalidated) {
                BridgeNotifyBB.incrementCounter("BridgeNotifyBB.NUM_INVALIDATE", BridgeNotifyBB.NUM_INVALIDATE);
            }
        }
        if (isCarefulValidation)
            verifyObjectInvalidated(aRegion, name);
    } catch (com.gemstone.gemfire.cache.EntryNotFoundException e) {
        if (isCarefulValidation)
            throw new TestException(TestHelper.getStackTrace(e));
        else {
            Log.getLogWriter().info("Caught " + e + " (expected with concurrent execution); continuing with test");
            return;
        }
//    } catch (CacheException e) {
//         throw new TestException(TestHelper.getStackTrace(e));
    }

    // serialBridgeNotifyMixedInterests needs help knowing how many events to expect
    int numExpectedEvents = 1;  // for ALL_KEYS client
    if (originalKeyList.contains(name)) {    // 1 for odd/even keylist client
       numExpectedEvents++;
    }
    List singleKeyList = (List)BridgeNotifyBB.getBB().getSharedMap().get( BridgeNotifyBB.SINGLE_KEYS_REGISTERED );
    if (singleKeyList.contains(name)) {
       numExpectedEvents++;
    }

    BridgeNotifyBB.getBB().getSharedMap().put( BridgeNotifyBB.numListeners, new Integer(numExpectedEvents) );
    //query(aRegion, name, (NewPortfolio)null);
  }
  
  protected void destroyObject(Region aRegion, Object name, boolean isLocalDestroy) {
    try {
        String callback = destroyCallbackPrefix + ProcessMgr.getProcessId();
        if (isLocalDestroy) {
            Log.getLogWriter().info("destroyObject: local destroy for " + name + " callback is " + callback);
            aRegion.localDestroy(name, callback);
            Log.getLogWriter().info("destroyObject: done with local destroy for " + name);
            BridgeNotifyBB.incrementCounter("BridgeNotifyBB.NUM_LOCAL_DESTROY", BridgeNotifyBB.NUM_LOCAL_DESTROY);
        } else {
            Log.getLogWriter().info("destroyObject: destroying name " + name + " callback is " + callback);
            aRegion.destroy(name, callback);
            Log.getLogWriter().info("destroyObject: done destroying name " + name);
            BridgeNotifyBB.incrementCounter("BridgeNotifyBB.NUM_DESTROY", BridgeNotifyBB.NUM_DESTROY);
        }
    } catch (com.gemstone.gemfire.cache.EntryNotFoundException e) {
        if (isCarefulValidation)
            throw new TestException(TestHelper.getStackTrace(e));
        else {
            Log.getLogWriter().info("Caught " + e + " (expected with concurrent execution); continuing with test");
            return;
        }
    } catch (BridgeWriterException bwe) {
        if (isCarefulValidation)
           throw new TestException(TestHelper.getStackTrace(bwe));
        else {  // concurrent tests
          Throwable cause = bwe.getCause();
          if (cause == null)
             throw new TestException(TestHelper.getStackTrace());

          if (cause.toString().startsWith("com.gemstone.gemfire.cache.EntryNotFoundException")) {
             Log.getLogWriter().info("Caught " + bwe + " (expected with concurrent execution); continuing with test");
          } else {
             throw new TestException(TestHelper.getStackTrace());
          }
        }
    } catch (Exception e) {
         throw new TestException(TestHelper.getStackTrace(e));
    }

    // serialBridgeNotifyMixedInterests needs help knowing how many events to expect
    int numExpectedEvents = 1;  // for ALL_KEYS client
    if (originalKeyList.contains(name)) {    // 1 for odd/even keylist client
       numExpectedEvents++;
    }
    List singleKeyList = (List)BridgeNotifyBB.getBB().getSharedMap().get( BridgeNotifyBB.SINGLE_KEYS_REGISTERED );
    if (singleKeyList.contains(name)) {
       numExpectedEvents++;
    }
    BridgeNotifyBB.getBB().getSharedMap().put( BridgeNotifyBB.numListeners, new Integer(numExpectedEvents) );
    //query(aRegion, name, (NewPortfolio)null);
  }

  /* ======================================================================== */
  /**
   * Updates the entry with the given key (<code>name</code>) in the
   * given region.
   */
  protected void updateObject(Region aRegion, Object name) {
      Object anObj = null;
      try {
          anObj = aRegion.get(name);
      } catch (CacheLoaderException e) {
          throw new TestException(TestHelper.getStackTrace(e));
      } catch (TimeoutException e) {
          throw new TestException(TestHelper.getStackTrace(e));
      }
      Object newObj = getUpdateObject((String)name);
      try {
          String callback = updateCallbackPrefix + ProcessMgr.getProcessId();
          Log.getLogWriter().info("updateObject: replacing name " + name + " with " +
                  TestHelper.toString(newObj) + "; old value is " + TestHelper.toString(anObj) +
                  ", callback is " + callback);
          aRegion.put(name, newObj, callback);
          Log.getLogWriter().info("Done with call to put (update)");
      } catch (Exception e) {
          throw new TestException(TestHelper.getStackTrace(e));
      }

      BridgeNotifyBB.incrementCounter("BridgeNotifyBB.NUM_UPDATE", BridgeNotifyBB.NUM_UPDATE);

      // serialBridgeNotifyMixedInterests needs help knowing how many events to expect
      int numExpectedEvents = 1;  // for ALL_KEYS client
      if (originalKeyList.contains(name)) {    // 1 for odd/even keylist client
         numExpectedEvents++;
      }
      List singleKeyList = (List)BridgeNotifyBB.getBB().getSharedMap().get( BridgeNotifyBB.SINGLE_KEYS_REGISTERED );
      if (singleKeyList.contains(name)) {
         numExpectedEvents++;
      }
      BridgeNotifyBB.getBB().getSharedMap().put( BridgeNotifyBB.numListeners, new Integer(numExpectedEvents) );
      //query(aRegion, name, (NewPortfolio)newObj);
  }
  
  protected Object getUpdateObject(String name) {
    return getObjectToAdd(name);
  }
  
  protected void addObject(Region aRegion, boolean logAddition) {
    super.addObject(aRegion,logAddition);
    doQuery(logAddition);
  }
  
  protected void updateObject(Region aRegion) {
    super.updateObject(aRegion);
    doQuery(true);
  }
  
  protected void readObject(Region aRegion) {
      Set aSet = aRegion.keys();
      if (aSet.size() == 0) {
          Log.getLogWriter().info("readObject: No names in region");
          return;
      }
      long maxNames = NameFactory.getPositiveNameCounter();
      if (maxNames <= 0) {
          Log.getLogWriter().info("readObject: max positive name counter is " + maxNames);
          return;
      }
      Object name = NameFactory.getObjectNameForCounter(TestConfig.tab().getRandGen().nextInt(1, (int)maxNames));
      Log.getLogWriter().info("readObject: getting name " + name);
      try {
          Object anObj = aRegion.get(name);
          Log.getLogWriter().info("readObject: got value for name " + name + ": " + TestHelper.toString(anObj));
      } catch (CacheLoaderException e) {
          throw new TestException(TestHelper.getStackTrace(e));
      } catch (TimeoutException e) {
          throw new TestException(TestHelper.getStackTrace(e));
      }
      //query(aRegion, name);
      doQuery(true);
  }

  
  protected void destroyObject(Region aRegion, boolean isLocalDestroy) {
    super.destroyObject(aRegion, isLocalDestroy);
    doQuery(true);
  }
  

  /* ======================================================================== */
  protected void doQuery(boolean logAddition) {
    String region1 = "/" + REGION_NAME;

    String queries[] = { 
       "select distinct * from " + region1 + " where name = $1 ",   
       "select distinct * from " + region1 + " where status = 'active'",
       "select distinct * from " + region1 + " where id != 0 AND status = 'active'",       
       "select distinct * from " + region1 + " where id <= 1 AND status <> 'active'",
       "select distinct * from " + region1 + " where NOT (id <= 5 AND status <> 'active')",
       "select distinct * from " + region1 + " where id < 5 OR status != 'active'", 
       "import parReg.\"query\".Position; select distinct * from " + region1 + " r, " +
           " r.positions.values pVal TYPE Position where r.status = 'active' AND pVal.mktValue >= 25.00",
       "import parReg.\"query\".Position; select distinct r.name, pVal, r.status from " + region1 + " r, " +
           "r.positions.values pVal TYPE Position where r.status = 'active' AND pVal.mktValue >= 25.00 AND pVal.qty >= 500",
       
       "import parReg.\"query\".Position; select distinct positions from " + region1, 

       "import parReg.\"query\".Position; select distinct \"type\" from " + region1 +
       ", positions.values pVal TYPE Position where pVal.qty > 1000.00"
//        The following two queries are not supported in parReg -- no join.        
//        "select distinct * from " + randRgn() + ", " + randRgn(),        
//        "select distinct * from " + randRgn() + ", " + randRgn() + ", " + randRgn(),      
//        "select distinct * from " + region1 + " p where p.id IN " + 
//              "(select distinct np.id from " + region1 + " np where np.status = 'active')", 
    };
    
    int i = ((new Random()).nextInt(queries.length));
    if (i == 0) {
      doQuery1(logAddition, region1);
      return;
    }
    Log.getLogWriter().info("query string number is " + i);
    Query query = CacheHelper.getCache().getQueryService().newQuery(queries[i]);
    if(logAddition) {
       Log.getLogWriter().info(" query = " + query);
    }   
    try {
       Object result = query.execute();
//       Log.getLogWriter().info(Utils.printResult(result));
       if ((result instanceof Collection) && logAddition) {
         Log.getLogWriter().info("Size of result is :" + ((Collection)result).size());
       }
    } catch (Exception e) {
       throw new TestException("Caught exception during query execution" + TestHelper.getStackTrace(e));
    }

  }

  /*
   * This method tests query with param based on key (name) on partition region.
   * Will throw exception if result.size() is not 0 or 1.
   */
  protected void doQuery1(boolean logAddition, String region1) {

    String queryString = "select distinct * from " + region1 + " where name = $1 ";

    Query query = CacheHelper.getCache().getQueryService().newQuery(queryString);
    if(logAddition) {
       Log.getLogWriter().info(" query = " + query.toString());
    }   
    
    int regionNum = -1;            
    for (int i=0; i<numOfRegions; i++) {
      if (region1.equals("/" + REGION_NAME + i)) {
        regionNum = i;
        Log.getLogWriter().info(" region num = " + regionNum);
      }
    }
    Region aRegion;
    //get the region thru region number
    if (regionNum > -1) {
      aRegion = CacheHelper.getCache().getRegion(REGION_NAME + ("" + regionNum));
    }
    else {
      aRegion = CacheHelper.getCache().getRegion(REGION_NAME);
    }
    Set keys = aRegion.keySet();  
    Iterator itr = keys.iterator();
    
    Object [] params = new Object[1];
    if (itr.hasNext()) {
      params[0] = itr.next();

      try {
         Object result = query.execute(params);
    //     Log.getLogWriter().info(Utils.printResult(result));
         if ((result instanceof Collection) && logAddition) {
           Log.getLogWriter().info("Size of result for first query ($1) is :" + ((Collection)result).size());
           if (((Collection)result).size() > 1) 
             throw new TestException ("Query result when checking key (name) can not be more than 1");
             
         }
      } catch (Exception e) {
         throw new TestException("Caught exception during query execution" + TestHelper.getStackTrace(e));
      }
    }

  }

}
