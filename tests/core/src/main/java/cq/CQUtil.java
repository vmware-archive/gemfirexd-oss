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
package cq; 

import util.*;
import hydra.*;
import hydra.blackboard.*;
import com.gemstone.gemfire.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.util.*;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.query.*;
import com.gemstone.gemfire.cache.query.internal.QueryUtils;
import com.gemstone.gemfire.cache.query.internal.types.TypeUtils;
import com.gemstone.gemfire.security.NotAuthorizedException;

import java.util.*;

/**
 * A class to contain methods useful for all CQ tests.
 */
public class CQUtil {

// the only instance of CqUtil
static private QueryService cqService = null;
static private boolean useCQ;
static private boolean logCQOperations;

protected static final String cqPrefix = "cq";

protected static final int GET_CQ_ATTRIBUTES = 0;
protected static final int GET_CQ_NAME       = 1;
protected static final int GET_CQ_RESULTS    = 2;
protected static final int GET_QUERY         = 3;
protected static final int GET_QUERY_STRING  = 4;
protected static final int GET_STATISTICS    = 5;
protected static final int[] cqOperations = {
   GET_CQ_ATTRIBUTES,
   GET_CQ_NAME,
   GET_CQ_RESULTS,
   GET_QUERY,
   GET_QUERY_STRING,
   GET_STATISTICS };

protected static final String[] cqOperationMethods = {
   "getCQAttributes()",
   "getCQName()",
   "getCQResults()",  
   "getQuery()",
   "getQueryString()",
   "getStatistics()"};

// ======================================================================== 
// initialization
// ======================================================================== 

/**
 *  Initialize the one instance of CQUtil
 */
public static synchronized void initialize() {
   useCQ = TestConfig.tab().booleanAt(CQUtilPrms.useCQ, false);
   Log.getLogWriter().info("CQUtil: useCQ = " + useCQ);

   logCQOperations = TestConfig.tab().booleanAt(CQUtilPrms.logCQOperations, true);
   Log.getLogWriter().info("CQUtil: logCQOperations = " + logCQOperations);
}

/**
 *  Initialize the CQService 
 */
public static synchronized void initializeCQService() {
  initializeCQService(false);
}
public static synchronized void initializeCQService(boolean useClientCache) {
   if (useCQ == false) {
      return;
   }

   try {
     Log.getLogWriter().info("Creating CQService.");
     // Check if QueryService needs to be obtained from Pool.
     String usingPool = TestConfig.tab().stringAt(CQUtilPrms.QueryServiceUsingPool, "false");
     boolean queryServiceUsingPool = Boolean.valueOf(usingPool).booleanValue();
     if (useClientCache) {
       Pool pool = PoolHelper.createPool(CQUtilPrms.getQueryServicePoolName());
       cqService = ClientCacheHelper.getCache().getQueryService(pool.getName());
       Log.getLogWriter().info("Initializing QueryService with ClientCache using Pool. PoolName: " + pool.getName());
     } else if (queryServiceUsingPool){
       Pool pool = PoolHelper.createPool(CQUtilPrms.getQueryServicePoolName());
       cqService = pool.getQueryService();
       Log.getLogWriter().info("Initializing QueryService using Pool. PoolName: " + pool.getName());
     } else {
       cqService = CacheHelper.getCache().getQueryService();
       Log.getLogWriter().info("Initializing QueryService using Cache.");
     }
   } catch (Exception e) {
     throw new TestException("Failed to create CQService " + TestHelper.getStackTrace(e));
   }
}

public static SelectResults registerCQ(Region aRegion) {
   String query = "SELECT * FROM " + aRegion.getFullPath();
   return registerCQ(getCqName(), query, aRegion);
}

  public static SelectResults registerDurableCQ(Region aRegion) {
    String query = "SELECT * FROM " + aRegion.getFullPath();
    String cqName = aRegion.getFullPath()
        + RemoteTestModule.getCurrentThread().getThreadId();
    return registerDurableCQ(cqName, query, aRegion);
  }

/** 
 *  Register a CQ.  Use cqName and/or query if provided.  If null, cq will 
 *  be named cq<threadId>.  
 *  Default query = "SELECT * from <regionName>"
 *  Desired ContinuousListener should be provided via CQUtilPrms.CQListener
 */
public static SelectResults registerCQ(String cqName, String query, Region aRegion) {

   SelectResults results = null;
   if (useCQ == true) {

      if (cqName == null) {
         cqName = getCqName();
      }
   
      if (query == null) {
         query = "SELECT * FROM " + aRegion.getFullPath();
      }

      CqAttributes cqAttrs = getCQAttributes();

      CqQuery cq = null;
     try {
          Log.getLogWriter().info("Creating CQ named " + cqName + " with query: " + query + " and cqAttrs: " + cqAttributesToString(cqAttrs));
          cq = cqService.newCq(cqName, query, cqAttrs);
          Log.getLogWriter().info("Successfully created CQ named " + cqName);
          // Use versioning to resolve api changes 
          //CqResults rs = cq.executeWithInitialResults();
          //results = CQUtil.getSelectResults(rs);
          results = new CQExecuteVersionHelper().executeWithInitialResults(cq);
      } catch (CqClosedException eClosed) {
        throw new TestException(TestHelper.getStackTrace(eClosed));
      } catch (RegionNotFoundException regionNotFound) {
        throw new TestException(TestHelper.getStackTrace(regionNotFound));
      } catch (CqExistsException cqe) {
        throw new TestException(TestHelper.getStackTrace(cqe));
      } catch (CqException e) {
          throw new TestException(TestHelper.getStackTrace(e));
      } catch (IllegalStateException ue) {
          throw new TestException(TestHelper.getStackTrace(ue));
      }
   }
   return results;
}

  /**
   * Register a durable CQ. Use cqName and/or query if provided. If null, cq
   * will be named cq<threadId>. Default query = "SELECT * from <regionName>"
   * Desired ContinuousListener should be provided via CQUtilPrms.CQListener
   */
  public static SelectResults registerDurableCQ(String cqName, String query,
      Region aRegion) {

    SelectResults results = null;
    if (useCQ == true) {

      if (cqName == null) {
        cqName = getCqName();
      }

      if (query == null) {
        query = "SELECT * FROM " + aRegion.getFullPath();
      }

      CqAttributes cqAttrs = getCQAttributes();

      CqQuery cq = null;
      try {
        Log.getLogWriter().info(
            "Creating CQ named " + cqName + " with query: " + query
                + " and cqAttrs: " + cqAttributesToString(cqAttrs));
        cq = cqService.newCq(cqName, query, cqAttrs, true);
        Log.getLogWriter().info("Successfully created CQ named " + cqName);
        // Use versioning to handle api changes
        //CqResults rs = cq.executeWithInitialResults();
        //results = CQUtil.getSelectResults(rs);
        results = new CQExecuteVersionHelper().executeWithInitialResults(cq);
      }
      catch (CqClosedException eClosed) {
        throw new TestException(TestHelper.getStackTrace(eClosed));
      }
      catch (RegionNotFoundException regionNotFound) {
        throw new TestException(TestHelper.getStackTrace(regionNotFound));
      }
      catch (CqExistsException cqe) {
        throw new TestException(TestHelper.getStackTrace(cqe));
      }
      catch (CqException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      }
      catch (IllegalStateException ue) {
        throw new TestException(TestHelper.getStackTrace(ue));
      }
    }
    return results;
  }

  /**
   * Register a CQ and then closes it (Used for security.bt). Use cqName and/or
   * query if provided. If null, cq will be named cq<threadId>. Default query =
   * "SELECT * from <regionName>" Desired ContinuousListener should be provided
   * via CQUtilPrms.CQListener
   */
  public static SelectResults registerAndCloseCQ(String cqName, String query,
      Region aRegion) throws CqException {

    SelectResults results = null;
    if (useCQ == true) {

      if (cqName == null) {
        cqName = getCqName();
      }

      if (query == null) {
        query = "SELECT * FROM " + aRegion.getFullPath();
      }

      CqAttributes cqAttrs = getCQAttributes();

      CqQuery cq = null;
      try {
        Log.getLogWriter().info(
            "Creating CQ named " + cqName + " with query: " + query
                + " and cqAttrs: " + cqAttributesToString(cqAttrs));
        cq = cqService.newCq(cqName, query, cqAttrs);
        Log.getLogWriter().info("Successfully created CQ named " + cqName);
        // use versioning to handle api changes
        //CqResults rs = cq.executeWithInitialResults();
        //results = CQUtil.getSelectResults(rs);
        results = new CQExecuteVersionHelper().executeWithInitialResults(cq);
        cq.close();
      }
      catch (CqClosedException eClosed) {
        throw new TestException(TestHelper.getStackTrace(eClosed));
      }
      catch (RegionNotFoundException regionNotFound) {
        throw new TestException(TestHelper.getStackTrace(regionNotFound));
      }
      catch (CqExistsException cqe) {
        throw new TestException(TestHelper.getStackTrace(cqe));
      }
      catch (IllegalStateException ue) {
        throw new TestException(TestHelper.getStackTrace(ue));
      }
    }
    return results;
  }
  
/**
 * Return CQAttributes based on CQUtilPrms: cqListener(s)
 */
private static CqAttributes getCQAttributes() {
   CqAttributesFactory factory = new CqAttributesFactory();
   factory.addCqListener( CQUtilPrms.getCQListener() );
   CqAttributes cqAttrs = factory.create();
   return cqAttrs;
}

private static String cqAttributesToString(CqAttributes attrs) {
   StringBuffer aStr = new StringBuffer();
   aStr.append("cqListener = " + attrs.getCqListener());
   return aStr.toString();
}

public static CqQuery getCQ() {
   return getCQ(getCqName());
}

/** Return the named ContinuousQuery
 */
public static CqQuery getCQ(String cqName) {
   CqQuery cq = null;
   if (useCQ == true) {
      cq = cqService.getCq(cqName);
   }
   return cq;
}

/** Return the SelectResults for a default-named CQ
 */

public static SelectResults getCQResults() {
   SelectResults results = null;
   if (useCQ == true) {
      results = getCQResults(getCqName());
   }
   return results;
}

public static synchronized SelectResults getCQResults(String cqName) {
   SelectResults results = null;
   if (useCQ == true) {
      CqQuery cq = getCQ(cqName);
      Log.getLogWriter().info("getCq(" + cqName + ") returns " + cq);
      try {
        cq.stop();
        // use versioning to handle api changes
        //CqResults rs = cq.executeWithInitialResults();
        //results = CQUtil.getSelectResults(rs);
        results = new CQExecuteVersionHelper().executeWithInitialResults(cq);
      } catch (CqClosedException cce) {
 
      } catch (RegionNotFoundException rne) {
        
      } catch (CqException ce) {
        
      }
   }
   return results;
}

/**
 * This takes the CQ Results and returns the result in the
 * form of SelectResults without the key. 
 */
public static SelectResults getSelectResults(CqResults cqResults){
  SelectResults sr = QueryUtils.getEmptySelectResults(TypeUtils.OBJECT_TYPE, null);
  Struct s = null;
  //Log.getLogWriter().info("### Getting SelectResults from CQResults. " + 
  //  " CQResults IS: " + cqResults);
  for (Object result : cqResults.asList()) {
    s = (Struct)result;
    sr.add(s.get("value"));
    //Log.getLogWriter().info("#### key :" + s.get("key") + " value is: " + s.get("value"));
  }
  //Log.getLogWriter().info("### Getting SelectResults from CQResults.SR IS: " +
  //  sr);
  return sr;
}

public static void displaySelectResults() {
   displaySelectResults(getCqName());
}

/** display SelectResults (using iterator)
 */
public static synchronized void displaySelectResults(String cqName) {
   if (useCQ == false) {
      return;
   }

   CqQuery cq = getCQ(cqName);
   SelectResults results = getCQResults(cqName);
   Iterator i = results.iterator();
   StringBuffer aStr = new StringBuffer("ResultSet (size " + results.size() + ") = \n");
   while (i.hasNext()) {
     Object o = i.next();
     aStr.append(o.toString() + "\n");
   }
   Log.getLogWriter().info(aStr.toString());
}

public static String getCqName() {
   return cqPrefix + RemoteTestModule.getCurrentThread().getThreadId();
}

/** TASK that can be invoked by a bridgeClient thread to execute
 *  random CQ operations on a randomly selected CQ (for 30 seconds with
 *  each invocation).
 */
public static void HydraTask_doCQOperations() {
   if (useCQ == false) {
      return;
   }

   long startTime = System.currentTimeMillis();

   do {
      // Select a CQ to operate on
      CqQuery cqs[] = cqService.getCqs();
      Log.getLogWriter().info("CQs = " + cqs);
      int randInt = TestConfig.tab().getRandGen().nextInt(0, cqs.length-1);
      CqQuery cq = cqs[randInt];
      String cqName = cq.getName();
      
      // Operation to execute on this CQ
      randInt = TestConfig.tab().getRandGen().nextInt(0, cqOperations.length-1);
      int whichOp = cqOperations[randInt];
      if (logCQOperations) {
         Log.getLogWriter().info("Performing " + cqOperationMethods[whichOp] + " on CQ named " + cqName);
      }
      switch (whichOp) {
         case (GET_CQ_ATTRIBUTES):
            CqAttributes attrs = cq.getCqAttributes();
            if (logCQOperations) {
               Log.getLogWriter().info("CQ Attrs = " + cqAttributesToString(attrs));
            }
            break;
         case (GET_CQ_NAME):
            String name = cq.getName();
            if (logCQOperations) {
               Log.getLogWriter().info("cqName = " + name);
            }
            break;
         case (GET_CQ_RESULTS):
            if (logCQOperations) {
               displaySelectResults(cqName);
            }
            break;
         case (GET_QUERY):
            Query query = cq.getQuery();
            if (logCQOperations) {
               Log.getLogWriter().info("Query = " + query);
            }
            break;
         case (GET_QUERY_STRING):
            String queryString = cq.getQueryString();
            if (logCQOperations) {
               Log.getLogWriter().info("QueryString = " + queryString);
            }
            break;
         case (GET_STATISTICS):
            CqStatistics cqStats = cq.getStatistics();
            if (logCQOperations) {
               Log.getLogWriter().info("QueryStatistics = " + cqStats.toString());
            }
            break;
         default:
            throw new TestException("Unrecognized cqOperation (" + randInt + ")");
      }
   } while (System.currentTimeMillis() - startTime < 30000);
}

/** CLOSETASK to verify that the CQListener was invoked at least 
 *  once.
 */
public static void HydraTask_verifyCQListenerInvoked() {
   if (useCQ == false) {
      return;
   }
   
   // display all counters
   CQUtilBB.printBB();

   // At least one event must be processed
   SharedCounters sc = CQUtilBB.getBB().getSharedCounters();
   long[] counterValues = sc.getCounterValues();
   boolean nonZeroValue = false;
   for (int i=0; i < counterValues.length; i++) {
      if (counterValues[i] > 0) {
         nonZeroValue = true;
      }
   }
   if (nonZeroValue == false) {
      throw new TestException("CQListener not invoked: CQUtilBB.counterValues = " + counterValues.toString());
   }
   long numErrors = sc.read(CQUtilBB.NUM_ERRORS);
   if (numErrors != 0) {
      throw new TestException("CQListener onError() invoked " + numErrors + " errors, check logs for details");
   }
}

/** CLOSETASK to verify that the CQListener processed:
 *  - CQ Create Events for server side loads
 *
 *  Also documents that the server invoked local CacheListeners for LOCAL_LOAD_CREATE
 */
public static void HydraTask_verifyCQListenerProcessedServerInitiatedEvents() {
   if (useCQ == false) {
      return;
   }
   
   // Workaround BUG 38176 - we sometimes see the edgeClients get the loadsCompleted stats for the servers
   // too early, which causes us to go over the expected number of creates.  This gives the servers a few 
   // seconds to update the stat archives before the clients access the loadsCompleted stat.
   MasterController.sleepForMs(5000);

   // display all counters
   CQUtilBB.printBB();

   String errMsg = new String();

   // Server invokes local CacheListener for LOCAL_LOAD_CREATE 
   SharedCounters sc = CQUtilBB.getBB().getSharedCounters();
   long localLoadCreates = sc.read(CQUtilBB.NUM_LOCAL_LOAD_CREATE);
   Log.getLogWriter().info("Server afterCreate() LOCAL_LOAD_CREATE = " + localLoadCreates);

   // All registered CQs should get these events (multipler)
   long numCQsCreated = (long)util.TestHelper.getNumCQsCreated();
   Log.getLogWriter().info("Combined CqServiceStats.getNumCQsCreated = " + numCQsCreated);

   // Check CQ (Create) Events for server side loads
   long numCQCreateEvents = sc.read(CQUtilBB.NUM_CREATE);
   long loadsCompleted = (long)util.TestHelper.getNumLoadsCompleted();
   Log.getLogWriter().info("Server side load operations = " + loadsCompleted);

   // Server side LOCAL_LOAD_CREATE verification
   if ((loadsCompleted > 0) && (localLoadCreates == 0)) {
      errMsg = "Expected Server side afterCreate (LOCAL_LOAD_CREATE) invocations, but none processed\n";
     throw new TestException(errMsg + TestHelper.getStackTrace());  
   }

   // Client side CQ (Create) Event check
    Log.getLogWriter().info("Expecting " + loadsCompleted*numCQsCreated + " (" + loadsCompleted + " server loadsCompleted * " + numCQsCreated +  " CQs) CQ Create Events");
   boolean exactCounterValue = TestConfig.tab().booleanAt(CQUtilPrms.exactCounterValue, true);
   TestHelper.waitForCounter( CQUtilBB.getBB(), "NUM_CREATE", CQUtilBB.NUM_CREATE, (loadsCompleted*numCQsCreated), exactCounterValue, 120000 );

   // check for CQListener's processing onError()
   long numErrors = sc.read(CQUtilBB.NUM_ERRORS);
   if (numErrors != 0) {
      errMsg = "CQListener onError() invoked " + numErrors + " errors, check logs for details";
      Log.getLogWriter().info(errMsg); 
      throw new TestException(errMsg + TestHelper.getStackTrace());
   }
}
}
