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
package cq.statistics;

import java.util.HashMap;
import java.util.Map;

import util.TestException;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.CqAttributes;
import com.gemstone.gemfire.cache.query.CqAttributesFactory;
import com.gemstone.gemfire.cache.query.CqClosedException;
import com.gemstone.gemfire.cache.query.CqException;
import com.gemstone.gemfire.cache.query.CqListener;
import com.gemstone.gemfire.cache.query.CqQuery;
import com.gemstone.gemfire.cache.query.CqServiceStatistics;
import com.gemstone.gemfire.cache.query.CqStatistics;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.cq.dunit.CqQueryTestListener;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.internal.CqQueryImpl;
import com.gemstone.gemfire.cache.query.internal.CqQueryVsdStats;
import com.gemstone.gemfire.cache.query.internal.CqService;
import com.gemstone.gemfire.cache.query.internal.CqServiceVsdStats;
import com.gemstone.gemfire.cache.query.internal.DefaultQueryService;

import hydra.BridgeHelper;
import hydra.CacheHelper;
import hydra.Log;
import hydra.RemoteTestModule;
import cq.CQTest;

public class CQStatisticsTest extends CQTest {
  
  static final int pauseSecs = 20;
  
  static public final String[] cqs = new String[] {
      // 0 - Test for ">"
      "p.ID > 0",

      // 1 - Test for "=".
      "p.ID = 2",

      // 2 - Test for "<" and "and".
      "p.ID < 5 and p.status='active'",

      // 3
      "p.description = NULL",

      // 4
      "p.ID > 0 and p.status='active'",
  };

  static final String CQUERY_NAME_PREFIX = "CQuery_";
  static final String CQUERY_PREFIX = "SELECT ALL * FROM /testRegion p where ";
  static public final String KEY = "key-";
  
  /** Initialize the queries to be used in the test by putting them in the queryMap
   * 
   *  @returns A Map, where keys are query names (Strings), and values are queries (Strings).
   *           Every query has a different unique name.
   */
  public static Map generateQueries(Region region) {
     Map queryMap = new HashMap();
     for (int i = 0; i < cqs.length; i++) {
       queryMap.put(CQUERY_NAME_PREFIX + i, CQUERY_PREFIX + cqs[i]);
     }
     return queryMap;
  }
  
  /** Creates and initializes a bridge server.
   */
  public synchronized static void HydraTask_initializeBridgeServer() {
     if (testInstance == null) {
        testInstance = new CQStatisticsTest();
        testInstance.initializeInstance();
        testInstance.aRegion = testInstance.initializeRegion("serverRegion");
        BridgeHelper.startBridgeServer("bridge");
        testInstance.isBridgeClient = false;
     }
     testInstance.uniqueKeyIndex.set(new Integer(RemoteTestModule.getCurrentThread().getThreadId())); 
  }
  
  /** Creates and initializes the singleton instance of CQTest in a client.
   */
  public synchronized static void HydraTask_initializeClient() {
     if (testInstance == null) {
        testInstance = new CQStatisticsTest();
        testInstance.initializeInstance();
        testInstance.aRegion = testInstance.initializeRegion("clientRegion");
        if (testInstance.isBridgeConfiguration) {
           testInstance.isBridgeClient = true;
           testInstance.registerInterest(testInstance.aRegion);
           if (CQsOn) {
              testInstance.initializeQueryService();
              testInstance.queryMap = generateQueries(testInstance.aRegion);
           } else {
              Log.getLogWriter().info("Not creating CQs because CQUtilPrms.CQsOn is " + CQsOn);
           }
        }
     }
     testInstance.uniqueKeyIndex.set(new Integer(RemoteTestModule.getCurrentThread().getThreadId())); 
  }

  private void validateCQStats(final String cqName,
      final int creates,
      final int updates,
      final int deletes,
      final int totalEvents,
      final int cqListenerInvocations) {

//  Get CQ Service.
    QueryService qService = null;
    try {          
      qService = CacheHelper.getCache().getQueryService();
    } catch (Exception cqe) {
      cqe.printStackTrace();
      throw new TestException("Failed to get query service.");
    }
    
    CqService cqService = null;
    try {
      cqService = ((DefaultQueryService)qService).getCqService();
    }
    catch (CqException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      throw new TestException("Failed to get CqService, CQ : " + cqName);
    }
    CqQuery cQuery = cqService.getCq(cqName);
    if (cQuery == null) {
      throw new TestException("Failed to get CqQuery for CQ : " + cqName);
    }
    
    CqStatistics cqStats = cQuery.getStatistics();
    CqQueryVsdStats cqVsdStats = ((CqQueryImpl)cQuery).getVsdStats();
    if (cqStats == null || cqVsdStats == null) {
      throw new TestException("Failed to get CqQuery Stats for CQ : " + cqName);
    }
    
    Log.getLogWriter().info("#### CQ stats for " + cQuery.getName() + ": " + 
        " Events Total: " + cqStats.numEvents() +
        " Events Created: " + cqStats.numInserts() +
        " Events Updated: " + cqStats.numUpdates() +
        " Events Deleted: " + cqStats.numDeletes() +
        " CQ Listener invocations: " + cqVsdStats.getNumCqListenerInvocations() +
        " Initial results time (nano sec): " + cqVsdStats.getCqInitialResultsTime());
    
    
//  Check for totalEvents count.
    if (totalEvents != cqStats.numEvents()) {
//    Result size validation.
      throw new TestException("Total Event Count mismatch Expected: "
          + totalEvents + " From statistics " + cqStats.numEvents());
    }
    
//  Check for create count.
    if (creates != cqStats.numInserts()) {
      throw new TestException("Create Event mismatch Expected: "
          + creates + " From statistics " + cqStats.numInserts());
    }
    
//  Check for update count.
    if (updates != cqStats.numUpdates()) {
      throw new TestException("Update Event mismatch Expected: "
          + updates + " From statistics " + cqStats.numUpdates());
    }
    
//  Check for delete count.
    if (deletes != cqStats.numDeletes()) {
      throw new TestException("Delete Event mismatch Expected: "
          + deletes + " From statistics " + cqStats.numDeletes());
    }
    
//  Check for CQ listener invocations.
    if (cqListenerInvocations != cqVsdStats.getNumCqListenerInvocations()) {
      throw new TestException("CQ Listener invocations mismatch Expected: "
          + cqListenerInvocations + " From statistics " + cqVsdStats.getNumCqListenerInvocations());
    }
  }
  
  private void validateCQServiceStats(final int created,
      final int activated,
      final int stopped,
      final int closed,
      final int cqsOnClient,
      final int cqsOnRegion,
      final int clientsWithCqs) {

    //  Get CQ Service.
    QueryService qService = null;
    qService = CacheHelper.getCache().getQueryService();
    CqServiceStatistics cqServiceStats = null;        
    cqServiceStats = qService.getCqStatistics();
    CqServiceVsdStats cqServiceVsdStats = null;
    try {
      cqServiceVsdStats = ((DefaultQueryService)qService).getCqService().stats;
    }
    catch (CqException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    if (cqServiceStats == null) {
      throw new TestException("Failed to get CQ Service Stats");
    }
    
    Log.getLogWriter().info("#### CQ Service stats: " + 
        " CQs created: " + cqServiceStats.numCqsCreated() +
        " CQs active: " + cqServiceStats.numCqsActive() +
        " CQs stopped: " + cqServiceStats.numCqsStopped() +
        " CQs closed: " + cqServiceStats.numCqsClosed() +
        " CQs on Client: " + cqServiceStats.numCqsOnClient() +
        " CQs on region "+testInstance.aRegion.getName() + " : " + cqServiceVsdStats.numCqsOnRegion(testInstance.aRegion.getName()) +
        " Clients with CQs: " + cqServiceVsdStats.getNumClientsWithCqs());
    // Check for created count.
    if (created != cqServiceStats.numCqsCreated()) {
      throw new TestException("Number of CQs created mismatch Expected: "
          + created + " From statistics " + cqServiceStats.numCqsCreated());
    }
    
    // Check for activated count.
    if (activated != cqServiceStats.numCqsActive()) {
      throw new TestException("Number of CQs activated mismatch Expected: "
          + activated + " From statistics " + cqServiceStats.numCqsActive());
    }

    // Check for stopped count.
    if (stopped != cqServiceStats.numCqsStopped()) {
      throw new TestException("Number of CQs stopped mismatch Expected: "
          + stopped + " From statistics " + cqServiceStats.numCqsStopped());
    }

    // Check for closed count.
    if (closed != cqServiceStats.numCqsClosed()) {
      throw new TestException("Number of CQs closed mismatch Expected: "
          + closed + " From statistics " + cqServiceStats.numCqsClosed());
    }

    // Check for CQs on client count.
    if (cqsOnClient != cqServiceStats.numCqsOnClient()) {
      throw new TestException("Number of CQs on client mismatch Expected: "
          + cqsOnClient + " From statistics " + cqServiceStats.numCqsOnClient());
    }
    
    // Check for CQs on region.
    if (cqsOnRegion != cqServiceVsdStats.numCqsOnRegion(testInstance.aRegion.getFullPath())) {
      throw new TestException("Number of CQs on region "+testInstance.aRegion.getFullPath()+" mismatch  Expected: "
          + cqsOnRegion + " From statistics " + cqServiceVsdStats.numCqsOnRegion(testInstance.aRegion.getFullPath()));
    }
    
    // Check for clients with CQs count.
    if (clientsWithCqs != cqServiceVsdStats.getNumClientsWithCqs()) {
      throw new TestException("Clints with CQs mismatch Expected: "
          + clientsWithCqs + " From statistics " + cqServiceVsdStats.getNumClientsWithCqs());
    }
  }

  private void verifyStatistics_SingleQuery() {
    /*created, activated, stopped, closed, cqsOnClient, cqsOnRegion, clientsWithCqs*/
    validateCQServiceStats(0, 0, 0, 0, 0, 0, 0);
    pause();
    createCQ(CQUERY_NAME_PREFIX + 0, CQUERY_PREFIX + cqs[0]);
    /*created, activated, stopped, closed, cqsOnClient, cqsOnRegion, clientsWithCqs*/
    validateCQServiceStats(1, 0, 1, 0, 1, 1, 0);
    pause();
    createValues(10000);
    pause();
    executeCQ(CQUERY_NAME_PREFIX + 0);
    pause();
    /*created, activated, stopped, closed, cqsOnClient, cqsOnRegion, clientsWithCqs*/
    validateCQServiceStats(1, 1, 0, 0, 1, 1, 0);
    pause();
    //We don't have serverside CQ name
    validateCQStats(CQUERY_NAME_PREFIX + 0, 0, 0, 0, 0, 0);
    pause();
    createValues(20000);
    pause();
    validateCQStats(CQUERY_NAME_PREFIX + 0, 10000, 10000, 0, 20000, 20000);
    pause();
    /* Delete values at server. */
    deleteValues(10000);
    pause();
    // Test CQ stats
    validateCQStats(CQUERY_NAME_PREFIX + 0, 10000, 10000, 10000, 30000, 30000);
    pause();
    stopCQ(CQUERY_NAME_PREFIX + 0);
    pause();
    /*created, activated, stopped, closed, cqsOnClient, cqsOnRegion, clientsWithCqs*/
    validateCQServiceStats(1, 0, 1, 0, 1, 1, 0);
    pause();
    closeCQ(CQUERY_NAME_PREFIX + 0);
    pause();
    /*created, activated, stopped, closed, cqsOnClient, cqsOnRegion, clientsWithCqs*/
    validateCQServiceStats(1, 0, 0, 1, 0, 0, 0);
    
  }
  
  private void verifyStatistics() {
    
    /*created, activated, stopped, closed, cqsOnClient, cqsOnRegion, clientsWithCqs*/
    validateCQServiceStats(0, 0, 0, 0, 0, 0, 0);
    pause();
    for (int i = 0; i < cqs.length; i++) {
      createCQ(CQUERY_NAME_PREFIX + i, CQUERY_PREFIX + cqs[i]);
    }
    
    /*created, activated, stopped, closed, cqsOnClient, cqsOnRegion, clientsWithCqs*/
    validateCQServiceStats(cqs.length, 0, cqs.length, 0, cqs.length, cqs.length, 0);
    pause();
    createValues(10000);
    pause();
    for (int i = 0; i < cqs.length; i++) {
      executeCQ(CQUERY_NAME_PREFIX + i);
    }
    
    pause();
    /*created, activated, stopped, closed, cqsOnClient, cqsOnRegion, clientsWithCqs*/
    validateCQServiceStats(cqs.length, cqs.length, 0, 0, cqs.length, cqs.length, 0);
    pause();
    //We don't have serverside CQ name
    for (int i = 0; i < cqs.length; i++) {
      /*creates, updates, deletes, totalEvents, listenerInvocations*/
      validateCQStats(CQUERY_NAME_PREFIX + i, 0, 0, 0, 0, 0);
    }
    
    pause();
    createValues(20000);
    pause();
    for (int i = 0; i < cqs.length; i++) {
      switch (i) {
      /* creates, updates, deletes, totalEvents, listenerInvocations */
      // p.id > 0
      case 0:
        validateCQStats(CQUERY_NAME_PREFIX + i, 10000, 10000, 0, 20000, 20000);
        break;
      // p.id = 2
      case 1:
        validateCQStats(CQUERY_NAME_PREFIX + i, 0, 1, 0, 1, 1);
        break;
      // p.id < 5 and status = active
      case 2:
        validateCQStats(CQUERY_NAME_PREFIX + i, 0, 2, 0, 2, 2);
        break;
      // p.description = NULL
      case 3:
        validateCQStats(CQUERY_NAME_PREFIX + i, 5000, 5000, 0, 10000, 10000);
        break;
      // p.ID > 0 and p.status='active'
      case 4:
        validateCQStats(CQUERY_NAME_PREFIX + i, 5000, 5000, 0, 10000, 10000);
        break;
      }
    }

    pause();
    /* Delete values at server. */
    deleteValues(10000);
    pause();
    // Test CQ stats
    validateCQStats(CQUERY_NAME_PREFIX + 0, 10000, 10000, 10000, 30000, 30000);
    for (int i = 0; i < cqs.length; i++) {
      switch (i) {
      /* creates, updates, deletes, totalEvents, listenerInvocations */
      // p.id > 0
      case 0:
        validateCQStats(CQUERY_NAME_PREFIX + i, 10000, 10000, 10000, 30000, 30000);
        break;
      // p.id = 2
      case 1:
        validateCQStats(CQUERY_NAME_PREFIX + i, 0, 1, 1, 2, 2);
        break;
      // p.id < 5 and status = active
      case 2:
        validateCQStats(CQUERY_NAME_PREFIX + i, 0, 2, 2, 4, 4);
        break;
      // p.description = NULL
      case 3:
        validateCQStats(CQUERY_NAME_PREFIX + i, 5000, 5000, 5000, 15000, 15000);
        break;
      // p.ID > 0 and p.status='active'
      case 4:
        validateCQStats(CQUERY_NAME_PREFIX + i, 5000, 5000, 5000, 15000, 15000);
        break;
      }
    }

    pause();
    for (int i = 0; i < cqs.length; i++) {
      stopCQ(CQUERY_NAME_PREFIX + i);  
    }
    
    pause();
    /*created, activated, stopped, closed, cqsOnClient, cqsOnRegion, clientsWithCqs*/
    validateCQServiceStats(cqs.length, 0, cqs.length, 0, cqs.length, cqs.length, 0);
    pause();
    for (int i = 0; i < cqs.length; i++) {
      closeCQ(CQUERY_NAME_PREFIX + i);
    }
    pause();
    /*created, activated, stopped, closed, cqsOnClient, cqsOnRegion, clientsWithCqs*/
    validateCQServiceStats(cqs.length, 0, 0, cqs.length, 0, 0, 0);
  }

  
  public static void HydraTask_verifyCqStatistics() throws Exception {
    ((CQStatisticsTest)testInstance).verifyStatistics();
  }
  
  private void pause() {
    try {Thread.sleep(pauseSecs *1000);} catch(Exception e) {}
  }
  
  public void createValues(int size) {
    Region region1 = testInstance.aRegion;
    for (int i = 1; i <= size; i++) {
      region1.put(KEY+i, new Portfolio(i));
    }
    Log.getLogWriter().info("### Number of Entries in Region :" + region1.keys().size());
  }
  
  public void deleteValues(int size) {
    Region region1 = testInstance.aRegion;
    for (int i = 1; i <= size; i++) {
      region1.destroy(KEY+i);
    }
    Log.getLogWriter().info("### Number of Entries in Region :" + region1.keys().size());
  }
  
  public void executeCQ(String cqName) {
    QueryService cqService = null;
    CqQuery cq1 = null;
    cqService = CacheHelper.getCache().getQueryService();
    try {
      cq1 = cqService.getCq(cqName);
      if (cq1 == null) {
        Log.getLogWriter().info("Failed to get CqQuery object for CQ name: " + cqName);
        throw new TestException("Could not get cq with name " + cqName);
      }
      else {
        Log.getLogWriter().info("Obtained CQ, CQ name: " + cq1.getName());
      }
      cq1.execute();
    } catch (Exception ex) {
      Log.getLogWriter().info("CqService is :" + cqService);
      Log.getLogWriter().error(ex);
      AssertionError err = new AssertionError("Failed to execute  CQ " + cqName);
      err.initCause(ex);
      throw err;
    }
  }
  
  public void createCQ(String cqName, String queryStr) {
    Log.getLogWriter().info("### Create CQ. ###" + cqName);
    // Get CQ Service.
    QueryService cqService = null;
    cqService = CacheHelper.getCache().getQueryService();    // Create CQ Attributes.
    CqAttributesFactory cqf = new CqAttributesFactory();
    CqListener[] cqListeners = {new CqQueryTestListener(Log.getLogWriter())};
    cqf.initCqListeners(cqListeners);
    CqAttributes cqa = cqf.create();
    // Create CQ.
    try {
      CqQuery cq1 = cqService.newCq(cqName, queryStr, cqa);
    } catch (Exception ex){
      AssertionError err = new AssertionError("Failed to create CQ " + cqName + " . ");
      err.initCause(ex);
      Log.getLogWriter().info("CqService is :" + cqService, err);
      throw err;
    }
  }
  
  public void stopCQ(String cqName) {
    QueryService cqService = null;
    cqService = CacheHelper.getCache().getQueryService();
    // Close CQ.
    CqQuery cq1 = null;
    cq1 = cqService.getCq(cqName);
    try {
      cq1.stop();
    } catch (CqClosedException e) {
      throw new TestException(e.getMessage());
    } catch (CqException e) {
      throw new TestException(e.getMessage());
    }
  }
  
  public void closeCQ(String cqName)  {
    QueryService cqService = null;
    cqService = CacheHelper.getCache().getQueryService();
    // Close CQ.
    CqQuery cq1 = null;
    cq1 = cqService.getCq(cqName);
    try {
      cq1.close();
    } catch (CqClosedException e) {
      throw new TestException(e.getMessage());
    } catch (CqException e) {
      throw new TestException(e.getMessage());
    }
  }
}
