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

import com.gemstone.gemfire.cache.query.CqQuery;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.CqResults;
import com.gemstone.gemfire.cache.query.IndexInvalidException;

import hydra.*;

import util.TestException;
import util.TestHelper;
import util.NameFactory;
import mapregion.MapPrms;
import java.util.*;

import parReg.query.Position;

public class ConcCQAndOpsTest extends ConcCQTest {
  protected static ConcCQAndOpsTest concCQAndOpsTest;
  protected int sleepMS = 10000;
  private boolean edgePutAndVerify;
  protected static CQUtilBB cqBB = CQUtilBB.getBB();
  protected boolean createIndex = false;
  
  public synchronized static void HydraTask_initialize() {
    if (concCQAndOpsTest == null) {
      concCQAndOpsTest = new ConcCQAndOpsTest();
      concCQAndOpsTest.initialize();
    }
  }
  
  protected void initialize() {
    try {
      if (cqBB.getSharedMap().get(CQUtilBB.VerifyFlag) == null) {
        initFlags();
      }
    }
    catch (NullPointerException e){
      initFlags();
    } 
    try {
      edgePutAndVerify = TestConfig.tab().booleanAt(CQUtilPrms.edgePutAndVerify);
    } catch (HydraConfigException e) {
      edgePutAndVerify = false;
    }

    ConcCQTest.initServerWithMultRegions();
    
    createIndex = TestConfig.tab().booleanAt(CQUtilPrms.createIndex, false);
    if (createIndex) {        
      createIndex(); //create indexes on bridges
    }
  }
  
  protected boolean createPrimaryIndex = TestConfig.tab().booleanAt(CQUtilPrms.createPrimaryIndex, false);
  protected void createIndex() {
    IndexTest indexTest = new IndexTest();
    String[] regionNames = MapPrms.getRegionNames();
    for (int i =0; i<regionNames.length; i++) {
      if (createPrimaryIndex) {
        try {
          indexTest.createIndex(regionNames[i]);
          throw new TestException ("Bug 38119 is detected. After setting value constraint and " +
              "creating primary index  -- IndexType.PRIMARY_KEY on a non-existing field " +
              "product did not throw IndexInvalidException." );
        } catch (IndexInvalidException e){
          Log.getLogWriter().info("Got expected IndexInvalidException, continuing tests");
          indexTest.createPrimaryIndex(regionNames[i]);
        }
      }
    }
  }
  
    
  protected void initFlags() {
    cqBB.getSharedMap().put(CQUtilBB.VerifyFlag, new Boolean(false));
    cqBB.getSharedMap().put(CQUtilBB.EntryDone, new Boolean(false));
    cqBB.getSharedMap().put(CQUtilBB.VerifyDone, new Boolean(false));
    cqBB.getSharedCounters().zero(CQUtilBB.EntryPerformed);
    cqBB.getSharedCounters().zero(CQUtilBB.Verified);
    cqBB.getSharedCounters().zero(CQUtilBB.NumCounterToDestroy);
    Log.getLogWriter().info("Init the BB varibles");
  }
  
  public static void HydraTask_verify() {
    concCQAndOpsTest.verify();
  }
  
  public static void HydraTask_doCQAndVerifyResults() {
    concCQAndOpsTest.performCQOpsAndVerifyResults();
  }
  
  // only verfiy when there is no more entry operation
  // one thread will set the verify Flag after verification is done.
  protected void verify() {
    boolean verify = ((Boolean) cqBB.getSharedMap().get(CQUtilBB.VerifyFlag)).booleanValue();
    boolean verifyDone = ((Boolean) cqBB.getSharedMap().get(CQUtilBB.VerifyDone)).booleanValue();
    if (!verify || verifyDone)    {
      if (edgePutAndVerify) return;  //if edges need to perform both operation and verification, do not sleep
      
      MasterController.sleepForMs(sleepMS);
      return;
    }
    
    long verified = cqBB.getSharedCounters().incrementAndRead(CQUtilBB.Verified);
    Log.getLogWriter().info("verified so far is " + verified);
    if (verified > ConcCQMultRegionsClient.verifyCount)  {
      return;
    }
    else if (verified == ConcCQMultRegionsClient.verifyCount) {
      cqBB.getSharedMap().put(CQUtilBB.VerifyDone, new Boolean(true));
      Log.getLogWriter().info("reset verifyDone to true");
      verifySelectResults();
      MasterController.sleepForMs(sleepMS);
      
     cqBB.getSharedMap().put(CQUtilBB.VerifyFlag, new Boolean(false));
     Log.getLogWriter().info("reset verifyFlag to false");
     cqBB.getSharedCounters().zero(CQUtilBB.Verified);
     cqBB.getSharedMap().put(CQUtilBB.VerifyDone, new Boolean(false));
    } // this thread will set the flag after verification is done.
    else {
      if (((Boolean) cqBB.getSharedMap().get(CQUtilBB.VerifyFlag)).booleanValue())
        verifySelectResults();
    } 
  }
        
  protected void verifySelectResults() {
    
   //stop cq
    CqQuery cq = null;
    SelectResults results = null;
    cq = getCQForOp();
    if (cq != null) {
      stopCQ(cq);
      Log.getLogWriter().info("Stopping CQ and to get initial results.");
      
      String qs = cq.getQueryString();
      String regionName = qs.substring(15,26); //only works for certain query strings
      
      if (cq.isClosed()) {
        reRegisterCQ(cq);
      }
      
      //execute cq with initial result
      try {
         CqResults rs = cq.executeWithInitialResults();
         results = CQUtil.getSelectResults(rs);
      } catch (Exception e) {
        throw new TestException(TestHelper.getStackTrace(e));
      }
      
      synchronized (cqList) {
        cqList.remove(cq);
      }
      
      //compare the results from snapshot with sr
      verifyFromSnapshots(results, regionName);
      //Log.getLogWriter().info("performing verification of SelectResults.");
    }     
    
  }
  
  protected CqQuery getNextRandonCQ () 
  {
    CqQuery cq = null;
    int randInt;
    CqQuery cqArray[] = cqService.getCqs();
    randInt = (int) (System.currentTimeMillis()%cqArray.length);
    cq = cqArray[randInt];
    return cq;
  }
  
  protected void verifyFromSnapshots(SelectResults results, String regionName) {
    // compare the initial results with data in the snapshot to see if there are missing or extra elements
    List dataFromSnapshots = getDataFromSnapshots(regionName);
    List resultsList = results.asList();
    Log.getLogWriter().info("regionName is " + regionName);
    Log.getLogWriter().info("size of selectResults is " + results.size());
    Log.getLogWriter().info("size of qualified data from snapshots is " + dataFromSnapshots.size());

    List dataFromSnapshotsCopy = new ArrayList(dataFromSnapshots);
    List resultsListCopy = new ArrayList(resultsList);
    
    resultsListCopy.removeAll(dataFromSnapshots);
    List unexpected = resultsListCopy;
    
    dataFromSnapshotsCopy.removeAll(resultsList);
    List missing = dataFromSnapshotsCopy;
    
    StringBuffer aStr = new StringBuffer();
    if (unexpected.size() > 0) {
      aStr.append("the following " + unexpected.size() + " unexpected elements in SelectResults: " + Position.toString(unexpected));
    }
    if (missing.size() > 0) {
      aStr.append("the following " + missing.size() + " elements were missing from SelectResults: " + Position.toString(missing));
    }
    
    if (aStr.length() != 0) {
      throw new TestException(aStr.toString());
    }
    
    if (dataFromSnapshots.size() == results.size()) {
      Log.getLogWriter().info("verified that results are correct");
    }
    else if (dataFromSnapshots.size() < results.size()) {
      throw new TestException("There are more data in selectResults");
    }
    else {
      throw new TestException("There are fewer data in selectResults");
    }

  }
  
  protected List getDataFromSnapshots(String regionName) {
    Set dataFromBB = ConcCQBB.getBB().getSharedMap().getMap().entrySet();
    List dataList = new ArrayList();
    Iterator itr = dataFromBB.iterator();  
    String key;
    Position aPosition;
    while (itr.hasNext()) {                //find all portfolios that meet requirements
      Map.Entry m = (Map.Entry) itr.next();
      key = (String) m.getKey();
      Object value = m.getValue();  
      if (value instanceof Position) {
         aPosition = (Position) m.getValue();  
         if (key.startsWith(regionName + "_Object")) {
           dataList.add(aPosition);
         }
      }
    }
    return dataList;
  }
    
  protected void performCQOpsAndVerifyResults() {
    boolean verify = ((Boolean) cqBB.getSharedMap().get(CQUtilBB.VerifyFlag)).booleanValue();
    if (verify) {
      verify();
    }
    else {
      super.performCQOperations();
    }
  } //edges may need to sometimes needs to perform verification and sometimes CQ operations
  
  public static void verifyCQListener() {
    concCQAndOpsTest.verifyEvents();
  }
  
  protected void verifyEvents() {
    Log.getLogWriter().info("verifyEvents");
    try {
      super.verifyEvents();
    } catch (TestException e) {
      String str = null;
      createIndex = TestConfig.tab().booleanAt(CQUtilPrms.createIndex, false);
      if (!createIndex) str = checkFromListener(); // check out which cq event (key) might be missing
      throw new TestException (e.getMessage() + str);
    }
  }
  
  // used for debug purpose to see which object might be missing
  // works when no duplicate keys are put
  public static String checkFromListener() {
    StringBuffer str = new StringBuffer();
    long counter = NameFactory.getPositiveNameCounter();
    int numEdges = MapPrms.getNumEdges();
    int [] array = new int [(int) counter + 1]; 
    for (int i= 1; i<= counter; i++) {
      array[i] = 0;
    } //Object starts from 1
    
    
    str.append("May have incorrect number of cq event invocations for the following objects:");
    Set aSet = cqBB.getSharedMap().getMap().entrySet(); //get keys from cq events
    Iterator itr = aSet.iterator();
    String key;
    String name;
    Set keySet = new HashSet();
    Set objectSet = new HashSet();
    String objectNumber;
    int arrayPosition;
    while (itr.hasNext()) {
      Map.Entry m = (Map.Entry) itr.next();
      key = (String) m.getKey();
      if (key.startsWith("Object")) {
        String[] strArray = key.split(":");
        name = strArray[0];
        keySet.add(name);
        
        objectNumber = name.substring(7);
        arrayPosition = Integer.parseInt(objectNumber);
        array[arrayPosition] ++;  //calculate how many cq listeners got invoked for the partical key
      }
    }
    
    for (int i=1; i<=counter; i++) {
      objectSet.add("Object_" + i );
    }
    objectSet.removeAll(keySet); // to find out if all edges not get the particular key
    itr = objectSet.iterator();
    while (itr.hasNext()) {
      str.append(" " + itr.next() + " ");
    }
    
    str.append("\n some edges may miss cq events on the following objects: " );
    for (int i=1; i<=counter; i++) {
      if (array[i] % numEdges != 0) {
        str.append(" Object_" + i + " " );
      } // if only some edges listener missed the cq events
    }
    
    return str.toString();
  }  
 
}
