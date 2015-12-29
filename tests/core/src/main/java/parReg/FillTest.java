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
package parReg;

import hydra.Log;
import hydra.MasterController;
import hydra.RemoteTestModule;
import hydra.StopSchedulingOrder;
import hydra.StopSchedulingTaskOnClientOrder;
import hydra.TestConfig;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import parReg.colocation.KeyResolver;
import parReg.colocation.Month;
import parReg.execute.PartitionObjectHolder;
import parReg.execute.RoutingHolder;
import util.CacheDefPrms;
import util.CacheDefinition;
import util.CacheUtil;
import util.DeclarativeGenerator;
import util.NameFactory;
import util.RandomValuesPrms;
import util.RegionDefPrms;
import util.RegionDefinition;
import util.TestException;
import util.TestHelper;
import admin.AdminTest;

import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.internal.cache.BucketDump;
import com.gemstone.gemfire.internal.cache.ForceReattemptException;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;

/** Test class to fill a partitioned region.
 */
public class FillTest extends ParRegTest {
    
/* instance fields */
// the local max memory for the partition in this VM
   protected long myLocalMaxMemoryMB = 0;       
// min number of entries needed before expected an alert for exceeding LOCAL_MAX_MEMORY
   protected long lowerThresholdNumEntries = 0; 
// max number of entries for expecting an alert of LOCAL_MAX_MEMORY
   protected long upperThresholdNumEntries = 0; 
// the name of the blackboard counter for counting alerts received for this VM's setting of LOCAL_MAX_MEMORY
   protected String myAlertCounterName;         
// the blackboard counter for counting alerts received for this VM's setting of LOCAL_MAX_MEMORY 
   protected int myAlertCounter;               
// the number of VMs in this test
   protected int numVMs;               

// for alert listener initialization
static boolean alertAdded = false;

// part of the string logged when a VM exceeds the LOCAL_MAX_MEMORY property
static final String ExceededLocalMaxMemoryMsg = "has exceeded local maximum memory configuration ";

// shared counter name
static final String CounterPrefix = "AlertForLocalMaxMemory";

protected static final int FIXED_LENGTH_KEY_SIZE = 20;


// ========================================================================
// initialization methods
    
/** Creates and initializes the singleton instance of FillTest in this VM.
 */
public synchronized static void HydraTask_initializeDataStore() {
   if (testInstance == null) {
      testInstance = new FillTest();
      ((FillTest)testInstance).initializeWithRegDef("region1");
      testInstance.initializeInstance();
   }
}
    
/** Creates and initializes the singleton instance of FillTest in this VM.
 */
public synchronized static void HydraTask_initializeAccessor() {
   if (testInstance == null) {
      testInstance = new FillTest();
      ((FillTest)testInstance).initializeForAccessor("region1");
      testInstance.initializeInstance();
   }
}
    
/** Initialize the region with a different value of LOCAL_MAX_MEMORY
 *  for each VM.
 */
protected void initializeWithRegDef(String specName) {
   cacheDef = CacheDefinition.createCacheDefinition(CacheDefPrms.cacheSpecs, "cache1");
   regDef = RegionDefinition.createRegionDefinition(RegionDefPrms.regionSpecs, specName);
   Properties localProps = regDef.getParRegLocalProperties();
   if (localProps == null)
      localProps = new Properties();
   myLocalMaxMemoryMB = ParRegBB.getBB().getSharedCounters().incrementAndRead(ParRegBB.LocalMaxMemoryCounter);
   localProps.setProperty("LOCAL_MAX_MEMORY", String.valueOf(myLocalMaxMemoryMB));
   regDef.setParRegLocalProperties(localProps);
   String key = VmIDStr + RemoteTestModule.getMyVmid();
   String xmlFile = key + ".xml";
   DeclarativeGenerator.createDeclarativeXml(key + ".xml", cacheDef, regDef, true);
   aRegion = CacheUtil.createRegion(cacheDef, regDef, xmlFile);
}

/** Initialize the region as an accessor that does not participate
 *  in exceeded LocalMaxMemory.
 *  This VM is the thread that does the puts, that fill the other VMs
 *  to the localMaxMemory size.
 */
protected void initializeForAccessor(String specName) {
   cacheDef = CacheDefinition.createCacheDefinition(CacheDefPrms.cacheSpecs, "cache1");
   regDef = RegionDefinition.createRegionDefinition(RegionDefPrms.regionSpecs, specName);
   Properties localProps = regDef.getParRegLocalProperties();
   if (localProps == null)
      localProps = new Properties();
   localProps.setProperty("LOCAL_MAX_MEMORY", String.valueOf(0));
   regDef.setParRegLocalProperties(localProps);
   if (regDef.getDataPolicy() != null) {
      if (regDef.getDataPolicy() == DataPolicy.PERSISTENT_PARTITION) { // persistence not allowed for accessors
        regDef.setDataPolicy(DataPolicy.PARTITION);
      }
   }
   String key = VmIDStr + RemoteTestModule.getMyVmid();
   String xmlFile = key + ".xml";
   DeclarativeGenerator.createDeclarativeXml(key + ".xml", cacheDef, regDef, true);
   aRegion = CacheUtil.createRegion(cacheDef, regDef, xmlFile);
}

/** initialize instance fields of this class 
 */
public void initializeInstance() {
   super.initializeInstance();
   numVMs = TestHelper.getNumVMs();
   if (myLocalMaxMemoryMB != 0) {
      myAlertCounterName = CounterPrefix + myLocalMaxMemoryMB;
      myAlertCounter = ParRegBB.getBB().getSharedCounter(myAlertCounterName);
      myAlertCounterName = "ParRegBB." + myAlertCounterName;
      long localMaxMemoryBytes = myLocalMaxMemoryMB * 1024 * 1024;
      // for now the pr product does not account for keys, only values
      int entrySize = TestConfig.tab().intAt(RandomValuesPrms.elementSize);
      long numEntriesForLocalMaxMem = localMaxMemoryBytes / entrySize;
      int allowance = (int)((localMaxMemoryBytes * 0.25) / (double)(entrySize)); // 20% of the local max memory
      allowance++; // most test failures are (for some reason) off by one, so allow one more 
                   // to get more runs to pass
      lowerThresholdNumEntries = numEntriesForLocalMaxMem - allowance;
      upperThresholdNumEntries = numEntriesForLocalMaxMem + allowance;
      Log.getLogWriter().info("    Local max memory for this vm: " + myLocalMaxMemoryMB + "MB\n" + 
                              "                      Entry size: " + entrySize + "bytes\n" +
                              "Num entries for local max memory: " + numEntriesForLocalMaxMem + "\n" +
                              "       Num entries for allowance: " + allowance + "\n" +
                              "     Lower threshold num entries: " + lowerThresholdNumEntries + "\n" +
                              "     Upper threshold num entries: " + upperThresholdNumEntries);
   }
}

/** Adds an alert listener to the admin distributed system.
 */
public synchronized static void HydraTask_addAlertListener() {
   if (!alertAdded) {
      ParRegAlertListener listener = new ParRegAlertListener();
      Log.getLogWriter().info("Adding " + listener + " to " + AdminTest.testInstance.ds);
      AdminTest.testInstance.ds.addAlertListener(listener);
      alertAdded = true;
   }
}

public static void HydraTask_disconnect() {
  CacheUtil.disconnect();
  testInstance = null;
  ParRegBB.getBB().getSharedCounters().zero(ParRegBB.LocalMaxMemoryCounter);
}
// ========================================================================
// test methods
public static void HydraTask_addToRegion() {
   ((FillTest)testInstance).addToRegion();
}

protected void addToRegion() {
   long startTime = System.currentTimeMillis();
   do {
      Object key = getFixedLengthKey();
      Object value = randomValues.getRandomObject();
      Log.getLogWriter().info("Putting key " + key + " value " + TestHelper.toString(value));
      aRegion.put(key, value);
      Log.getLogWriter().info("Done putting key " + key);
      MasterController.sleepForMs(300);
      checkAlertError();
   } while (System.currentTimeMillis() - startTime < minTaskGranularityMS);
   endTestIfOver();
}

  public static void HydraTask_addToRegionWithPartitionResolver() {
    ((FillTest)testInstance).addToRegionWithPartitionResolver();
  }

  protected void addToRegionWithPartitionResolver() {
    if (TestConfig.tab().stringAt(parReg.ParRegPrms.partitionResolverData)
        .equalsIgnoreCase("callbackarg")) {
      Log.getLogWriter().info("Inside callback");
      addToRegionCallbackPartitionResolver();
    }
    else {
      if (TestConfig.tab().stringAt(parReg.ParRegPrms.partitionResolverData)
          .equalsIgnoreCase("key")) {
        Log.getLogWriter().info("Inside key");
        addToRegionKeyPartitionResolver();
      }
      else {
        if (TestConfig.tab().stringAt(parReg.ParRegPrms.partitionResolverData)
            .equalsIgnoreCase("value")) {
          Log.getLogWriter().info("Inside value");
          addToRegionValueRoutingResolver();
        }
      }
    }
  }

  protected void addToRegionValueRoutingResolver() {
    long startTime = System.currentTimeMillis();
    do {
      String keyName = getFixedLengthKey().toString();
      Object valueObject = randomValues.getRandomObject();
      Log.getLogWriter().info(
          "Putting key " + keyName + " value "
              + TestHelper.toString(valueObject));
      Month routingObjectHolder = Month.months[TestConfig.tab().getRandGen()
          .nextInt(11)];
      PartitionObjectHolder key = new PartitionObjectHolder(keyName,
          routingObjectHolder);
      PartitionObjectHolder value = new PartitionObjectHolder(valueObject,
          routingObjectHolder);
      aRegion.put(key, value);
      Log.getLogWriter().info("Done putting key " + key);
      MasterController.sleepForMs(300);
      checkAlertError();
    } while (System.currentTimeMillis() - startTime < minTaskGranularityMS);
    endTestIfOver();
  }

  protected void addToRegionKeyPartitionResolver() {
    long startTime = System.currentTimeMillis();
    do {
      String keyName = getFixedLengthKey().toString();
      Object value = randomValues.getRandomObject();
      Log.getLogWriter().info(
          "Putting key " + keyName + " value " + TestHelper.toString(value));
      Month routingObjectHolder = Month.months[TestConfig.tab().getRandGen()
          .nextInt(11)];
      KeyResolver key = new KeyResolver(keyName, routingObjectHolder);
      aRegion.put(key, value);
      Log.getLogWriter().info("Done putting key " + key);
      MasterController.sleepForMs(300);
      checkAlertError();
    } while (System.currentTimeMillis() - startTime < minTaskGranularityMS);
    endTestIfOver();
  }

  protected void addToRegionCallbackPartitionResolver() {
    long startTime = System.currentTimeMillis();
    do {
      String keyName = getFixedLengthKey().toString();
      Object value = randomValues.getRandomObject();
      Log.getLogWriter().info(
          "Putting key " + keyName + " value " + TestHelper.toString(value));
      Month routingObjectHolder = Month.months[TestConfig.tab().getRandGen()
          .nextInt(11)];
      PartitionObjectHolder key = new PartitionObjectHolder(keyName,
          routingObjectHolder);
      KeyResolver callBackArg = new KeyResolver(keyName,routingObjectHolder);
      aRegion.put(key, value, callBackArg);
      Log.getLogWriter().info("Done putting key " + key);
      MasterController.sleepForMs(300);
      checkAlertError();
    } while (System.currentTimeMillis() - startTime < minTaskGranularityMS);
    endTestIfOver();
  }

  
public static void HydraTask_monitorAlerts() {
   ((FillTest)testInstance).monitorAlerts();
}

protected void monitorAlerts() {
   long startTime = System.currentTimeMillis();
   do {
      long counter = ParRegBB.getBB().getSharedCounters().read(myAlertCounter);
      // Because of bug 35183 (admin alert is not sent to all VMs with alert listener
      // installed) we need to poll the counters to find out when this VM sends alerts
      // to all others (because of 35183, we do not get our own alert)
      if (counter > 0) {
         // this VM caused an alert
         long localSize = ParRegUtil.getLocalSize(aRegion);
         String key = "Alert-" + myLocalMaxMemoryMB + "MB: approximate num keys at time of alert";
         ParRegBB.getBB().getSharedMap().put(key, new Long(localSize));
         Log.getLogWriter().info("Detected alert for localMaxMemory " + myLocalMaxMemoryMB + 
            ", put key " + key + " value " + localSize + " into blackboard");
         throw new StopSchedulingTaskOnClientOrder("This VM caused an alert for localMaxMemory " + 
            myLocalMaxMemoryMB + "; local data store size is " + localSize +
            ", upperThresholdNumEntries: " + upperThresholdNumEntries +
            ", lowerThresholdNumEntries: " + lowerThresholdNumEntries);
      }
      MasterController.sleepForMs(50);
   } while (System.currentTimeMillis() - startTime < minTaskGranularityMS);
}

// ========================================================================
// close task methods
    
/** Verify the alerts caused by exceeding local max memory.
 */
public synchronized static void HydraTask_verify() {
   ((FillTest)testInstance).verify();
}
    
/** Verify that each alert for this VM's localMaxMemory setting occurred at the correct time.
 */
protected void verify() {
   ParRegBB.getBB().print();
   checkAlertError();
   StringBuffer errStr = new StringBuffer();

   // check that we got the correct number of alerts
   long counterValue = ParRegBB.getBB().getSharedCounters().read(myAlertCounter);
   Log.getLogWriter().info("BB counter " + myAlertCounterName + " has value " + counterValue);
   int numAlertsExpected = numVMs - 1;
   if (counterValue != numAlertsExpected) {
      errStr.append("Expected " + numAlertsExpected + " alerts to be received for exceeded this " +
         "VM's localMaxMemory setting of " + myLocalMaxMemoryMB + ", but received " + counterValue + "\n");
   }

   // check that each alert that occurred for this VM's localMaxMemory setting occurred at the correct time
   boolean foundAlert = false;
   Map sharedMap = ParRegBB.getBB().getSharedMap().getMap();
   Iterator it = sharedMap.keySet().iterator();
   while (it.hasNext()) {
      Object key = it.next();
      Log.getLogWriter().info("Considering key " + key);
      if (key instanceof String) {
         String keyStr = (String)key;
         if (keyStr.indexOf("Alert") < 0) {
            continue;
         }
         int index1 = keyStr.indexOf("MB");
         int index2 = keyStr.lastIndexOf("-", index1-1); 
         int maxMemoryForThisAlert = (Integer.valueOf(keyStr.substring(index2+1, index1))).intValue();
         Log.getLogWriter().info("For key " + key + ", maxMemoryForThisAlert is " + maxMemoryForThisAlert);

         if (maxMemoryForThisAlert == myLocalMaxMemoryMB) { 
            // this alert occurred for this vm's localMaxMemory setting; check the num entries
            foundAlert = true;
            long numEntriesAtAlertTime = ((Long)(sharedMap.get(key))).longValue();
            if ((numEntriesAtAlertTime < lowerThresholdNumEntries) ||
                (numEntriesAtAlertTime > upperThresholdNumEntries)) {
               errStr.append("Alert for localMaxMemory " + maxMemoryForThisAlert + 
                  " occurred at unexpected time, num entries in this VM at the time of the alert is " + 
                  numEntriesAtAlertTime + ", but expected it to be between " + lowerThresholdNumEntries + 
                  " and " + upperThresholdNumEntries);
            } else {
               Log.getLogWriter().info("Alert for localMaxMemory " + maxMemoryForThisAlert + 
                  " occurred at expected time, num entries in this VM at the time of the alert is " + 
                  numEntriesAtAlertTime + ", lower theshold is " + lowerThresholdNumEntries + 
                  " and upperThreshold is " + upperThresholdNumEntries);
            }
            break;
         }
      }
   }

   if (!foundAlert) {
      errStr.append("Did not detect an alert for localMaxMemory setting " + myLocalMaxMemoryMB + "\n");
   }

   if (errStr.length() != 0) {
      throw new TestException(errStr.toString());
   }
}

  /**
   * Task to verify custom partitioning.
   */
  public synchronized static void HydraTask_verifyCustomPartitioning() {
    ((FillTest)testInstance).verifyCustomPartitioning();
  }

  protected void verifyCustomPartitioning() {

    PartitionedRegion pr = (PartitionedRegion)aRegion;
    int totalBuckets = pr.getTotalNumberOfBuckets();
    RegionAttributes attr = aRegion.getAttributes();
    PartitionAttributes prAttr = attr.getPartitionAttributes();
    int redundantCopies = prAttr.getRedundantCopies();
    int expectedNumCopies = redundantCopies + 1;
    int verifyBucketCopiesBucketId = 0;

    while (true) {

      if (verifyBucketCopiesBucketId >= totalBuckets) {
        break; // we have verified all buckets
      }

      Log.getLogWriter().info(
          "Verifying data for bucket id " + verifyBucketCopiesBucketId
              + " out of " + totalBuckets + " buckets");
      List<BucketDump> listOfMaps = null;
      try {
        listOfMaps = pr.getAllBucketEntries(verifyBucketCopiesBucketId);
      }
      catch (ForceReattemptException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      }

      // check that we have the correct number of copies of each bucket
      // listOfMaps could be size 0; this means we have no entries in this
      // particular bucket
      int size = listOfMaps.size();
      if (size == 0) {
        Log.getLogWriter().info(
            "Bucket " + verifyBucketCopiesBucketId + " is empty");
        verifyBucketCopiesBucketId++;
        continue;
      }
      
      if (size != expectedNumCopies) {
        throw new TestException("For bucketId " + verifyBucketCopiesBucketId
            + ", expected " + expectedNumCopies + " bucket copies, but have "
            + listOfMaps.size());
      }
      else {
        Log.getLogWriter().info(
            "For bucketId " + verifyBucketCopiesBucketId + ", expected "
                + expectedNumCopies + " bucket copies, and have "
                + listOfMaps.size());
      }

      Log.getLogWriter().info(
          "Validating co-location for all the redundant copies of the bucket with Id : "
              + verifyBucketCopiesBucketId);
      // Check that all copies of the buckets have the same data
      for (int i = 0; i < listOfMaps.size(); i++) {
        BucketDump dump = listOfMaps.get(i);
        Map map = dump.getValues();
        verifyCustomPartition(map, verifyBucketCopiesBucketId);
        verifyUniqueBucketForCustomPartioning(verifyBucketCopiesBucketId);
      }

      verifyBucketCopiesBucketId++;
    }

  }

  protected void verifyCustomPartition(Map map, int bucketid) {

    Iterator iterator = map.entrySet().iterator();
    Map.Entry entry = null;
    RoutingHolder key = null;

    while (iterator.hasNext()) {
      entry = (Map.Entry)iterator.next();
      if (entry.getKey() instanceof KeyResolver) {
        key = (KeyResolver)entry.getKey();
      }
      else if (entry.getKey() instanceof PartitionObjectHolder) {
        key = (PartitionObjectHolder)entry.getKey();
      }

      if (ParRegBB.getBB().getSharedMap().get(
          "RoutingObjectForBucketid:" + bucketid) == null) {
        Log.getLogWriter().info(
            "RoutingObject for the bucket id to be set in the BB");
        ParRegBB.getBB().getSharedMap().put(
            "RoutingObjectForBucketid:" + bucketid,
            key.getRoutingHint().toString());
        ParRegBB.getBB().getSharedMap().put(
            "RoutingObjectKeyBucketid:" + bucketid, key);
        Log.getLogWriter().info(
            "BB value set to " + key.getRoutingHint().toString());
      }
      else {
        Log.getLogWriter().info("Checking the value for the routing object ");
        String blackBoardRoutingObject = (String)ParRegBB.getBB()
            .getSharedMap().get("RoutingObjectForBucketid:" + bucketid);
        String keyRoutingObject = key.getRoutingHint().toString();
        if (!keyRoutingObject.equalsIgnoreCase(blackBoardRoutingObject)) {
          throw new TestException(
              "Expected same routing objects for the entries in this bucket id "
                  + bucketid + "but got different values "
                  + blackBoardRoutingObject + " and " + keyRoutingObject);
        }
        else {
          Log.getLogWriter().info(
              "Got the expected values "
                  + blackBoardRoutingObject
                  + " and "
                  + keyRoutingObject
                  + " for the keys "
                  + ParRegBB.getBB().getSharedMap().get(
                      "RoutingObjectKeyBucketid:" + bucketid) + " and " + key);
        }
      }
    }
  }

  /**
   * Task to verify that there is only a single bucket id for a routing Object
   * 
   */
  protected void verifyUniqueBucketForCustomPartioning(int bucketId) {

    if (bucketId == 0) {
      Log
          .getLogWriter()
          .info(
              "This is the first bucket, so no validation required as there is no bucket to be compared");
      return;
    }
    else {
      for (int i = 0; i < bucketId; i++) {
        if (!(ParRegBB.getBB().getSharedMap().get(
            "RoutingObjectForBucketid:" + i) == null)) {
          String referenceValue = (String)ParRegBB.getBB().getSharedMap().get(
              "RoutingObjectForBucketid:" + i);
          String currentValue = (String)ParRegBB.getBB().getSharedMap().get(
              "RoutingObjectForBucketid:" + bucketId);
          Log.getLogWriter().info("currentValue: " + currentValue);
          Log.getLogWriter().info("referenceValue: " + referenceValue);

          if (currentValue.equalsIgnoreCase(referenceValue)) {
            throw new TestException("Two buckets with the id " + i + " and "
                + bucketId + " have the same routing Object " + referenceValue);
          }
          else {
            Log.getLogWriter().info(
                "As expected the bucket with ids " + i + " and " + bucketId
                    + " have the different routing Object " + currentValue
                    + " and " + referenceValue);
          }

        }

      }
    }

  }

  public static void dumpBuckets() {
    ((FillTest)testInstance).dumpAllTheBuckets();
  }

  public void dumpAllTheBuckets() {
    try {
      PartitionedRegion pr = (PartitionedRegion)aRegion;
      pr.dumpAllBuckets(false,Log.getLogWriter().convertToLogWriterI18n());
    }
    catch (com.gemstone.gemfire.distributed.internal.ReplyException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
    catch (ClassCastException e) {
      Log
          .getLogWriter()
          .info(
              "Not dumping data stores on "
                  + aRegion
                  + " because it is not a PartitionedRegion (probably because it's a region "
                  + " from a bridge client");
    }
  }

  public static void HydraTask_verifyPR() {
    ((FillTest)testInstance).verifyPR();
  }

  public void verifyPR() {

    // verifying the PR metadata
    try {
      ParRegUtil.verifyPRMetaData(aRegion);
    }
    catch (Exception e) {
      throw new TestException(e.getMessage());
    }
    catch (TestException e) {
      throw new TestException(e.getMessage());
    }

    // verifying the primaries
    try {
      ParRegUtil.verifyPrimaries(aRegion, redundantCopies);
    }
    catch (Exception e) {
      throw new TestException(e.getMessage());
    }
    catch (TestException e) {
      throw new TestException(e.getMessage());
    }
  }

// ========================================================================

// other methods

/** Return the expected alert message for this VM's LOCAL_MAX_MEMORY
 */
static String getExpectedAlertMsg(int MB) {
   return ExceededLocalMaxMemoryMsg + MB + " Mb, currentsize is " + MB + " Mb";
}

/** Check if an alert has recorded an error in the blackboard.
 */
protected void checkAlertError() {
   Object error = ParRegBB.getBB().getSharedMap().get(ParRegBB.ErrorKey);
   if (error != null)
      throw new TestException(error.toString());
}

/** Deteremine if the test is over and if it is, throw StopSchedulingException.
 *  The test is over when all alerts have been received
 */
protected void endTestIfOver() {
   long largestLMM = ParRegBB.getBB().getSharedCounters().read(ParRegBB.LocalMaxMemoryCounter); 
   for (int i = 1; i <= largestLMM; i++) {
      String counterName = CounterPrefix + i;
      int counter = ParRegBB.getBB().getSharedCounter(counterName);
      counterName = "ParRegBB." + counterName;
      long counterValue = ParRegBB.getBB().getSharedCounters().read(counter);
      // Because of bug 35183 (admin alert is not sent to all VMs with alert listener
      // installed) 
      if (counterValue < numVMs - 1) { 
         // did not receive all alerts for this counter; test is not over
         return;
      }
   }
   throw new StopSchedulingOrder("All alerts have been caused and received");
}

protected static Object getFixedLengthKey() {
   String key = NameFactory.getNextPositiveObjectName().toString();
   StringBuffer aStr = new StringBuffer();
   aStr.append(key);
   while (aStr.length() < FIXED_LENGTH_KEY_SIZE) {
      aStr.append(" ");
   }
   return aStr.toString();
}

}
