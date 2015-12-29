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
package parReg.tx;

import hydra.*;
import com.gemstone.gemfire.cache.*;

import java.util.*;
import util.*;

/**
 * 
 * Test to 
 *
 * @see 
 * @see 
 *
 * @author Lynn Hughes-Godfrey
 * @since 6.1
 */
public class TxTest extends util.OperationsClient {

  // Single instance in this VM
  static protected TxTest testInstance;

  private ArrayList errorMsgs = new ArrayList();
  private ArrayList errorException = new ArrayList();

  /**
   *  Create the cache and Region.  Check for forced disconnects (for gii tests)
   */
  public synchronized static void HydraTask_initialize() {
    if (testInstance == null) {
      testInstance = new TxTest();
      testInstance.initializeOperationsClient();

      try {
        testInstance.initializePrms();
        testInstance.initialize();
      } catch (Exception e) {
        Log.getLogWriter().info("initialize caught Exception " + e + ":" + e.getMessage());
        throw new TestException("initialize caught Exception " + TestHelper.getStackTrace(e));
      }  
    }
  }

  /* 
   * Creates cache and region (CacheHelper/RegionHelper)
   */
  protected void initialize() {

    // create cache/region (and connect to the DS)
    if (CacheHelper.getCache() == null) {
      Cache myCache = CacheHelper.createCache(ConfigPrms.getCacheConfig());
      TransactionListener listener = PrTxPrms.getTxListener();
      if (listener != null) {
         myCache.getCacheTransactionManager().setListener(listener);
      }
 
      List regionConfigNames = PrTxPrms.getRegionConfigNames();
      for (Iterator it = regionConfigNames.iterator(); it.hasNext(); ) {
         createRegion((String)it.next());
      }
    }
  }

  protected void createRegion(String regionConfig) {
    RegionDescription rd = RegionHelper.getRegionDescription(regionConfig);
    AttributesFactory factory = RegionHelper.getAttributesFactory(regionConfig);
    String regionName = rd.getRegionName();
    Region aRegion = RegionHelper.createRegion(regionName, factory);
    Log.getLogWriter().info("Created region " + aRegion.getName());

    // put some entries into the region
    for (int i = 0; i < 10; i++) {
       addEntry(aRegion);
    }
  }

  protected void initializePrms() {
  }

  /**
   *  Performs puts/gets on entries in TestRegion
   *  Allows Shutdown and CacheClosedException if the result of forcedDisconnect
   */
  public static void HydraTask_doOpsAcrossAllRegions() {
    try {
       testInstance.doOpsAcrossAllRegions();
    } catch (Exception e) {
       Log.getLogWriter().info("doEntryOperations threw Exception " + e);
       throw new TestException("doEntryOperations caught Exception " + TestHelper.getStackTrace(e));
    }
}

 /** Do random entry operations in each region in the cache as part of a single tx
   */
  protected void doOpsAcrossAllRegions() {

    StringBuffer regionNames = new StringBuffer();
    long startTime = System.currentTimeMillis();
    int numOps = 0;

    // In transaction tests, package the operations into a single transaction
    if (useTransactions) {
      TxHelper.begin();
    }

    Set regions = CacheHelper.getCache().rootRegions();
    for (Iterator it = regions.iterator(); it.hasNext(); ) {

       TestHelper.checkForEventError(PrTxBB.getBB());
       Region aRegion = (Region)it.next();
       regionNames.append(aRegion.getName() + " ");
       Log.getLogWriter().info("Executing operation in " + aRegion.getName());

       int whichOp = getOperation(OperationsClientPrms.entryOperations);
       int size = aRegion.size();
       if (size >= upperThreshold) {
          whichOp = getOperation(OperationsClientPrms.upperThresholdOperations);
       } else if (size <= lowerThreshold) {
          whichOp = getOperation(OperationsClientPrms.lowerThresholdOperations);
       }   

       String lockName = null;
       boolean gotTheLock = false;
       if (lockOperations) {
          lockName = LOCK_NAME + TestConfig.tab().getRandGen().nextInt(1, 20);
          Log.getLogWriter().info("Trying to get distributed lock " + lockName + "...");
          gotTheLock = distLockService.lock(lockName, -1, -1);
          if (!gotTheLock) {
             throw new TestException("Did not get lock " + lockName);
          }
          Log.getLogWriter().info("Got distributed lock " + lockName + ": " + gotTheLock);
       }

       try {
          switch (whichOp) {
             case ENTRY_ADD_OPERATION:
                addEntry(aRegion);
                break;
             case ENTRY_INVALIDATE_OPERATION:
                invalidateEntry(aRegion, false);
                break;
             case ENTRY_DESTROY_OPERATION:
                destroyEntry(aRegion, false);
                break;
             case ENTRY_UPDATE_OPERATION:
                updateEntry(aRegion);
                break;
             case ENTRY_GET_OPERATION:
                getKey(aRegion);
                break;
             case ENTRY_GET_NEW_OPERATION:
                getNewKey(aRegion);
                break;
             case ENTRY_LOCAL_INVALIDATE_OPERATION:
                invalidateEntry(aRegion, true);
                break;
             case ENTRY_LOCAL_DESTROY_OPERATION:
                destroyEntry(aRegion, true);
                break;
             case ENTRY_PUTALL_OPERATION:
               putAll(aRegion);
               break;
             default: {
                throw new TestException("Unknown operation " + whichOp);
             }
         }
      } finally {
         if (gotTheLock) {
            gotTheLock = false;
            distLockService.unlock(lockName);
            Log.getLogWriter().info("Released distributed lock " + lockName);
         }
      }
      numOps++;
      Log.getLogWriter().info("Completed op " + numOps + " for this task");
   } 

    // finish transactions (commit or rollback)
    if (useTransactions) {
       int n = 0;
       int commitPercentage = OperationsClientPrms.getCommitPercentage();
       n = TestConfig.tab().getRandGen().nextInt(1, 100);

       if (n <= commitPercentage) {
         try {
            TxHelper.commit();
         } catch (ConflictException e) {
            Log.getLogWriter().info("ConflictException " + e + " expected, continuing test");
         }
       } else {
           TxHelper.rollback();
       }
    }
    Log.getLogWriter().info("Done in doEntryOperations with regions " + regionNames.toString());
}

}
