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
package diskReg.oplogs;

import mapregion.*;
import diskReg.DiskRegUtil;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.internal.cache.TestHelperForHydraTests;
import hydra.*;
import objects.*;
import java.util.*;
import util.*;
import diskReg.DiskRegPrms;

/**
 * The aim of this test to test the recovery of disk regions from oplogs
 * This test is modified for crash recovery testsing
 * @author prafulla
 * @since 5.1
 */

public class DiskRegRecoveryFromOplogsTest
{

  static DiskRegRecoveryFromOplogsTest testInstance;
  static private RegionAttributes attr;
  static private Cache cache;
  static Region diskRegion = null;
  static String regionName;

  protected static final int DISK_FOR_PERSIST = 1;
  protected static final int DISK_FOR_OVRFLW_PERSIST = 2;

  protected static final String INIT_REGION_SIZE = "initial region size";

  protected int numVMsToStop;                 // value of MapPrms.numVMsToStop

  ////////constructor
  public DiskRegRecoveryFromOplogsTest (){}

  /**
   * Hydra task to connect to distributed system abd initialize cache.
   * It also creates a disk region and initializes the attributes of a region.
   *
   */
  public static synchronized void HydraTask_initialize() {
    if(testInstance == null){
      testInstance = new DiskRegRecoveryFromOplogsTest();
      testInstance.initialize();
    }
  }//end of HydraTask_initialize

  protected void initialize (){
    try{
    	if (cache == null || cache.isClosed()) {
      	  cache = CacheHelper.createCache(ConfigPrms.getCacheConfig());
        }
        
        TestHelperForHydraTests.setIssueCallbacksOfCacheObserver(true);
        
        attr = RegionHelper.getRegionAttributes(ConfigPrms.getRegionConfig());
        regionName = RegionHelper.getRegionDescription(ConfigPrms.getRegionConfig()).getRegionName();

      createDiskRegion ();

      boolean populateCache = TestConfig.tab().booleanAt(MapPrms.populateCache, false);

      if(populateCache)
      {
        addInBatches();
        int sizeInCurrentVm = diskRegion.size();
        Integer sizeInt = (Integer) DiskBB.getBB().getSharedMap().get(INIT_REGION_SIZE);
        int sizeInAnotherVM = 0;
        if (sizeInt != null)
        {
          sizeInAnotherVM = sizeInt.intValue();
        }

        if ( sizeInAnotherVM == 0 || sizeInCurrentVm < sizeInAnotherVM )
        {
          DiskBB.getBB().getSharedMap().put(INIT_REGION_SIZE, new Integer (sizeInCurrentVm));
        }

        Log.getLogWriter().info("DiskRegion is initialized with "+sizeInCurrentVm+" entries");

      }

      numVMsToStop = TestConfig.tab().intAt(MapPrms.numVMsToStop, 1);

    }catch(Exception ex){
      throw new TestException (TestHelper.getStackTrace(ex));
    }
  }// initialize

  protected void createDiskRegion (){
    try{
      diskRegion = RegionHelper.createRegion(regionName, attr);
      Log.getLogWriter().info("diskRegion size after recovery is: "+diskRegion.size());
    }catch(Exception ex){
      throw new TestException (TestHelper.getStackTrace(ex));
    }
  }

  /**
   * Hydra task to add entries into region.
   *
   */
  public static void HydraTask_addEntries() {
    //1. Add entries into the region
    testInstance.addInBatches();
    int endTestOnNumKeysInRegion = TestConfig.tab().intAt(DiskRegPrms.endTestOnNumKeysInRegion, -1);
    try{
      if (endTestOnNumKeysInRegion > -1) {       
        int numKeys = testInstance.diskRegion.keySet().size();
        Log.getLogWriter().info("Current numKeys is " + numKeys + ", endTestOnNumKeysInRegion is " +
           endTestOnNumKeysInRegion);
        if (numKeys >= endTestOnNumKeysInRegion) {
          throw new StopSchedulingTaskOnClientOrder("Workload based test has " + numKeys + 
             " keys in region, endTestOnNumKeysInRegion is " + endTestOnNumKeysInRegion);
        }
      }      
    }
    catch (CancelException cclex){
        Log.getLogWriter().info("CancelException may occur in concurrent environment - continuing with test: "  + cclex);
    }
    catch(RegionDestroyedException rdex){
      Log.getLogWriter().info("RegionDestroyedException may occur in concurrent environment - continuing with test");
    }  
  }//end of HydraTask_addEntries

  static volatile int putKeyInt = 0;
  static Object putLock = "put";

  protected void addInBatches (){
    // add entries into the region for a particular amount of time.
    // note down how many entries are added into region
    try{
      long limit = TestConfig.tab().longAt(MapPrms.timeForPutOperation);
      Object key=null, val=null, returnObj=null;
      String objectType = MapPrms.getObjectType();

      long startTime = System.currentTimeMillis();
      do {
        //// this synchronization would ensure that the thread will put an unique key inside the region
        synchronized (putLock) {
          key = NameFactory.getNextPositiveObjectName();
          putKeyInt = (int) NameFactory.getCounterForName(key);
          val = ObjectHelper.createObject(objectType, putKeyInt);
        }

        diskRegion.put(key, val);

        DiskBB.getBB().incrementCounter("DiskBB.NUM_PUT ", DiskBB.NUM_PUT);

      } while((System.currentTimeMillis() - startTime) < limit);

    }catch(RegionDestroyedException rdex){
      Log.getLogWriter().info("RegionDestroyedException may occur in concurrent environment - continuing with test");
      Log.getLogWriter().info("recovering region");
      recoverRegion();
    }
    catch (CancelException cclex){
      Log.getLogWriter().info("CancelException may occur in concurrent environment - continuing with test: " + cclex);
    }
    catch(Exception ex){
      throw new TestException (TestHelper.getStackTrace(ex));
    }
  }//end of addInBatches

  public static void HydraTask_closeRegion() {
    testInstance.closeRegion();
  }//end of HydraTask_destroyRegion

  protected void closeRegion(){
    try{
      diskRegion.close();
    }catch(RegionDestroyedException rdex){
      Log.getLogWriter().info("RegionDestroyedException may occur in concurrent environment - continuing with test");
      Log.getLogWriter().info("recovering region");
      recoverRegion();
    } catch(Exception ex){
      throw new TestException (TestHelper.getStackTrace(ex));
    }
  }//end of destroyRegion

  protected synchronized void recoverRegion(){
    try{
      if (diskRegion == null || diskRegion.isDestroyed())
      {
        diskRegion = cache.createRegion(regionName, attr);
        Log.getLogWriter().info("created region: "+diskRegion);
      }
    } catch (RegionExistsException rex){
      Log.getLogWriter().info("RegionExistsException may occur in concurrent environment - continuing with test");
    }
    catch (CancelException cclex){
      Log.getLogWriter().info("CancelException may occur in concurrent environment - continuing with test: " + cclex);
    }
    catch(Exception ex){
      throw new TestException (TestHelper.getStackTrace(ex));
    }
  }//end of recoverRegion


  public static synchronized void HydraTask_closeTask() {
    if (testInstance != null){
      testInstance.validateRegionAfterRecovery();
      testInstance.closeCache();
      testInstance = null;
    }
  }//end of HydraTask_closeTask

  protected void validateRegion(){
    try{
      if(cache != null || (!cache.isClosed())){
        if(diskRegion.isDestroyed()) {
          reinitialize();
        }
        int numOfEntries = (int) DiskBB.getBB().getSharedCounters().read(DiskBB.NUM_PUT);        
        int diskRegSize = diskRegion.size();
        int nameFactoryCntr = (int) NameFactory.getPositiveNameCounter();
        if( ! MapPrms.getCloseCacheDuringGii()) {
          if( ( nameFactoryCntr - numOfEntries) < ( nameFactoryCntr - diskRegSize) ){
            throw new TestException ("Region recovery failure. Number of puts happened are: "+numOfEntries+". But Region size is: "+diskRegSize);
          }
        } else {
          Integer minRegSize = (Integer) DiskBB.getBB().getSharedMap().get(INIT_REGION_SIZE);
          if (minRegSize != null){
            numOfEntries = minRegSize.intValue();
          }

          if(Log.getLogWriter().fineEnabled()){
            Log.getLogWriter().info("diskRegSize: "+diskRegSize +". numOfEntries "+numOfEntries);
          }

          if(diskRegSize < numOfEntries){
            throw new TestException ("Region recovery failure. Min. number of puts happened are: "+numOfEntries+". But Region size is: "+diskRegSize);
          }
        }
        Set keySet = diskRegion.keySet();
        Iterator itr = keySet.iterator();

        Object key, value;
        int keyInt;

        while (itr.hasNext()){
          key = itr.next();
          keyInt = (int) NameFactory.getCounterForName(key);
          value = diskRegion.get(key);
          ObjectHelper.validate(keyInt, value);
        }
        Log.getLogWriter().info("Region recovery is successful");

      }
    } catch(Exception ex){
      throw new TestException (TestHelper.getStackTrace(ex));
    }
  }//end of validateRegion

  protected synchronized void closeCache(){
    try{
      if(cache != null || (!cache.isClosed())){
        CacheHelper.closeCache();
      }
    } catch(Exception ex){
      throw new TestException (TestHelper.getStackTrace(ex));
    }
  }//end of closeCache

  public static synchronized void HydraTask_crashVM() {
    testInstance.crashVM();
    if(MapPrms.getCloseCacheDuringGii())
    {
      testInstance.reinitialize();
    }
  }//end of HydraTask_crashVM

  protected void crashVM (){
    try{
      Log.getLogWriter().info("In crashVM ...");
      Vector otherVmIds = ClientVmMgr.getOtherClientVmids();
      for (int i = 0; i < otherVmIds.size(); i++) {
        DiskBB.getBB().getSharedCounters().zero(DiskBB.VmReinitializedCounter);
         DiskBB.getBB().getSharedCounters().zero(DiskBB.VmRestartedCounter);

         // get the VMs to stop
         ArrayList vmList = new ArrayList();
         ArrayList stopModeList = new ArrayList();
         Integer vmID = (Integer)(otherVmIds.get(i));
         ClientVmInfo targetVm = new ClientVmInfo(vmID, null, null);
         vmList.add(targetVm);
         stopModeList.add(ClientVmMgr.MeanKill);
         otherVmIds.remove(i);
         for (int j = 1; j < numVMsToStop; j++) {
            int randInt = TestConfig.tab().getRandGen().nextInt(0, otherVmIds.size()-1);
            vmID = (Integer)(otherVmIds.get(randInt));
            targetVm = new ClientVmInfo(vmID, null, null);
            vmList.add(targetVm);
            stopModeList.add(ClientVmMgr.MeanKill);
            otherVmIds.remove(randInt);
         }
         Log.getLogWriter().info("numVMsToStop is " + numVMsToStop + ", stopping " + vmList);

         // concurrently stop all the targeted VMs
         List threadList = StopStartVMs.stopStartAsync(vmList,
                                                       stopModeList);

         if(MapPrms.getCloseCacheDuringGii()){
           // wait for the vms to restart
           TestHelper.waitForCounter( DiskBB.getBB(),
                                     "DiskBB.VmRestartedCounter",
                                     DiskBB.VmRestartedCounter,
                                     1,
                                     false,
                                     120000 ); // wait for maximum 2 minutes to restart a vm

           //close the cache while gii is happening in other vms from this vm
           Log.getLogWriter().info("Closing cache for gii");
           cache.close();
           Log.getLogWriter().info("Closed cache");
         }

         testInstance.reinitialize(); // other vms will be waiting for this vm to be first because it was the member with the latest disk files
         // wait indefinitely (maxResultWaitSec) for the vms to reinitialize
         StopStartVMs.joinStopStart(vmList, threadList);
      }

      Thread.sleep(2000);//sleep current thread for 2 seconds to let all the vms back into their state.

      Log.getLogWriter().info("Done crashing and restarting vms");
    }
    catch(Exception ex){
      throw new TestException (TestHelper.getStackTrace(ex));
    }
  }//crashVM

  public static synchronized void HydraTask_Reinitialize() {
    if (testInstance == null){
      testInstance = new DiskRegRecoveryFromOplogsTest();
      long counter = DiskBB.getBB().getSharedCounters().incrementAndRead(DiskBB.VmRestartedCounter);
      Log.getLogWriter().info("VmRestartedCounter is now " + counter);
      testInstance.reinitialize();
      counter = DiskBB.getBB().getSharedCounters().incrementAndRead(DiskBB.VmReinitializedCounter);
      Log.getLogWriter().info("VmReinitializedCounter is now " + counter);
    }
  } //end of HydraTask_Reinitialize

  protected void reinitialize (){
    try{
      if(cache == null || cache.isClosed()){
        DiskRegRecoveryFromOplogsTest.cache = CacheHelper.createCache(ConfigPrms.getCacheConfig());
      }
      // everytime take parameters from test config file for region definition.
      // this will make sure that different values are picked up from config files for DiskStore.
      attr = RegionHelper.getRegionAttributes(ConfigPrms.getRegionConfig());
      regionName = RegionHelper.getRegionDescription(ConfigPrms.getRegionConfig()).getRegionName();

      if(MapPrms.getCloseCacheDuringGii()){
        com.gemstone.gemfire.internal.cache.InitialImageOperation.slowImageProcessing = 1;
      }

      createDiskRegion ();

      numVMsToStop = TestConfig.tab().intAt(MapPrms.numVMsToStop, 1);
    } catch(Exception ex) {
      throw new TestException (TestHelper.getStackTrace(ex));
    }
  } // reinitialize

  protected void validateRegionAfterRecovery()
  {
    try
    {
      validateRegion();
      int regionSize = diskRegion.size();      
      Log.getLogWriter().info("regionSize: "+regionSize);
      boolean persistentReplicate;
      int evictionLimit = 0, regionType = 0;

      // set the selected disk attributes in this test instance's field
     
      persistentReplicate = attr.getDataPolicy().isPersistentReplicate();
     
     evictionLimit = attr.getEvictionAttributes().getMaximum();

      // set the type of region for this VM
      if (persistentReplicate) {
         if (evictionLimit <= 0)
            regionType = DISK_FOR_PERSIST;
         else
            regionType = DISK_FOR_OVRFLW_PERSIST;
      }     

      switch (regionType) {
      case DISK_FOR_PERSIST:
        {
        int numEntriesWrittenToDisk = (int) DiskRegUtil.getNumEntriesWrittenToDisk(diskRegion);
        int numEntriesInVm = (int) DiskRegUtil.getNumEntriesInVM(diskRegion);
        int numEntriesOverflownToDisk = (int) DiskRegUtil.getNumOverflowOnDisk(diskRegion);
        int numRecoveredEntriesFromDisk = (int) DiskRegUtil.getRecoveredEntryCount(diskRegion);

        Log.getLogWriter().info("numEntriesWrittenToDisk: "+numEntriesWrittenToDisk);
        Log.getLogWriter().info("numEntriesInVm: "+numEntriesInVm);
        Log.getLogWriter().info("numEntriesOverflownToDisk: "+numEntriesOverflownToDisk);
        Log.getLogWriter().info("numRecoveredEntriesFromDisk: "+numRecoveredEntriesFromDisk);

        if(numEntriesWrittenToDisk != (regionSize - numRecoveredEntriesFromDisk) )
        {
          throw new TestException("After region recovery, numEntriesWrittenToDisk is not equal to numEntriesInVm for persistent_replicate region");
        }

        if(numEntriesOverflownToDisk != 0)
        {
          throw new TestException("After region recovery, numEntriesOverflownToDisk is not zero for persistent_replicate region");
        }

        Log.getLogWriter().info("Done with verification of statistics after region recovery for region type:- DISK_FOR_PERSIST");

        } break;

      case DISK_FOR_OVRFLW_PERSIST:
      {
        int numEntriesWrittenToDisk = (int) DiskRegUtil.getNumEntriesWrittenToDisk(diskRegion);
        int numEntriesInVm = (int) DiskRegUtil.getNumEntriesInVM(diskRegion);
        int numEntriesOverflownToDisk = (int) DiskRegUtil.getNumOverflowOnDisk(diskRegion);
        int numRecoveredEntriesFromDisk = (int) DiskRegUtil.getRecoveredEntryCount(diskRegion);

        Log.getLogWriter().info("numEntriesWrittenToDisk: "+numEntriesWrittenToDisk);
        Log.getLogWriter().info("numEntriesInVm: "+numEntriesInVm);
        Log.getLogWriter().info("numEntriesOverflownToDisk: "+numEntriesOverflownToDisk);
        Log.getLogWriter().info("numRecoveredEntriesFromDisk: "+numRecoveredEntriesFromDisk);

        //Mitul : this validation does not hold true in all scenarios
//        if(numEntriesWrittenToDisk != (regionSize - numRecoveredEntriesFromDisk) )
//        {
//          throw new TestException("After region recovery, numEntriesWrittenToDisk is not equal to (regionSize - numRecoveredEntriesFromDisk) for persistent_replicate with overflow region");
//        }

/*        if(numEntriesOverflownToDisk != (regionSize - evictionLimit) )
        {
          throw new TestException("After region recovery, numEntriesOverflownToDisk is not equal to (regionSize - evictionLimit) for persist with overflow region");
        }*/

        Log.getLogWriter().info("Done with verification of statistics after region recovery for region type:- DISK_FOR_OVRFLW_PERSIST");

        } break;

      default:
        break;
      }

    }
    catch (Exception  ex)
    {
      throw new TestException (TestHelper.getStackTrace(ex));
    }

  }// validateRegionAfterRecovery

}// end of DiskRegRecoveryFromOplogsTest
