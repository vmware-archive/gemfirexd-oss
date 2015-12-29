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
package diskRecovery;

import com.gemstone.gemfire.admin.AdminDistributedSystem;
import com.gemstone.gemfire.admin.AdminException;
import com.gemstone.gemfire.admin.BackupStatus;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.DiskStoreFactory;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.snapshot.CacheSnapshotService;
import com.gemstone.gemfire.cache.snapshot.SnapshotFilter;
import com.gemstone.gemfire.cache.snapshot.SnapshotOptions;
import com.gemstone.gemfire.cache.snapshot.SnapshotOptions.SnapshotFormat;
import com.gemstone.gemfire.cache.persistence.ConflictingPersistentDataException;
import com.gemstone.gemfire.cache.persistence.PersistentID;
import com.gemstone.gemfire.distributed.DistributedSystemDisconnectedException;
import com.gemstone.gemfire.internal.cache.persistence.PersistenceObserverHolder;
import com.gemstone.gemfire.internal.cache.persistence.PersistenceObserverHolder.PersistenceObserver;
import com.gemstone.gemfire.pdx.PdxInstance;

import hydra.CacheHelper;
import hydra.ClientVmInfo;
import hydra.DiskStoreHelper;
import hydra.DiskStorePrms;
import hydra.GsRandom;
import hydra.HostHelper;
import hydra.Log;
import hydra.MasterController;
import hydra.RegionDescription;
import hydra.RegionHelper;
import hydra.RemoteTestModule;
import hydra.TestConfig;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;

import pdx.PdxTest;

import snapshot.SnapshotPrms;

import util.AdminHelper;
import util.CliHelperPrms;
import util.DeclarativeGenerator;
import util.NameFactory;
import util.PRObserver;
import util.PersistenceUtil;
import util.RandomValues;
import util.StopStartVMs;
import util.TestException;
import util.TestHelper;
import util.TestHelperPrms;
import util.BaseValueHolder;
import util.ValueHolder;

/**
 * @author lynn
 * This class contains code that references classes available in GemFire
 * 6.6 (and beyond) but are not available in pre-6.5 version.
 * 
 * The methods in this class are methods that could not be included in
 * diskRecovery.RecoveryTest since diskRecovery.RecoveryTest must be able
 * to run in pre-6.5 versions.
 */
public class RecoveryTestVersionHelper {

  /** Choose a random DiskStore and set it in the attributes factory.
   * 
   * @param baseAttr The region attributes we start with (without a DiskStore set)
   * @param attrFac The attributes factory to set the DiskStore in.
   * @param diskStoreNames A List of potential DiskStore names to choose from.
   * @param rand The random number generator used while creating regions and attributes.
   */
  public static void setRandomDiskStore(RegionAttributes baseAttr, 
      AttributesFactory attrFac, 
      List<String> diskStoreNames,
      GsRandom rand) {
    if ((baseAttr.getDiskStoreName() != null) && (baseAttr.getDiskStoreName().equals("notUsed"))) { // disk store should be chosen randomly
      // setting the diskStoreName to unused is a signal that we do need a disk store and to choose randomly
      String diskStoreName = diskStoreNames.get(rand.nextInt(1, diskStoreNames.size()-1));
      if (diskStoreName.equals("unused")) {
        throw new TestException("Error in test, diskStoreName is " + diskStoreName);
      }
      Log.getLogWriter().info("Test is setting diskStoreName to " + diskStoreName);
      attrFac.setDiskStoreName(diskStoreName);
    }
  }

  /** Create the DiskStore if the RegionAttributes specifies it.
   * 
   * @param attr The RegionAttributes that could specify a DiskStore.
   * @return A String description of the DiskStore, useful for logging.
   */
  public static String createDiskStore(RegionAttributes attr) {
    String diskDirsStr = "";
    Log.getLogWriter().info("DiskStore name in attributes is " + attr.getDiskStoreName());
    if ((attr.getDiskStoreName() != null) && (!attr.getDiskStoreName().equals("notUsed"))) {
      Log.getLogWriter().info("Creating diskStore " + attr.getDiskStoreName());
      DiskStoreFactory dsFactory = DiskStoreHelper.getDiskStoreFactory(attr.getDiskStoreName());
      dsFactory.create(attr.getDiskStoreName());
      File[] diskDirs = CacheHelper.getCache().findDiskStore(attr.getDiskStoreName()).getDiskDirs();
      for (File diskDir: diskDirs) {
        diskDirsStr = diskDirsStr + diskDir.getName() + " ";
      }
      diskDirsStr = " with disk dirs " + diskDirsStr;
    }
    return "disk store name is " + attr.getDiskStoreName() + " " +
    diskDirsStr;
  }

  /** Appends to a StringBuffer logging that describes a DiskSTore for the 
   *  given Region.
   * @param aRegion The Region possibly containing a DiskStore.
   * @param aStr The StringBuffer to append to.
   */
  public static void logDiskStore(Region aRegion, StringBuffer aStr) {
    if (aRegion.getAttributes().getDiskStoreName() != null) {
      aStr.append(" diskStoreName " + aRegion.getAttributes().getDiskStoreName());
    }
  }

  /** Install a PersistenceObserver to help tests create a "tie" on disk
   *  recovery.
   */
  public static void installPersistenceObserver() {
    PersistenceObserver observer = RecoveryPrms.getPersistenceObserver();
    if (observer == null) {
      Log.getLogWriter().info("No PersistenceObserver was specified");
    } else {
      Log.getLogWriter().info("Installing " + observer.getClass().getName());
      PersistenceObserverHolder.setInstance(observer);
    }
  }

  /** Creates and initializes a server or peer. 
   */
  public synchronized static void HydraTask_initializeExpectException() throws Throwable {
    if (RecoveryTest.testInstance == null) {
      RecoveryTest.testInstance = new RecoveryTest();
      int numRootRegions = RecoveryPrms.getNumRootRegions();
      int numSubregions = RecoveryPrms.getNumSubregions();
      int hierDepth = RecoveryPrms.getRegionHierarchyDepth();
      if ((numSubregions != 0) || (hierDepth != 1)) {
        throw new TestException("Unable to handle subregions in this test");
      }
      CacheHelper.createCache("cache1");
      final String regionConfigName = TestConfig.tasktab().stringAt(RecoveryPrms.regionConfigNames, TestConfig.tab().stringAt(RecoveryPrms.regionConfigNames, null));
      String createdRegions = "";
      for (int i=1; i <= numRootRegions; i++) {
        try {
          final String regionName = "Region_" + i;
          createdRegions = createdRegions + RegionHelper.createRegion(regionName, regionConfigName).getFullPath() + " ";
        } catch (ConflictingPersistentDataException e) {
          Log.getLogWriter().info("Caught expected exception " + TestHelper.getStackTrace(e));
        }
      }
      if (createdRegions.length() > 0) {
         throw new TestException("Expected to get " + ConflictingPersistentDataException.class.getName() +
          " on region creation, but the following regions were successfully created: " + createdRegions);
      }
    }
  }

  /** Verify that the system is waiting for member(s) to come up before recovering.
   *  @param expectedWaitingForMember This specifies the vmID of the member the system
   *         should be waiting for.
   *  @param membersWaiting This is a List of the members that should be waiting.
   */
  protected static void verifyWaiting(ClientVmInfo expectedWaitingForMember, List<ClientVmInfo> membersWaiting) {
    AdminDistributedSystem adminDS = AdminHelper.getAdminDistributedSystem();
    boolean conditionPreviouslyMet = false;
    do {
      try {
        Set<PersistentID> waitingForMembers = adminDS.getMissingPersistentMembers();
        Log.getLogWriter().info("System is waiting for " + waitingForMembers.size() + " member(s); " +
            waitingForMembers + ", test is expecting the system to be waiting for member with vmID " +
            expectedWaitingForMember.getVmid());
        Set vmsWaitingFor = new TreeSet();
        for (PersistentID id: waitingForMembers) {
          String diskDirWaitingFor = id.getDirectory();
          String searchStr = "vm_";
          int index = diskDirWaitingFor.indexOf(searchStr) + searchStr.length();
          int index2 = diskDirWaitingFor.indexOf("_", index);
          String vmIdStr = diskDirWaitingFor.substring(index, index2);
          vmsWaitingFor.add(vmIdStr);
        }

        if (vmsWaitingFor.size() == 1) { // waiting for correct number of members
          String singleVmWaitingFor = (String)vmsWaitingFor.iterator().next();
          if (singleVmWaitingFor.equals(expectedWaitingForMember.getVmid().toString())) { // waiting for 1 member only; it is the correct member
            // now we found what we were waiting for, but to make sure the product sticks with this
            // over a period of time; we will sleep and check again
            Log.getLogWriter().info("System is waiting for expected member " + waitingForMembers);
            if (conditionPreviouslyMet) { // this is the second time the condition has been met
              verifyMissingDiskStoresCommandLineTool(waitingForMembers);
              Log.getLogWriter().info("Verified that the system is waiting on vm ID " + expectedWaitingForMember.getVmid() +
                  "; system is waiting for vm with ID " + singleVmWaitingFor);
              return;
            } else { // this if the first time the correct condition has been met
              conditionPreviouslyMet = true;
              Log.getLogWriter().info("Sleeping for 180 seconds to make sure the member the system is waiting for does not change");
              MasterController.sleepForMs(180000);
            }
          } else {
            Log.getLogWriter().info("System is not waiting for the expected member " + expectedWaitingForMember.getVmid());
            conditionPreviouslyMet = false;
          }
        } else {
          Log.getLogWriter().info("System is not waiting for the expected member " + expectedWaitingForMember.getVmid());
          conditionPreviouslyMet = false;
        }
      } catch (AdminException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      }

      MasterController.sleepForMs(2000);
    } while (true);
  }

  /**  Verify that the command line tool for listing missing disk stores contains
   *   the PersistentIDs in the given set.
   *   
   * @param expected The members that are expected to be in the results of
   *        the list-missing-disk-stores command line tool. 
   */
  private static void verifyMissingDiskStoresCommandLineTool(Set<PersistentID> expected) {
    // Workaround to bug 42432; when this bug is fixed, remove the following if
    if (HostHelper.isWindows() && !CliHelperPrms.getUseCli()) {
      Log.getLogWriter().info("To avoid bug 42432, not running list-missing-disk-stores command line tool");
      return;
    }
    String result = PersistenceUtil.runListMissingDiskStores();
    
    // convert result in to List of diskDirs
    List<String> actualDiskIds = new ArrayList();
    String[] tokens = result.split("\n");
    if (CliHelperPrms.getUseCli()) { // result from gfsh is different than the old gemfire script output
      for (String line: tokens) {
        if (line.indexOf("vm_") >= 0) {
          String[] lineTokens = line.split("[\\s]+"); // split on white space
          actualDiskIds.add(lineTokens[0]); // diskId is first token
        }
      }
    } else {
      for (String line: tokens) { // make resultLines contain only DiskStores
        Matcher matcher = PersistenceUtil.DISK_STORE_ID.matcher(line);
        if (matcher.matches()) {
          actualDiskIds.add(matcher.group(1));
        }
      }
    }
    
    // convert expected into List of diskDirs
    List<String> expectedDiskDirs = new ArrayList();
    for (PersistentID id: expected) {
      expectedDiskDirs.add(id.getUUID().toString());
    }
    
    // find disk dirs missing or extra in command line tool results
    List<String> missing = new ArrayList(expectedDiskDirs);
    List<String> extra = new ArrayList(actualDiskIds);
    missing.removeAll(actualDiskIds);
    extra.removeAll(expectedDiskDirs);
    if (missing.size() > 0) {
      throw new TestException("The following DiskStore IDs were expected in the result of" +
          " list or show missing-disk-stores command line tool: " + missing +
          ", result of command line tool is " + result);
    }
    if (extra.size() > 0) {
      throw new TestException("The following unexpected DiskStore IDs were returned" +
          " from list or show missing-disk-stores command line tool: " + extra + 
          ", expected " + expected);
    }
    Log.getLogWriter().info("list or show missing-disk-stores command returned expected results");
  }

  /** Forces recovery by revoking the members the system is waiting for.
   *  It is the responsibility of the caller to know that the correct members
   *  are being waited for since this will revoke all waiting members.
   *  
   *  @param doValidation If true, then validate the missing disk stores (used
   *         for serial tests or tests that are in a silent phase), if false
   *         then don't do validation (used for concurrent tests).
   */
  public static void forceRecovery(boolean doValidation) {
    AdminDistributedSystem adminDS = AdminHelper.getAdminDistributedSystem();
    Set<PersistentID> waitingForMembers;
    try {
      waitingForMembers = adminDS.getMissingPersistentMembers();
      if (doValidation) {
        verifyMissingDiskStoresCommandLineTool(waitingForMembers);
      }
      for (PersistentID persistId: waitingForMembers) {
        boolean revokeWithCommandLineTool = TestConfig.tab().getRandGen().nextBoolean();
        // workaround for bug 42432; when this bug is fixed remove the following if
        if (HostHelper.isWindows()) {
          revokeWithCommandLineTool = false; // force usage of API
        }
        if (CliHelperPrms.getUseCli()) {
          revokeWithCommandLineTool = true;
        }
        if (revokeWithCommandLineTool) { // revoke with command line tool
          PersistenceUtil.runRevokeMissingDiskStore(persistId);
        } else {
          Log.getLogWriter().info("Revoking PersistentID " + persistId);
          adminDS.revokePersistentMember(persistId.getUUID());
        } 
      }
    } catch (AdminException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
  }

  /** Return whether the given region has persistence
   * 
   * @param aRegion The Region to check for persistence.
   * @return True if the region has persistence, false otherwise. 
   */
  public static boolean withPersistence(Region aRegion) {
    // only GemFire 6.5 (and beyond) has a method for DataPolicy.withPersistence()
    return aRegion.getAttributes().getDataPolicy().withPersistence();
  }

  /** Remove disk files in disk dirs corresponding to this vm's vmID.
   * 
   */
  public static synchronized void removeDiskFiles() {
    String prefix = "vm_" + RemoteTestModule.getMyVmid() + "_";
    String currDirName = System.getProperty("user.dir");
    File currDir = new File(currDirName);
    File[] listing = currDir.listFiles();
    for (File aFile: listing) {
      String fileName = aFile.getName();
      if (fileName.startsWith(prefix) &&
          (fileName.contains("disk_")) &&
          aFile.isDirectory()) { // found a diskDir for this vm's vmID
        File[] diskFiles = aFile.listFiles();
        if (diskFiles == null) {
          continue;
        }
        for (File diskFile : diskFiles) {
          Log.getLogWriter().info("Working to delete file:" + diskFile.getAbsolutePath());

          if (diskFile.exists()) {
            boolean result = diskFile.delete();
            Log.getLogWriter().info("File delete result: " + result);
            if (!result) {
              Log.getLogWriter().info("After 'failed' delete, Does file still exist (delete)? " + diskFile.exists());
              //throw new TestException("Unable to delete file " + diskFile.getAbsolutePath());
            }
            Log.getLogWriter().info("Removed " + diskFile.getAbsolutePath());
          }
        }
      }
    }
  }

  /** Task to do random operations that expect exceptions from shutDownAllMembers
   * 
   * @param testInstance The RecoveryTest instance.
   */
  public static void doOperationsShutDownAll(RecoveryTest testInstance) throws Exception {
    long minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec);
    long minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
    try {
      testInstance.doOperations(minTaskGranularityMS);
    } catch (CacheClosedException e) {
      boolean shutDownAllInProgress = (Boolean)RecoveryBB.getBB().getSharedMap().get(RecoveryBB.shutDownAllKey);
      if (shutDownAllInProgress) {
        Log.getLogWriter().info("Caught expected exception " + e + "; continuing test");
      } else {
        throw new TestException(TestHelper.getStackTrace(e));
      }
    } catch (DistributedSystemDisconnectedException e) {
      boolean shutDownAllInProgress = (Boolean)RecoveryBB.getBB().getSharedMap().get(RecoveryBB.shutDownAllKey);
      if (shutDownAllInProgress) {
        Log.getLogWriter().info("Caught expected exception " + e + "; continuing test");
      } else {
        throw new TestException(TestHelper.getStackTrace(e));
      }
    }
  }

  /** Task to do random operations that expect exceptions from shutDownAllMembers
   * 
   * @param testInstance The RecoveryTest instance.
   */
  public static void doOperationsHA(RecoveryTest testInstance) throws Exception {
    long minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec);
    long minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
    try {
      testInstance.doOperations(minTaskGranularityMS);
    } catch (CacheClosedException e) {
      boolean thisVMReceivedNiceKill = StopStartVMs.niceKillInProgress();
      if (thisVMReceivedNiceKill) {
        Log.getLogWriter().info("Caught expected exception " + e + "; continuing test");
      } else {
        throw new TestException(TestHelper.getStackTrace(e));
      }
    } catch (DistributedSystemDisconnectedException e) {
      boolean thisVMReceivedNiceKill = StopStartVMs.niceKillInProgress();
      if (thisVMReceivedNiceKill) {
        Log.getLogWriter().info("Caught expected exception " + e + "; continuing test");
      } else {
        throw new TestException(TestHelper.getStackTrace(e));
      }
    } catch (RegionDestroyedException e) {
      boolean thisVMReceivedNiceKill = StopStartVMs.niceKillInProgress();
      if (thisVMReceivedNiceKill) {
        Log.getLogWriter().info("Caught expected exception " + e + "; continuing test");
      } else {
        throw new TestException(TestHelper.getStackTrace(e));
      }      
    }
  }

  /** During a shutDownAll we are disconnected from the DS and exceptions can occur.
   *  This method is called when we know we are undergoing a shutdown all and allows
   *  certain exceptions
   * @param e The exception that occurred during shutDownAll
   */
  public static void handleOpsExceptionDuringShutDownAll(Exception e) {
    if ((e instanceof CacheClosedException) ||
        (e instanceof DistributedSystemDisconnectedException)){
      Log.getLogWriter().info("Caught expected " + e + " during shutDownAllMembers; continuing test");
    } else {
      throw new TestException(TestHelper.getStackTrace(e));
    }
  }

  /** Install the PRObserver
   * 
   */
  public static void installPRObserver() {
    PRObserver.installObserverHook();
  }
  
  /** Initialize the PRObserver
   * 
   */
  public static void initPRObserver() {
    PRObserver.initialize();
  }

  /** Set a random DiskStore for the region defined with regionConfigName if
   *  it is persistent or has eviction with overflow to disk. 
   * @param regionConfigName A hydra region config name. 
   * return factory The attributes factory for the regionConfigName with possibly
   *                a DiskStore.
   */
  public static AttributesFactory setDiskStoreIfNeeded(String regionConfigName) {
    AttributesFactory factory = RegionHelper.getAttributesFactory(regionConfigName);
    RegionDescription desc = RegionHelper.getRegionDescription(regionConfigName);
    EvictionAttributes evAttr = desc.getEvictionAttributes();
    if ((desc.getDataPolicy().withPersistence()) ||
        ((evAttr != null) && (evAttr.getAction().isOverflowToDisk()))) {
       List diskStoreNames = TestConfig.tab().vecAt(DiskStorePrms.names);
       factory.setDiskStoreName((String)(diskStoreNames.get(TestConfig.tab().getRandGen().nextInt(1, diskStoreNames.size()-1))));
    }
    return factory;
  }
  
  public static void handleOpsException(Exception e) {
    if ((e instanceof CacheClosedException) || (e instanceof DistributedSystemDisconnectedException)) {
      boolean thisVMReceivedNiceKill = StopStartVMs.niceKillInProgress();
      if (thisVMReceivedNiceKill) {
        Log.getLogWriter().info("Caught expected exception " + e + "; continuing test");
      } else {
        throw new TestException(TestHelper.getStackTrace(e));
      }
    } else {
      throw new TestException(TestHelper.getStackTrace(e));
    }
  }
  
  /** Create an xml file for the current cache and all its regions.
   * 
   */
  public static synchronized void createXmlFile() {
    String bbKey = null;
    String clientName = RemoteTestModule.getMyClientName();
    String fileName = RecoveryTest.getXmlFileName(RemoteTestModule.getMyVmid());
    File aFile = new File(fileName);
    if (aFile.exists()) {
       return;
    }
    if (clientName.indexOf("oldVersion") >= 0) {
      bbKey = RecoveryTest.oldVersionXmlKey;
    } else if (clientName.indexOf("newVersion") >= 0) {
      bbKey = RecoveryTest.newVersionXmlKey;
    } 
    DeclarativeGenerator.createDeclarativeXml(fileName, RecoveryTest.theCache, false, true);
    if (bbKey != null) {
      File xmlFile = new File(fileName);
      String xmlFilePath;
      try {
        xmlFilePath = xmlFile.getCanonicalPath();
      } catch (IOException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      }
      RecoveryBB.getBB().getSharedMap().put(bbKey, xmlFilePath);
    }
  }

  /**
   * 
   */
  public static void initPdxClassLoader() {
    PdxTest.initClassLoader();
  }

  /**
   * @param valueClassName
   * @param key
   * @param randomValues
   * @return
   */
  public static BaseValueHolder getVersionedValueHolder(String valueClassName,
      String key, RandomValues randomValues) {
    return PdxTest.getVersionedValueHolder(valueClassName, key, randomValues);
  }

  /**
   * @param value
   * @return
   */
  public static boolean instanceOfPdxInstance(Object value) {
    return value instanceof PdxInstance;
  }

  /**
   * @param value
   * @return
   */
  public static BaseValueHolder toValueHolder(Object value) {
    return PdxTest.toValueHolder(value);
  }

  //---------------------------------------------------------------------------
  // Snapshot support for 7.0
  //---------------------------------------------------------------------------
  /** Randomly select one of the SnapshotFilters (Key, Value or ValueType)
   *  Returns instance of SnapshotFilter.
   */
  private static SnapshotFilter getSnapshotFilter() {
    int i = TestConfig.tab().getRandGen().nextInt(0, 2);
    SnapshotFilter filter = new KeySnapshotFilter();
    if (i == 2) {
      filter = new ValueSnapshotFilter();
    } else if (i == 3) {
      filter = new ValueTypeSnapshotFilter();
    }
    Log.getLogWriter().info("getSnapshotFilter returning instance of " + filter.getClass().getName());
    return filter;
  }

  /**
   * Key SnapshotFilter only accepts original entries (created before snapshot written)
   * based on the key (FilterObject_XXX vs. Object_XXX)
   */
  public static class KeySnapshotFilter implements SnapshotFilter {
    public boolean accept(Map.Entry entry) {
      boolean accepted = false;
      String key = (String)entry.getKey();
      if (key.startsWith(NameFactory.OBJECT_NAME_PREFIX)) {
        accepted = true;
      }
      return accepted;
    }
  }

  /**
   * Value SnapshotFilter only accepts original entries (created before snapshot written)
   * based on the value (value should NOT start with this string "object to be filtered").
   */
  public static class ValueSnapshotFilter implements SnapshotFilter {
    public boolean accept(Map.Entry entry) {
      boolean accepted = true;
      Object value = entry.getValue();

      if ((value instanceof String) && (((String)value).startsWith("object to be filtered"))) {
        accepted = false;
      }
      return accepted;
    }
  }

  /**
   * Value Type SnapshotFilter only accepts original entries (created before snapshot written)
   * based on the value type (ValueHolders are accepted, String values are not).
   */
  public static class ValueTypeSnapshotFilter implements SnapshotFilter {
    public boolean accept(Map.Entry entry) {
      boolean accepted = false;
      Object value = entry.getValue();

      if (value instanceof ValueHolder) {
        accepted = true;
      }
      return accepted;
    }
  }

  /** Do operations and pause when directed by the snapshotController
   *  Also, writes the snapshot to the blackboard and exports snapshot to disk
   */
  protected static void snapshotResponder(RecoveryTest testInstance) throws Exception {
    long pausing = RecoveryBB.getBB().getSharedCounters().read(RecoveryBB.pausing);
    if (pausing > 0) { // controller has signaled to pause
      if ((Boolean)(testInstance.threadIsPaused.get())) { // this thread has already paused, so don't increment counter
        Log.getLogWriter().info("Thread has paused");
      } else {
        Log.getLogWriter().info("This thread is pausing");
        testInstance.threadIsPaused.set(new Boolean(true));
        RecoveryBB.getBB().getSharedCounters().incrementAndRead(RecoveryBB.pausing);
      }
      long writeSnapshot = RecoveryBB.getBB().getSharedCounters().read(RecoveryBB.writeSnapshot);
      if (writeSnapshot > 0) {
        long leader = RecoveryBB.getBB().getSharedCounters().incrementAndRead(RecoveryBB.leader);
        if (leader == 1) { // this is the thread to write the snapshot
          Log.getLogWriter().info("This thread is the leader; it will write the snapshot");
          testInstance.writeSnapshot(true);  // include non-persistent regions
          long executionNumber = RecoveryBB.getBB().getSharedCounters().read(RecoveryBB.executionNumber);

          // Add numFilterObjects entries to each region (SnapshotFilter tests)
          if (SnapshotPrms.useFilterOnExport() || SnapshotPrms.useFilterOnImport()) {
            Map allRegionsSnapshot = new HashMap();
            int numToCreate = SnapshotPrms.numFilterObjects();
            for (Region aRegion: testInstance.allRegions) {
              for (int i=1; i <=numToCreate; i++) {
                String key = "FilterObject_" + i;
                String value = "object to be filtered via snapshot.save() or snapshot.load(): this should never be a value in the cache once snapshot restored";
                aRegion.put(key, value);
              }
            }
            Log.getLogWriter().info("Wrote " + numToCreate + " FilterObject entries to each region");
          }

          CacheSnapshotService snapshot = CacheHelper.getCache().getSnapshotService();
          SnapshotOptions options = snapshot.createOptions();
          if (SnapshotPrms.useFilterOnExport()) {
            options.setFilter(getSnapshotFilter());
          }

          String currDirName = System.getProperty("user.dir");
          String snapshotDirName = currDirName + File.separator + "cacheSnapshotDir_" + executionNumber;
          Log.getLogWriter().info("Starting cacheSnapshot to " + snapshotDirName);
          snapshot.save(new File(snapshotDirName), SnapshotFormat.GEMFIRE, options);
          Log.getLogWriter().info("Completed cacheSnapshot to " + snapshotDirName);
          RecoveryBB.getBB().getSharedCounters().increment(RecoveryBB.snapshotWritten);
        }
      }
      MasterController.sleepForMs(5000);
    } else { // not pausing
      long minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec);
      long minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
      testInstance.doOperations(minTaskGranularityMS);
    }
  }

  /**
   *  One thread imports ... everyone validates against blackboard snapshot
   */
  protected static void importAndVerify(RecoveryTest testInstance) {
    long counter = RecoveryBB.getBB().getSharedCounters().incrementAndRead(RecoveryBB.importSnapshot);
    if (counter == 1) {
      Log.getLogWriter().info("This thread is the leader; it will import the snapshot");
      long executionNumber = RecoveryBB.getBB().getSharedCounters().read(RecoveryBB.executionNumber);
      String currDirName = System.getProperty("user.dir");
      String snapshotDirName = currDirName + File.separator + "cacheSnapshotDir_" + executionNumber;
      CacheSnapshotService snapshot = CacheHelper.getCache().getSnapshotService();
      long startTime = System.currentTimeMillis();
      try {
        if (SnapshotPrms.useFilterOnImport()) {
          SnapshotOptions options = snapshot.createOptions();
          options.setFilter(getSnapshotFilter());

          File dir = new File(snapshotDirName);
          File[] files = dir.listFiles();
          StringBuffer aStr = new StringBuffer();
          aStr.append("Invoking snapshot.load() with files:\n");
          for (int i=0; i < files.length; i++) {
            aStr.append("   " + files[i] + "\n");
          }
          aStr.append("Options.getFilter() = " + options.getFilter().getClass().getName());
          Log.getLogWriter().info(aStr.toString());
          snapshot.load(files, SnapshotFormat.GEMFIRE, options);
        } else {
          Log.getLogWriter().info("Starting CacheSnapshotService.load() from " + snapshotDirName);
          snapshot.load(new File(snapshotDirName), SnapshotFormat.GEMFIRE);
        }
      } catch (IOException ioe) {
        throw new TestException("Caught " + ioe + " while importing region snapshot from " + snapshotDirName + " " + TestHelper.getStackTrace(ioe));
      } catch (ClassNotFoundException e) {
        throw new TestException("Caught " + e + " while importing region snapshot from " + snapshotDirName + " " + TestHelper.getStackTrace(e));
      }
      long endTime = System.currentTimeMillis();
      Log.getLogWriter().info("CacheSnapshotService.load() of " + snapshotDirName + " took " + (endTime - startTime) + " ms.");
      Log.getLogWriter().info(testInstance.regionHierarchyToString());
      RecoveryBB.getBB().getSharedCounters().increment(RecoveryBB.snapshotImported);
    } else {
      TestHelper.waitForCounter(RecoveryBB.getBB(), "RecoveryBB.snapshotImported",
          RecoveryBB.snapshotImported, 1, true, -1, 2000);
      testInstance.verifyFromSnapshot();
      RecoveryBB.getBB().getSharedCounters().increment(RecoveryBB.doneVerifyingCounter);
    }
    TestHelper.waitForCounter(RecoveryBB.getBB(), "RecoveryBB.doneVerifyingCounter",
        RecoveryBB.doneVerifyingCounter,  RemoteTestModule.getCurrentThread().getCurrentTask().getTotalThreads()-1,
        true, -1, 2000);
  }

  /**
   * Performs a backup.
   */
  protected static void performBackup() {
    long execution = RecoveryBB.getBB().getSharedCounters().read(RecoveryBB.executionNumber);

    File backupDir = createBackupDir(execution);
    File baselineDir = (execution > 1 ? getBackupDir(execution - 1) : null);

    try {
      BackupStatus status = AdminHelper.getAdminDistributedSystem().backupAllMembers(backupDir,baselineDir);
    } catch (AdminException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
  }

  /**
   * Locates a previously created backup directory associated with an online backup counter.
   * @param counter a backup number
   * @throws TestException a backup directory could not be located or was malformed (not a directory or contains no children or too many children).
   */
  private static File getBackupDir(long counter) {
    File backupDir = new File("backup_" + counter);

    if(!backupDir.exists()) {
      throw new TestException("getBackupDir: Backup directory " + backupDir.getName() + " does not exist.");
    }

    File[] files = backupDir.listFiles();

    if((null == files) || (files.length != 1)) {
      throw new TestException("getBackupDir: Backup directory " + backupDir.getName() + " is malformed or is not a directory");
    }

    return files[0];
  }

  /**
   * Creates a backup directory or returns an already created directory for a backup number.
   * @param backup a backup number.
   */
  private static File createBackupDir(long backup) {
    File file = new File("backup_" + backup);
    if(!file.exists()) {
      file.mkdirs();
    }

    return file;
  }
}
