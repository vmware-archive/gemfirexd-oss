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
package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.SocketCreator;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;
import com.gemstone.gemfire.internal.shared.Version;

import dunit.AsyncInvocation;
import dunit.Host;
import dunit.SerializableCallable;
import dunit.VM;
import hydra.*;

import java.io.File;
import java.rmi.RemoteException;

/**
 * @author xzhou
 *
 */
public class UpgradeDiskStoreDUnitTest extends CacheTestCase {

  final int mcastPort = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
  final String member1_dir = "member1_dir";
  final String member2_dir = "member2_dir";
  final String member1_dir4test = "member1_dir4test";
  final String member2_dir4test = "member2_dir4test";
  int pid1;
  int pid2;

//  protected Object forWaitNotify = new Object();
//  protected boolean gotNotification = false;

  /**
   * @param name
   */
  public UpgradeDiskStoreDUnitTest(String name) {
    super(name);
  }
  
  public void tearDown2() throws Exception {
    super.tearDown2();
    FileUtil.rmdir(member1_dir4test, false);
    FileUtil.rmdir(member2_dir4test, false);
    removePID(pid1);
    removePID(pid2);
  }
  
  public void testPRCases() throws Throwable {
    prepPR();
    doPRTest();
    retryPRTest();
  }

  public void prepPR() throws Throwable {
    createTestData("662", true);
  }
  
  public void doPRTest() throws Throwable {
    disconnectAllFromDS();
    com.gemstone.gemfire.internal.FileUtil.copy(new File(member1_dir), new File(member1_dir4test));
    com.gemstone.gemfire.internal.FileUtil.copy(new File(member2_dir), new File(member2_dir4test));
    do_normal(true, true, false);
  }
  
  public void retryPRTest() throws Throwable {
    disconnectAllFromDS();
    com.gemstone.gemfire.internal.FileUtil.copy(new File(member1_dir), new File(member1_dir4test));
    com.gemstone.gemfire.internal.FileUtil.copy(new File(member2_dir), new File(member2_dir4test));
    do_negative(true, true);
  }
  
  public void testDRCases() throws Throwable {
    prepDR();
    doDRTest();
    retryDRTest();
    doDR_RecoveryOnly();
  }
  
  public void prepDR() throws Throwable {
    createTestData("662", false);
  }
  
  public void doDRTest() throws Throwable {
    disconnectAllFromDS();
    com.gemstone.gemfire.internal.FileUtil.copy(new File(member1_dir), new File(member1_dir4test));
    com.gemstone.gemfire.internal.FileUtil.copy(new File(member2_dir), new File(member2_dir4test));
    do_normal(false, true, false);
  }
  
  public void retryDRTest() throws Throwable {
    disconnectAllFromDS();
    com.gemstone.gemfire.internal.FileUtil.copy(new File(member1_dir), new File(member1_dir4test));
    com.gemstone.gemfire.internal.FileUtil.copy(new File(member2_dir), new File(member2_dir4test));
    do_negative(false, true);
  }
  
  public void doDR_RecoveryOnly() throws Throwable {
    disconnectAllFromDS();
    com.gemstone.gemfire.internal.FileUtil.copy(new File(member1_dir), new File(member1_dir4test));
    com.gemstone.gemfire.internal.FileUtil.copy(new File(member2_dir), new File(member2_dir4test));

    // do validate before convert, it should failed
    try {
      DiskStoreImpl.validate(RunCacheInOldGemfire.diskStoreName1, getDiskDirsForMember(member1_dir4test));
      fail("Exception was not triggered");
    } catch (RuntimeException rte) {
      assertTrue(rte.getMessage().contains("This disk store is still at version GFE pre-7.0"));
    }
    try {
      DiskStoreImpl.validate(RunCacheInOldGemfire.diskStoreName2, getDiskDirsForMember(member1_dir4test));
      fail("Exception was not triggered");
    } catch (RuntimeException rte) {
      assertTrue(rte.getMessage().contains("This disk store is still at version GFE pre-7.0"));
    }

    do_normal(false, true, true);
  }
  
  public void testDR651() throws Throwable {
    prepDR651();
    doTestDR651();
  }

  public void prepDR651() throws Throwable {
    createTestData("651", false);
  }
  
  public void doTestDR651() throws Throwable {
    disconnectAllFromDS();
    com.gemstone.gemfire.internal.FileUtil.copy(new File(member1_dir), new File(member1_dir4test));
    com.gemstone.gemfire.internal.FileUtil.copy(new File(member2_dir), new File(member2_dir4test));
    do_normal(false, true, false);
  }

  protected void do_normal(boolean isPR, boolean upgradeVersionOnly, boolean doRecoveryOnly) throws Throwable {
    if (!doRecoveryOnly) {
      // convert member1_dir
      printDirectory(member1_dir4test, "before do_normal isPR="+isPR);
      printDirectory(member2_dir4test, "before do_normal isPR="+isPR);

      DiskStoreImpl ds1_member1 = DiskStoreImpl.offlineCompact(RunCacheInOldGemfire.diskStoreName1, 
          getDiskDirsForMember(member1_dir4test), upgradeVersionOnly, RunCacheInOldGemfire.maxOplogSize);
      DiskStoreImpl ds2_member1 = DiskStoreImpl.offlineCompact(RunCacheInOldGemfire.diskStoreName2, 
          getDiskDirsForMember(member1_dir4test), upgradeVersionOnly, RunCacheInOldGemfire.maxOplogSize);
      {
        File drfFile1 = new File(member1_dir4test, "BACKUP"+RunCacheInOldGemfire.diskStoreName1+"_2.drf");
        File drfFile2 = new File(member1_dir4test, "BACKUP"+RunCacheInOldGemfire.diskStoreName2+"_2.drf");
        assertTrue(!drfFile1.exists());
        assertTrue(!drfFile2.exists());
      }

      assertEquals(Version.CURRENT, ds1_member1.getRecoveredGFVersion());
      assertEquals(Version.CURRENT, ds2_member1.getRecoveredGFVersion());
      DiskStoreImpl.validate(RunCacheInOldGemfire.diskStoreName1, getDiskDirsForMember(member1_dir4test));
      DiskStoreImpl.validate(RunCacheInOldGemfire.diskStoreName2, getDiskDirsForMember(member1_dir4test));

      // convert member2_dir
      DiskStoreImpl ds1_member2 = DiskStoreImpl.offlineCompact(RunCacheInOldGemfire.diskStoreName1, 
          getDiskDirsForMember(member2_dir4test), upgradeVersionOnly, RunCacheInOldGemfire.maxOplogSize);
      DiskStoreImpl ds2_member2 = DiskStoreImpl.offlineCompact(RunCacheInOldGemfire.diskStoreName2, 
          getDiskDirsForMember(member2_dir4test), upgradeVersionOnly, RunCacheInOldGemfire.maxOplogSize);
      {
        File drfFile1 = new File(member2_dir4test, "BACKUP"+RunCacheInOldGemfire.diskStoreName1+"_2.drf");
        File drfFile2 = new File(member2_dir4test, "BACKUP"+RunCacheInOldGemfire.diskStoreName2+"_2.drf");
        assertTrue(!drfFile1.exists());
        assertTrue(!drfFile2.exists());
      }

      assertEquals(Version.CURRENT, ds1_member2.getRecoveredGFVersion());
      assertEquals(Version.CURRENT, ds2_member2.getRecoveredGFVersion());
      DiskStoreImpl.validate(RunCacheInOldGemfire.diskStoreName1, getDiskDirsForMember(member2_dir4test));
      DiskStoreImpl.validate(RunCacheInOldGemfire.diskStoreName2, getDiskDirsForMember(member2_dir4test));
    } // convertion
    
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    // restart member1
    AsyncInvocation inst0 = createPeerCache(vm0, mcastPort, RunCacheInOldGemfire.regionName1, 
        isPR, member1_dir4test, RunCacheInOldGemfire.diskStoreName1);
    AsyncInvocation inst1 = createPeerCache(vm0, mcastPort, RunCacheInOldGemfire.regionName2, 
        isPR, member1_dir4test, RunCacheInOldGemfire.diskStoreName2);

    // restart member2
    AsyncInvocation inst2 = createPeerCache(vm1, mcastPort, RunCacheInOldGemfire.regionName1, 
        isPR, member2_dir4test, RunCacheInOldGemfire.diskStoreName1);
    AsyncInvocation inst3 = createPeerCache(vm1, mcastPort, RunCacheInOldGemfire.regionName2, 
        isPR, member2_dir4test, RunCacheInOldGemfire.diskStoreName2);
    
    inst0.join(30000);
    inst1.join(30000);
    inst2.join(30000);
    inst3.join(30000);
    if (doRecoveryOnly) {
      assertTrue(inst0.exceptionOccurred());
      assertTrue(inst1.exceptionOccurred());
      assertTrue(inst2.exceptionOccurred());
      assertTrue(inst3.exceptionOccurred());
      Throwable t = inst0.getException();
      assertTrue(t.getMessage().contains("This disk store is still at version GFE pre-7.0"));
    }
    
    if (inst0.exceptionOccurred() || inst1.exceptionOccurred() 
        || inst2.exceptionOccurred() || inst3.exceptionOccurred()) {
      // if exceptions occurred (maybe expected), then stop here
      return;
    }
    
    // expect GII to happen
    VersionTag tagsFromVm0[] = new VersionTag[4];
    for (int i=0; i<4; i++) {
      tagsFromVm0[i] = getVersionTag(vm0, RunCacheInOldGemfire.regionName1, "key"+(1+i));
    }
    VersionTag tagsFromVm1[] = new VersionTag[4];
    for (int i=0; i<4; i++) {
      tagsFromVm1[i] = getVersionTag(vm1, RunCacheInOldGemfire.regionName1, "key"+(1+i));
    }
    
    int region1size = getRegionSize(vm0, RunCacheInOldGemfire.regionName1);
    int region2size = getRegionSize(vm1, RunCacheInOldGemfire.regionName2);
    assertEquals(RunCacheInOldGemfire.numOfKeys+3, region1size);
    assertEquals(RunCacheInOldGemfire.numOfKeys+3, region2size);
    
    // destroyed entries will not have version after conversion
    String v1 = getValue(vm0, RunCacheInOldGemfire.regionName1, "key1");
    String v2 = getValue(vm0, RunCacheInOldGemfire.regionName1, "key2");
    String v3 = getValue(vm0, RunCacheInOldGemfire.regionName1, "key3");
    assertNull(v1);
    assertNull(v2);
    assertNull(v3);
    assertNull(tagsFromVm0[0]);
    assertNull(tagsFromVm0[1]);
    assertNull(tagsFromVm1[0]);
    assertNull(tagsFromVm1[1]);
    
    // after gii, the version should match
    assertTrue(tagsFromVm1[2].equals(tagsFromVm0[2]));
    assertTrue(tagsFromVm1[3].equals(tagsFromVm0[3]));
    
    // the entry version should be 1
    assertEquals(expectedDummyEntryVersion(), tagsFromVm1[2].getEntryVersion());
    assertEquals(expectedDummyEntryVersion(), tagsFromVm1[3].getEntryVersion());
    String v4 = getValue(vm0, RunCacheInOldGemfire.regionName1, "key4");
    String v5 = getValue(vm0, RunCacheInOldGemfire.regionName1, "key5");
    assertTrue(v4.equals("value4"));
    assertTrue(v5.equals("value6"));
    v4 = getValue(vm1, RunCacheInOldGemfire.regionName1, "key4");
    v5 = getValue(vm1, RunCacheInOldGemfire.regionName1, "key5");
    assertTrue(v4.equals("value4"));
    assertTrue(v5.equals("value6"));
    
    // verify other entries
    tagsFromVm0 = new VersionTag[RunCacheInOldGemfire.numOfKeys];
    for (int i=0; i<RunCacheInOldGemfire.numOfKeys; i++) {
      tagsFromVm0[i] = getVersionTag(vm0, RunCacheInOldGemfire.regionName1, ""+i);
    }
    tagsFromVm1 = new VersionTag[RunCacheInOldGemfire.numOfKeys];
    for (int i=0; i<RunCacheInOldGemfire.numOfKeys; i++) {
      tagsFromVm1[i] = getVersionTag(vm1, RunCacheInOldGemfire.regionName1, ""+i);
      assertTrue(tagsFromVm1[i].equals(tagsFromVm0[i]));
    }


    // negative case1: convert again after conversion
    vm0.invoke(new CacheSerializableRunnable("close cache") {
      public void run2() throws CacheException {
        Cache cache = getCache();
        cache.close();
      }
    });
    vm1.invoke(new CacheSerializableRunnable("close cache") {
      public void run2() throws CacheException {
        Cache cache = getCache();
        cache.close();
      }
    });

    try {
      DiskStoreImpl.offlineCompact(RunCacheInOldGemfire.diskStoreName1, 
          getDiskDirsForMember(member1_dir4test), true, RunCacheInOldGemfire.maxOplogSize);
      fail("Did not throw the expected exception");
    } catch (RuntimeException rte) {
      assertTrue(rte.getMessage().contains("This disk store is already at version "+Version.CURRENT));
    }
    try {
      DiskStoreImpl.offlineCompact(RunCacheInOldGemfire.diskStoreName2, 
          getDiskDirsForMember(member1_dir4test), true, RunCacheInOldGemfire.maxOplogSize);
      fail("Did not throw the expected exception");
    } catch (RuntimeException rte) {
      assertTrue(rte.getMessage().contains("This disk store is already at version "+Version.CURRENT));
    }
    try {
      DiskStoreImpl.offlineCompact(RunCacheInOldGemfire.diskStoreName1, 
          getDiskDirsForMember(member2_dir4test), true, RunCacheInOldGemfire.maxOplogSize);
      fail("Did not throw the expected exception");
    } catch (RuntimeException rte) {
      assertTrue(rte.getMessage().contains("This disk store is already at version "+Version.CURRENT));
    }
    try {
      DiskStoreImpl.offlineCompact(RunCacheInOldGemfire.diskStoreName2, 
          getDiskDirsForMember(member2_dir4test), true, RunCacheInOldGemfire.maxOplogSize);
      fail("Did not throw the expected exception");
    } catch (RuntimeException rte) {
      assertTrue(rte.getMessage().contains("This disk store is already at version "+Version.CURRENT));
    }
  }
  
  protected void do_negative(boolean isPR, boolean upgradeVersionOnly) throws Throwable {
    // convert member1_dir4test
    try {
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
      CacheObserverHolder.setInstance(new CacheObserverAdapter() {
        public void afterSwitchingOplog()
        {
          throw new RuntimeException("Abort convertion after switched oplog");
        }
      });
      
      try {
        DiskStoreImpl.offlineCompact(RunCacheInOldGemfire.diskStoreName1, 
            getDiskDirsForMember(member1_dir4test), upgradeVersionOnly, RunCacheInOldGemfire.maxOplogSize);
        fail("CacheObserver was not triggered");
      } catch (RuntimeException rte) {
        if (!rte.getMessage().contains("Abort convertion after switched oplog")) {
          getLogWriter().info("expected an exception saying 'Abort converted' but received this instead:", rte);
        }
        assertTrue(rte.getMessage().contains("Abort convertion after switched oplog"));
      }
      // drf_2,crf_2,krf_2 becomes 3 & 4
      // TODO: read diskinit file to check gfversion is still 0
      pause(3000);
      try {
        DiskStoreImpl.offlineCompact(RunCacheInOldGemfire.diskStoreName2, 
            getDiskDirsForMember(member1_dir4test), upgradeVersionOnly, RunCacheInOldGemfire.maxOplogSize);
        fail("CacheObserver was not triggered");
      } catch (RuntimeException rte) {
        assertTrue(rte.getMessage().contains("Abort convertion after switched oplog"));
      }
      printDirectory(member1_dir4test, "after do_negative isPR="+isPR);

    } finally {
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
      CacheObserverHolder.setInstance(null);
    }
    
    // redo the diskstores for member1, this time it should work
    do_normal(isPR, upgradeVersionOnly, false);
    // do conversion again on the failed diskstore
  }
  
  public File[] getDiskDirsForMember(String member_dir) {
    File dir = new File(member_dir);
    return new File[] {dir};
  }

  protected void createTestData(final String version, boolean isPR) throws Throwable {
    FileUtil.rmdir(member1_dir, false);
    FileUtil.rmdir(member2_dir, false);

    pid1 = runScript(version, member1_dir, isPR, false, false);
    pid2 = runScript(version, member2_dir, isPR, true, false);
    
    String host = HostHelper.getLocalHost();
    int maxWaitSec = 300;
    if ( ! ProcessMgr.waitForDeath( host, pid1, maxWaitSec ) ) {
      fail("The "+pid1+" is still running");
    }
    if ( ! ProcessMgr.waitForDeath( host, pid2, maxWaitSec ) ) {
      fail("The "+pid2+" is still running");
    }
    
    File crfFile1 = new File(member1_dir, "BACKUP"+RunCacheInOldGemfire.diskStoreName1+"_2.crf");
    File crfFile2 = new File(member1_dir, "BACKUP"+RunCacheInOldGemfire.diskStoreName2+"_2.crf");
    File drfFile1 = new File(member1_dir, "BACKUP"+RunCacheInOldGemfire.diskStoreName1+"_2.drf");
    File drfFile2 = new File(member1_dir, "BACKUP"+RunCacheInOldGemfire.diskStoreName2+"_2.drf");
    assertEquals(true, crfFile1.exists());
    assertEquals(true, crfFile2.exists());
    assertEquals(true, drfFile1.exists());
    assertEquals(true, drfFile2.exists());
  }

  public static String getGemfireLocation(String version) {
    if (version.equals(BasePrms.DEFAULT)) {
      String gemfire = System.getProperty("gemfire.home");
      if (gemfire == null) {
        gemfire = System.getProperty("JTESTS")+"/../../product";
      }
      return gemfire;
    } else {
      String gemfireHome;
      if (version.equals(BasePrms.DEFAULT)) {
        gemfireHome = BasePrms.DEFAULT;
      } else {
        gemfireHome = System.getProperty("RELEASE_DIR",
            "/export/gcm/where/gemfire/releases") + "/GemFire"
            + VersionDescription.dotVersionFor(version) + "-all/product";
      }
      return gemfireHome;
    }
  }

  /**
   * create and run following script:
   * java -cp $GEMFIRE/lib/gemfire.jar:$JTESTS 
   * com.gemstone.gemfire.internal.cache.partitioned.UpgradeDiskStoreDUnitTest version
   * 
   * It will start a cache and do some operations to create a diskstore
   */
  protected int runScript(String version, String diskdir, boolean isPR, 
      boolean doOps, boolean isSDA) throws Throwable {
    String javacmd = System.getProperty( "java.home" ) + File.separator + "bin" + File.separator + "java";
    String jtests = System.getProperty("JTESTS");
    String userDir = System.getProperty("user.dir");
    String oldgemfire = getGemfireLocation(version);
    String classpath = " -cp " + oldgemfire + File.separator + "lib" + File.separator + "gemfire.jar" + File.pathSeparator + jtests;
    String command = javacmd + classpath
        + " com.gemstone.gemfire.internal.cache.RunCacheInOldGemfire "
        + mcastPort + " " + diskdir + " " + isPR + " " + doOps + " " + isSDA
        + " " + SocketCreator.getLocalHost().getHostAddress();
    if (Log.getLogWriter() == null) {
      Log.createLogWriter("script.log", "info");
    }
    Log.getLogWriter().info("Running script: current directory is "+userDir);
    
    int pid = ProcessMgr.bgexec(command);
    Log.getLogWriter().info("Running script: calling "+command+" at pid "+pid);
    
    recordPID(pid);
    return pid;
  }

  protected void recordPID(int pid) throws RemoteException {
    String gemfireName = System.getProperty(GemFirePrms.GEMFIRE_NAME_PROPERTY);
    if ( gemfireName == null ) {
      String s = "No gemfire name has been specified";
      throw new HydraConfigException(s);
    } else {
      HostDescription hd = TestConfig.getInstance().getGemFireDescription(gemfireName).getHostDescription();
      RemoteTestModule.Master.recordPID(hd, pid);
    }
  }
  
  protected void removePID(int pid) throws RemoteException {
    if (pid <= 0) {
      return;
    }
    String gemfireName = System.getProperty(GemFirePrms.GEMFIRE_NAME_PROPERTY);
    if ( gemfireName == null ) {
      String s = "No gemfire name has been specified";
      throw new HydraConfigException(s);
    } else {
      HostDescription hd = TestConfig.getInstance().getGemFireDescription(gemfireName).getHostDescription();
      RemoteTestModule.Master.removePID(hd, pid);
    }
  }
  
  protected AsyncInvocation createPeerCache(VM server, @SuppressWarnings("hiding") final int mcastPort, final String regionName, final boolean createPR, final String member_dir, final String diskStoreName) {
    AsyncInvocation inst = server.invokeAsync(new CacheSerializableRunnable("Create Cache and Region") {
      public void run2() throws CacheException {
        // Create Region
        AttributesFactory factory = new AttributesFactory();
        Cache cache = getCache();
        
        Region region;
        // create diskStore if required
        if (diskStoreName!=null) {
          DiskStore ds = cache.findDiskStore(diskStoreName);
          if(ds == null) {
            ds = cache.createDiskStoreFactory()
            .setDiskDirs(getDiskDirsForMember(member_dir)).create(diskStoreName);
          }
          if (createPR) {
            PartitionAttributesFactory paf = new PartitionAttributesFactory();
            paf.setRedundantCopies(1);
            factory.setPartitionAttributes(paf.create());
            factory.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
          } else {
            factory.setScope(Scope.DISTRIBUTED_ACK);
            factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
          }
          factory.setConcurrencyChecksEnabled(true);
          factory.setDiskStoreName(diskStoreName);
          region = createRootRegion(regionName, factory.create());
        } else {
          RegionFactory rf;
          if (createPR) {
            rf = cache.createRegionFactory(RegionShortcut.PARTITION_REDUNDANT_PERSISTENT);
          } else {
            rf = cache.createRegionFactory(RegionShortcut.REPLICATE_PERSISTENT);
          }
          rf.setConcurrencyChecksEnabled(true);
          region = rf.create(regionName);
        }

        if (createPR) {
          assertTrue(region instanceof PartitionedRegion);
        } else {
          assertTrue(region instanceof DistributedRegion);
        }
      }
    });
    return inst;
  }

  private VersionTag getVersionTag(VM vm, final String regionName, final String key) {
    SerializableCallable getVersionTag = new SerializableCallable("verify version tag for key "+key) {
      public Object call() {
        Cache cache = getCache();
        LocalRegion region = (LocalRegion)cache.getRegion(regionName);

        VersionTag tag = region.getVersionTag(key);
        return tag;
      }
    };
    return (VersionTag)vm.invoke(getVersionTag);
  }
  private String getValue(VM vm, final String regionName, final String key) {
    SerializableCallable getValue = new SerializableCallable("verify value for key "+key) {
      public Object call() {
        Cache cache = getCache();
        LocalRegion region = (LocalRegion)cache.getRegion(regionName);
        String value = (String)region.get(key);
        return value;
      }
    };
    return (String)vm.invoke(getValue);
  }
  private int getRegionSize(VM vm, final String regionName) {
    SerializableCallable getRegionSize = new SerializableCallable("get region size for "+regionName) {
      public Object call() {
        Cache cache = getCache();
        LocalRegion region = (LocalRegion)cache.getRegion(regionName);

        return region.size();
      }
    };
    return (Integer)vm.invoke(getRegionSize);
  }
  
  private static File[] listFiles(File dir) {
    File[] result = dir.listFiles();
    if (result == null) {
      result = new File[0];
    }
    return result;
  }
  public static void printDirectory(String dirname, String testcasename) {
    File[] files = listFiles(new File(dirname));
    System.out.println(testcasename+":list dir:"+dirname);
    for (File f: files) {
      System.out.println("list dir:"+f+":size="+f.length());
    }
  }

  private static int expectedDummyEntryVersion() {
    return Version.SQLF_1099.equals(Version.CURRENT)
        || Version.SQLF_11.equals(Version.CURRENT) ? 0 : 1;
  }
}
