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
package com.gemstone.gemfire.management.internal.cli.commands;

import hydra.FileUtil;
import hydra.GemFirePrms;
import hydra.HostDescription;
import hydra.HostHelper;
import hydra.HydraConfigException;
import hydra.Log;
import hydra.ProcessMgr;
import hydra.RemoteTestModule;
import hydra.TestConfig;

import java.io.File;
import java.rmi.RemoteException;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.SocketCreator;
import com.gemstone.gemfire.internal.cache.DiskStoreImpl;
import com.gemstone.gemfire.internal.cache.DistributedRegion;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.RunCacheInOldGemfire;
import com.gemstone.gemfire.internal.cache.UpgradeDiskStoreDUnitTest;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;
import com.gemstone.gemfire.internal.shared.Version;
import com.gemstone.gemfire.management.cli.Result.Status;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.result.CommandResult;
import com.gemstone.gemfire.management.internal.cli.util.CommandStringBuilder;

import dunit.AsyncInvocation;
import dunit.SerializableCallable;
import dunit.VM;


public class UpgradeOfflineDiskStoreDUnitTest extends CliCommandTestBase {

  private static final long serialVersionUID = 1L;

  public UpgradeOfflineDiskStoreDUnitTest(String name) {
    super(name);
    // TODO Auto-generated constructor stub
  }
  final int mcastPort = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
  final String member1_dir = "mem1_dir";
  final String member2_dir = "mem2_dir";
  final String member1_dir4test = "mem1_dir4test";
  final String member2_dir4test = "mem2_dir4test";
  int pid1;
  int pid2;

  public void tearDown2() throws Exception {
    super.tearDown2();
    FileUtil.rmdir(member1_dir4test, false);
    FileUtil.rmdir(member2_dir4test, false);
    removePID(pid1);
    removePID(pid2);
  }
  
  
  public void testCommand() throws Throwable {
    disconnectAllFromDS();
    CommandResult cmdResult = executeUpgradeDiskStore("dd", "dddd", "1");
    assertTrue(Status.ERROR.equals(cmdResult.getStatus()));
  }

  public void testPR() throws Throwable {
    disconnectAllFromDS();
    createTestData("662", true);
    com.gemstone.gemfire.internal.FileUtil.copy(new File(member1_dir), new File(member1_dir4test));
    com.gemstone.gemfire.internal.FileUtil.copy(new File(member2_dir), new File(member2_dir4test));
    do_normal(true, true, false);
  }

  
  public void testDR() throws Throwable {
    disconnectAllFromDS();
    createTestData("662", false);
    com.gemstone.gemfire.internal.FileUtil.copy(new File(member1_dir), new File(member1_dir4test));
    com.gemstone.gemfire.internal.FileUtil.copy(new File(member2_dir), new File(member2_dir4test));
    do_normal(false, true, false);
  }

  public void testDR651() throws Throwable {
    disconnectAllFromDS();
    createTestData("651", false);
    com.gemstone.gemfire.internal.FileUtil.copy(new File(member1_dir), new File(member1_dir4test));
    com.gemstone.gemfire.internal.FileUtil.copy(new File(member2_dir), new File(member2_dir4test));
    do_normal(false, true, false);
  }
  
  protected void do_normal(boolean isPR, boolean upgradeVersionOnly, boolean doRecoveryOnly) throws Throwable {
    
    if (!doRecoveryOnly) {
   // convert member1_dir
      printDirectory(member1_dir4test, "before do_normal isPR="+isPR);
      printDirectory(member2_dir4test, "before do_normal isPR="+isPR);


      //Upgrade the diskstores for member1
      CommandResult cmdResult = executeUpgradeDiskStore(RunCacheInOldGemfire.diskStoreName1, member1_dir4test, "1");
      assertTrue(Status.OK.equals(cmdResult.getStatus()));
      cmdResult = executeUpgradeDiskStore(RunCacheInOldGemfire.diskStoreName2, member1_dir4test, "1");
      assertTrue(Status.OK.equals(cmdResult.getStatus()));

      //The following calls should result in RuntimeException, hence validating that the gfsh command to upgrade the disk-stores successfully upgrade the 
      // specified offline disk-store
      
      try {
        DiskStoreImpl ds1_member2 = DiskStoreImpl.offlineCompact(RunCacheInOldGemfire.diskStoreName1, 
            getDiskDirsForMember(member1_dir4test), true, RunCacheInOldGemfire.maxOplogSize);
        fail("Did not throw the expected exception");
      } catch (RuntimeException rte) {
        assertTrue(rte.getMessage().contains("This disk store is already at version "+Version.CURRENT));
      } 
      
      try {
        DiskStoreImpl ds2_member2 = DiskStoreImpl.offlineCompact(RunCacheInOldGemfire.diskStoreName2, 
            getDiskDirsForMember(member1_dir4test), true, RunCacheInOldGemfire.maxOplogSize);
      } catch (RuntimeException rte) {
        assertTrue(rte.getMessage().contains("This disk store is already at version "+Version.CURRENT));
      }
      
      {
        File drfFile1 = new File(member1_dir4test, "BACKUP"+RunCacheInOldGemfire.diskStoreName1+"_2.drf");
        File drfFile2 = new File(member1_dir4test, "BACKUP"+RunCacheInOldGemfire.diskStoreName2+"_2.drf");
        assertTrue(!drfFile1.exists());
        assertTrue(!drfFile2.exists());
      }


      DiskStoreImpl.validate(RunCacheInOldGemfire.diskStoreName1, getDiskDirsForMember(member1_dir4test));
      DiskStoreImpl.validate(RunCacheInOldGemfire.diskStoreName2, getDiskDirsForMember(member1_dir4test));
      
      
      //Upgrade the disk-stores for member2
      
      cmdResult = executeUpgradeDiskStore(RunCacheInOldGemfire.diskStoreName1, member2_dir4test, "1");
      assertTrue(Status.OK.equals(cmdResult.getStatus()));
      cmdResult = executeUpgradeDiskStore(RunCacheInOldGemfire.diskStoreName2, member2_dir4test, "1");
      assertTrue(Status.OK.equals(cmdResult.getStatus()));


    //The following calls should result in RuntimeException, hence validating that the gfsh command to upgrade the disk-stores successfully upgrade the 
      // specified offline disk-store
      try {
        DiskStoreImpl ds1_member2 = DiskStoreImpl.offlineCompact(RunCacheInOldGemfire.diskStoreName1, 
            getDiskDirsForMember(member2_dir4test), true, RunCacheInOldGemfire.maxOplogSize);
        fail("Did not throw the expected exception");
      } catch (RuntimeException rte) {
        assertTrue(rte.getMessage().contains("This disk store is already at version "+Version.CURRENT));
      } 
      
      try {
        DiskStoreImpl ds2_member2 = DiskStoreImpl.offlineCompact(RunCacheInOldGemfire.diskStoreName2, 
            getDiskDirsForMember(member2_dir4test), true, RunCacheInOldGemfire.maxOplogSize);
      } catch (RuntimeException rte) {
        assertTrue(rte.getMessage().contains("This disk store is already at version "+Version.CURRENT));
      }
      
      {
        File drfFile1 = new File(member2_dir4test, "BACKUP"+RunCacheInOldGemfire.diskStoreName1+"_2.drf");
        File drfFile2 = new File(member2_dir4test, "BACKUP"+RunCacheInOldGemfire.diskStoreName2+"_2.drf");
        assertTrue(!drfFile1.exists());
        assertTrue(!drfFile2.exists());
      }

      DiskStoreImpl.validate(RunCacheInOldGemfire.diskStoreName1, getDiskDirsForMember(member2_dir4test));
      DiskStoreImpl.validate(RunCacheInOldGemfire.diskStoreName2, getDiskDirsForMember(member2_dir4test));
    }
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
    String oldgemfire = UpgradeDiskStoreDUnitTest.getGemfireLocation(version);
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

  private CommandResult executeUpgradeDiskStore(String name, String diskDirs, String maxOplogSize) {
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.UPGRADE_OFFLINE_DISK_STORE);
    csb.addOption(CliStrings.UPGRADE_OFFLINE_DISK_STORE__NAME, name);
    csb.addOption(CliStrings.UPGRADE_OFFLINE_DISK_STORE__DISKDIRS, diskDirs);
    csb.addOption(CliStrings.UPGRADE_OFFLINE_DISK_STORE__MAXOPLOGSIZE, maxOplogSize);
    
    String commandString = csb.toString();
    getLogWriter().info("Command String : " + commandString);
    
    CommandResult commandResult = executeCommand(commandString);
    String resultAsString = commandResultToString(commandResult);
    getLogWriter().info("Command Result : " + resultAsString);
    
    return commandResult;
  }

}
