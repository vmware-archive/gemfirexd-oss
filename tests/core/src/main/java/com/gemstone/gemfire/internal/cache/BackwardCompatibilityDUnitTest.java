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

import hydra.FileUtil;
import hydra.GemFireDescription;
import hydra.GemFirePrms;
import hydra.HostDescription;
import hydra.HydraConfigException;
import hydra.HydraRuntimeException;
import hydra.ProcessMgr;
import hydra.RemoteTestModule;
import hydra.TestConfig;

import java.io.File;
import java.io.IOException;
import java.rmi.RemoteException;
import java.util.Properties;

import util.TestException;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.AvailablePort;

import dunit.DistributedTestCase;
import dunit.Host;
import dunit.SerializableCallable;
import dunit.VM;


/**
 * Tests backward compatability with rolling version upgrades
 * @author sjigyasu
 */
public class BackwardCompatibilityDUnitTest extends DistributedTestCase {

  private final String rollingUpgradeBaseVersion = "7.0.99";
  
  private final String rollingUpgradeBaseVersionDir = "/export/gcm/where/gemfire/releases/GemFire7.0.99-all";
  
  static final String GCM_WHERE;

  static {
    String gcmdir = System.getenv("GCMDIR");
    if (gcmdir == null || gcmdir.length() == 0) {
      gcmdir = "/gcm";
    }
    GCM_WHERE = gcmdir + "/where";
  }
  private String vm1WorkingDir;
  private String vm2WorkingDir;
  private String vm3WorkingDir;
  private String vm4WorkingDir;

  public BackwardCompatibilityDUnitTest(String name) {  
    super(name);
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    vm1WorkingDir = (String)Host.getHost(0).getVM(0).invoke(getVMWorkingDir);
    vm2WorkingDir = (String)Host.getHost(0).getVM(1).invoke(getVMWorkingDir);
    vm3WorkingDir = (String)Host.getHost(0).getVM(2).invoke(getVMWorkingDir);
    vm4WorkingDir = (String)Host.getHost(0).getVM(3).invoke(getVMWorkingDir);

    getLogWriter().info("VMDIR:" + vm1WorkingDir);
    getLogWriter().info("VMDIR:" + vm2WorkingDir);
    getLogWriter().info("VMDIR:" + vm3WorkingDir);
    getLogWriter().info("VMDIR:" + vm4WorkingDir);
    
    // Begin with clean directories
    cleanUp(new String[]{vm1WorkingDir, vm2WorkingDir, vm3WorkingDir, vm4WorkingDir});
  }
  SerializableCallable getVMWorkingDir = new SerializableCallable(
      "get working directory of this JVM") {
    @Override
    public Object call() {
      return getSysDir();
    }
  };
  public String getSysDir(){
    String sysDirName = null;
    String gemfireName =
        System.getProperty(GemFirePrms.GEMFIRE_NAME_PROPERTY);
      if ( gemfireName == null ) {
        String s = "No gemfire name has been specified";
        throw new HydraConfigException(s);
      } else {
        GemFireDescription gfd =
          TestConfig.getInstance().getGemFireDescription(gemfireName);

        sysDirName = gfd.getSystemDirectory() + "_" + ProcessMgr.getProcessId();
        if (!FileUtil.exists(sysDirName)) {
          FileUtil.mkdir(new File(sysDirName));
          HostDescription hd = gfd.getHostDescription();
          try {
            RemoteTestModule.Master.recordDir(hd, gfd.getName(), sysDirName);
          } catch (RemoteException e) {
            String s = "Unable to access master to record directory: " + sysDirName;
            throw new HydraRuntimeException(s, e);
          }
        }
      }
      return sysDirName;
  }
  
  private void cleanUp(String[] dirs){
    
    // Controller's data stores
    //deleteDataDictionaryDir();
    //deleteDefaultDiskStoreFiles(getSysDirName(getGemFireDescription()));
    
    // Individual VM's datastores
    for(String dirName : dirs){
      File dir = new File(dirName + "/datadictionary");
      deleteDir(dir);
//      deleteDefaultDiskStoreFiles(dirName);
    }
  }
  public static boolean deleteDir(File dir) {
    if (!dir.exists()) return false;
    deleteDirContents(dir);
    return deleteDirOnly(dir);
  }

  protected static void deleteDirContents(final File dir) {
    if (!dir.exists()) return;
    final File[] files = dir.listFiles();
    if (files != null) {
      for (File f : files) {
        if (f.isDirectory()) {
          deleteDirContents(f);
        }
        deleteDirOnly(f);
      }
    }
  }
  protected static boolean deleteDirOnly(File dir) {
    int numTries = 10;
    while (numTries-- > 0 && dir.exists()) {
      if (dir.delete()) {
        return true;
      }
    }
    return (numTries > 0);
  }
  
  /**
   * Tests a mix of 7.0.99 and current version locators and servers.
   * Valid only from 7.0.99 onwards.
   * Test config is as follows:
   * 7.0.99 locator -- controller-directory/baselocator
   * vm3 -- 7.0.99 server
   * vm4 -- 7.0.99 server
   * vm2 -- 7.1 locator
   * vm1 -- 7.1 server
   */
  /**
   * Not valid since 7.1. Commenting it out.
   */
  public void testMixedVersion() throws Exception{
    /*
    // Create a locator working dir.
    String baseLocatorPath = getSysDir() + "/baseVersionLocator";
    getLogWriter().info("Creating locator dir for base version: "+ baseLocatorPath);
    File baseVersionLocatorDir = new File(baseLocatorPath) ;
    assertTrue(baseVersionLocatorDir.mkdir());

    // Start a locator and two servers 
    // For the two servers, use VM working dirs of the third and fourth VM
    getLogWriter().info("Starting locator version: " + rollingUpgradeBaseVersion + " with working dir: " + baseLocatorPath);
    int baseVersionLocatorPort = startVersionedLocator(rollingUpgradeBaseVersion, rollingUpgradeBaseVersionDir, baseLocatorPath, true);
    Properties props = new Properties();
    props.put("locators", "localhost:"+baseVersionLocatorPort);
    
    
    getLogWriter().info("Starting server version: " + rollingUpgradeBaseVersion + " with working dir: " + vm3WorkingDir);
    int clientPort1 = startVersionedServer(rollingUpgradeBaseVersion, rollingUpgradeBaseVersionDir, vm3WorkingDir, 0, baseVersionLocatorPort, true);
    
    getLogWriter().info("Starting server version: " + rollingUpgradeBaseVersion + " with working dir: " + vm4WorkingDir);
    startVersionedServer(rollingUpgradeBaseVersion, rollingUpgradeBaseVersionDir, vm4WorkingDir, 0, baseVersionLocatorPort, true);

    // Create cache in the two servers
    createCacheAndRegion(Host.getHost(0).getVM(2), props);
    createCacheAndRegion(Host.getHost(0).getVM(3), props);
    
    // Add some data
    addData(Host.getHost(0).getVM(3));
    
    getLogWriter().info("Starting locator with current version in the second VM of this dunit test");
    int currentVersionLocatorPort = this.startLocator(Host.getHost(0).getVM(1), props);

    props.remove("locators");
    props.put("locators", "localhost:" + currentVersionLocatorPort);
    
    getLogWriter().info("Starting server with current version in the first VM");
    startCacheServer(Host.getHost(0).getVM(0), props);
    createCacheAndRegion(Host.getHost(0).getVM(0), props);
   
    verifyData(Host.getHost(0).getVM(0));
    
    getLogWriter().info("Stopping the server and the locator running in test VMs");
    this.stopCacheServer(Host.getHost(0).getVM(0), props);
    this.stopLocator(Host.getHost(0).getVM(1), props);
    
    getLogWriter().info("Stopping versioned servers");
    stopVersionedServer(rollingUpgradeBaseVersion, rollingUpgradeBaseVersionDir, vm3WorkingDir);
    stopVersionedServer(rollingUpgradeBaseVersion, rollingUpgradeBaseVersionDir, vm4WorkingDir);
    
    stopVersionedLocator(rollingUpgradeBaseVersion, rollingUpgradeBaseVersionDir, baseLocatorPath);
    */
  }
  private void createCacheAndRegion(VM vm, Properties props) {
    final Properties p = props; 
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        
        DistributedSystem ds = (DistributedSystem)DistributedSystem.connect(p);
        Cache cache = null;
        try{
          cache = CacheFactory.getAnyInstance();
        } catch (CacheClosedException e) {
          cache = CacheFactory.create(ds);
        }
        Region r = cache.createRegionFactory(RegionShortcut.REPLICATE).create("REG1");
        return null;
      }
    });
  }
  
  private void addData(VM vm) {
    vm.invoke(new SerializableCallable("Adding data") {
      @Override
      public Object call() throws Exception {
        Cache cache = CacheFactory.getAnyInstance();
        Region r = cache.getRegion("/REG1");
        for(int i = 0; i < 10; i++) {
          r.put("name"+i, "value"+i);  
        }
        Region reg1 = cache.getRegion("/REG1");
        assertEquals(10, r.size());
        
        return null;
      }
    });
  }
  private void verifyData(VM vm) {
    getLogWriter().info("Verifying data for" + vm);
    vm.invoke(new SerializableCallable("Verifying data"){
      @Override
      public Object call() throws Exception {
        Cache cache = CacheFactory.getAnyInstance();
        Region r = cache.getRegion("/REG1");
        assertEquals(10, r.size());
        for(int i = 0; i < 10; i++) {
          assertTrue(r.containsKey("name"+i));
          assertEquals("value"+i, r.get("name"+i));
        }
        return null;
      }
    });
  }
  public String getTestLogNamePrefix() {
    return getTestClass().getName() + "-" + getTestName();
  }
  
  private int startVersionedLocator(String version, String versionDir, String workingDir, 
      boolean withAuth) throws Exception{

    String gfeDir = versionDir.charAt(0) != '/' ? GCM_WHERE + '/' + versionDir
        : versionDir;
    String utilLauncher = getGemfireLauncher(gfeDir);
    final int peerDiscoveryPort = AvailablePort
        .getRandomAvailablePort(AvailablePort.SOCKET);

    getLogWriter().info(
        "Starting locator version " + version + " from " + gfeDir + " on port "
            + peerDiscoveryPort);

    final String logFile = getTestLogNamePrefix() + "-locator" + version + ".log";
    
    String[] startOps = null; 
        
    if(withAuth){
      startOps = new String[] { utilLauncher, "start-locator",
          "-dir=" + workingDir, 
          "-port=" + peerDiscoveryPort
          };
    }
    else{
      // TODO: Add security properties
      startOps = new String[] { utilLauncher, "start-locator",
          "dir=" + workingDir, 
          "port=" + peerDiscoveryPort
          };
    }
    final Process serverProc = new ProcessBuilder(startOps).start();
    final int exitValue = serverProc.waitFor();
    if (exitValue != 0) {
      throw new TestException("Unexpected exit value " + exitValue
          + " while starting locator version " + version + ". See logs in "
          + logFile + " and start file output.");
    }
    return peerDiscoveryPort;
  }
  
  public void stopVersionedLocator(String version, String versionDir,
      String workingDir) throws Exception {

    String gfxdDir = versionDir.charAt(0) != '/' ? GCM_WHERE + '/' + versionDir
        : versionDir;
    String utilLauncher = getGemfireLauncher(gfxdDir);

    getLogWriter().info(
        "Stopping locator version " + version + " from " + gfxdDir);

    final String[] startOps = new String[] { utilLauncher, "stop-locator",
        "-dir=" + workingDir };

    final Process serverProc = new ProcessBuilder(startOps).start();
    final int exitValue = serverProc.waitFor();
    if (exitValue != 0) {
      throw new TestException("Unexpected exit value " + exitValue
          + " while stopping locator version " + version + '.');
    }
  }
  
  public int startVersionedServer(String version, String versionDir, String workingDir, 
      int mcastPort, int locatorPort, boolean withAuth) throws Exception{
    String gfxdDir = versionDir.charAt(0) != '/' ? GCM_WHERE + '/' + versionDir
        : versionDir;
    String utilLauncher = getCacheServerLauncher(gfxdDir);

    final int clientPort = AvailablePort
        .getRandomAvailablePort(AvailablePort.SOCKET);
    
    final String logFile = getTestLogNamePrefix() + "-server" + version + ".log";
    final String[] startOps;
    if(locatorPort > 0){
      if(withAuth){
        // TODO: Add auth parameters
        startOps = new String[] { utilLauncher, "start",
            "-dir=" + workingDir, 
            "-server-port=" + clientPort,
            //"-J-DgemfirePropertyFile="+gfePropFileName
            "locators=localhost["+locatorPort+"]"
            };
      }
      else{
        startOps = new String[] { utilLauncher, "start",
            "-dir=" + workingDir, 
            "-server-port=" + clientPort,
            //"-J-DgemfirePropertyFile="+gfePropFileName
            "locators=localhost["+locatorPort+"]"
            };
      }
    }
    else{
      startOps = new String[] { utilLauncher, "start",
          "-dir=" + workingDir, 
          "-server-port=" + clientPort,
          "-mcast-port" + mcastPort
          };
    }
    getLogWriter().info(
        "Starting cache server version " + version + " from " + gfxdDir + " on port "
            + clientPort);
    
    final Process serverProc = new ProcessBuilder(startOps).start();
    final int exitValue = serverProc.waitFor();
    if (exitValue != 0) {
      throw new TestException("Unexpected exit value " + exitValue
          + " while starting cache server version " + version + ". See logs in "
          + logFile
          );
    }
    return clientPort;
  }

  public void stopVersionedServer(String version, String versionDir,
      String workingDir) throws Exception {
    String gfeDir = versionDir.charAt(0) != '/' ? GCM_WHERE + '/' + versionDir
        : versionDir;
    String utilLauncher = getCacheServerLauncher(gfeDir);

    getLogWriter().info(
        "Stopping cache server version " + version + " from " + gfeDir);

    final String[] startOps = new String[] { utilLauncher, "stop",
        "-dir=" + workingDir };

    final Process serverProc = new ProcessBuilder(startOps).start();
    final int exitValue = serverProc.waitFor();
    if (exitValue != 0) {
      throw new TestException("Unexpected exit value " + exitValue
          + " while stopping cache server version " + version + '.');
    }
    
  }
  private int startLocator(VM locatorVM, Properties props) throws IOException{
    
    final int locatorPort = AvailablePort
        .getRandomAvailablePort(AvailablePort.SOCKET);
    final Properties pr = props;
    final File locatorLogFile = new File("locator-" + locatorPort + ".log");
    locatorVM.invoke(new SerializableCallable() {
      public Object call() throws IOException {
        try{
          
        Properties p = getDistributedSystemProperties();
        p.putAll(pr);
        Locator.startLocatorAndDS(locatorPort, locatorLogFile, p);
        }
        catch(Exception e){
          e.printStackTrace();
        }
        return null;
      }
    });
    return locatorPort;
  }
  private void stopLocator(VM locatorVM, Properties props) throws Exception{
    final Properties p = props;
    locatorVM.invoke(new SerializableCallable() {
      public Object call() throws IOException {
        try{
          Locator locator = Locator.getLocator();
          if(locator == null) {
            throw new Exception("No locator found");
          } else {
            locator.stop();
          }
        }
        catch(Exception e){
          e.printStackTrace();
        }
        return null;
      }
    });
    
  }
  private void startCacheServer(VM serverVM, Properties props) {
    final Properties p = props;
    serverVM.invoke(new SerializableCallable(){
      @Override
      public Object call() throws Exception {
        try{
          InternalDistributedSystem ds = (InternalDistributedSystem )DistributedSystem.connect(p);
          Cache cache = CacheFactory.create(ds);
        } catch (Exception e) {
          e.printStackTrace();
        }
        return null;
      }
    });
  }
  private void stopCacheServer(VM serverVM, Properties props) {
    final Properties p = props;
    serverVM.invoke(new SerializableCallable(){
      @Override
      public Object call() throws Exception {
        try{
          Cache cache = CacheFactory.getAnyInstance();
          if(cache != null) {
            cache.close();
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
        return null;
      }
    });
  }

  
  
  private String getGemfireLauncher(final String gfeDir) {
    String os = System.getProperty("os.name").toLowerCase();
    
    if (os.indexOf("win") >= 0) {
      return gfeDir + "/product/bin/gemfire.bat";
    }
    else {
      return gfeDir + "/product/bin/gemfire";
    }
  }
  private String getCacheServerLauncher(final String gfeDir) {
    String os = System.getProperty("os.name").toLowerCase();
    
    if (os.indexOf("win") >= 0) {
      return gfeDir + "/product/bin/cacheserver.bat";
    }
    else {
      return gfeDir + "/product/bin/cacheserver";
    }
  }
  
  
}
