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

import java.io.File;
import java.util.Properties;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.DiskStoreFactory;
import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.management.cli.Result.Status;
import com.gemstone.gemfire.management.internal.cli.domain.Stock;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.result.CommandResult;
import com.gemstone.gemfire.management.internal.cli.util.CommandStringBuilder;

import dunit.Host;
import dunit.SerializableCallable;
import dunit.VM;



public class IndexCommandsDUnitTest extends CliCommandTestBase {

  private static final long serialVersionUID = 1L;
  private static final String VM1Name = "VM1";
  private static final String group1 = "G1";
  private static final String indexName = "Id1";
  private static final String parRegPersName = "ParRegPers";
  private static final String repRegPersName = "RepRegPer";
  
  public IndexCommandsDUnitTest(String name) {
    super(name);
    // TODO Auto-generated constructor stub
  }
  private Region<?,?> createParReg(String regionName, Cache cache, Class keyConstraint, Class valueConstraint ) {
    RegionFactory regionFactory = cache.createRegionFactory();
    regionFactory.setDataPolicy(DataPolicy.PARTITION);
    regionFactory.setKeyConstraint(keyConstraint);
    regionFactory.setValueConstraint(valueConstraint);
    return regionFactory.create(regionName);
  }
  
  private Region<?,?> createParRegWithPersistence(String regionName, String diskStoreName, String diskDirName) {
    Cache cache = getCache();
    File diskStoreDirFile = new File(diskDirName);
    diskStoreDirFile.deleteOnExit();
    
    if (!diskStoreDirFile.exists()) {
      diskStoreDirFile.mkdirs();
    }
    
    DiskStoreFactory diskStoreFactory = cache.createDiskStoreFactory();
    diskStoreFactory.setDiskDirs(new File[] { diskStoreDirFile });
    diskStoreFactory.setMaxOplogSize(1);
    diskStoreFactory.setAllowForceCompaction(true);
    diskStoreFactory.setAutoCompact(false);
    diskStoreFactory.create(diskStoreName);
    
    /****
     * Eviction Attributes
     */
    EvictionAttributes ea = EvictionAttributes.createLRUEntryAttributes(1, EvictionAction.OVERFLOW_TO_DISK);
    
    RegionFactory regionFactory = cache.createRegionFactory();
    regionFactory.setDiskStoreName(diskStoreName);
    regionFactory.setDiskSynchronous(true);
    regionFactory.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
    regionFactory.setEvictionAttributes(ea);
    
    return regionFactory.create(regionName);
  }
  
  private Region<?,?> createRepRegWithPersistence(String regionName, String diskStoreName, String diskDirName) {
    Cache cache = getCache();
    File diskStoreDirFile = new File(diskDirName);
    diskStoreDirFile.deleteOnExit();
    
    if (!diskStoreDirFile.exists()) {
      diskStoreDirFile.mkdirs();
    }
    
    DiskStoreFactory diskStoreFactory = cache.createDiskStoreFactory();
    diskStoreFactory.setDiskDirs(new File[] { diskStoreDirFile });
    diskStoreFactory.setMaxOplogSize(1);
    diskStoreFactory.setAllowForceCompaction(true);
    diskStoreFactory.setAutoCompact(false);
    diskStoreFactory.create(diskStoreName);
    
    /****
     * Eviction Attributes
     */
    EvictionAttributes ea = EvictionAttributes.createLRUEntryAttributes(1, EvictionAction.OVERFLOW_TO_DISK);
    
    RegionFactory regionFactory = cache.createRegionFactory();
    regionFactory.setDiskStoreName(diskStoreName);
    regionFactory.setDiskSynchronous(true);
    regionFactory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    regionFactory.setEvictionAttributes(ea);
    
    return regionFactory.create(regionName);
  }
 
  
    public void testCreateKeyIndexOnRegionWithPersistence() {
      setupSystemPersist();
      
      //Creating key indexes on Persistent Partitioned Region
      CommandStringBuilder csb = new CommandStringBuilder(CliStrings.CREATE_INDEX);
      csb.addOption(CliStrings.CREATE_INDEX__NAME, "id1");
      csb.addOption(CliStrings.CREATE_INDEX__EXPRESSION, "ty");
      csb.addOption(CliStrings.CREATE_INDEX__REGION, "/" + parRegPersName);
      csb.addOption(CliStrings.CREATE_INDEX__TYPE, "key");
      String commandString = csb.toString();
      writeToLog("Command String :\n ", commandString);
      CommandResult commandResult = executeCommand(commandString);
      String resultAsString = commandResultToString(commandResult);
      writeToLog("Command Result :\n", resultAsString);
      assertTrue(Status.OK.equals(commandResult.getStatus()));
      //Creating key indexes on Persistent Replicated Regions
      csb = new CommandStringBuilder(CliStrings.CREATE_INDEX);
      csb.addOption(CliStrings.CREATE_INDEX__NAME, "id2");
      csb.addOption(CliStrings.CREATE_INDEX__EXPRESSION, "ee");
      csb.addOption(CliStrings.CREATE_INDEX__REGION, "/" + repRegPersName);
      csb.addOption(CliStrings.CREATE_INDEX__TYPE, "key");
      commandString = csb.toString();
      writeToLog("Command String :\n ", commandString);
      commandResult = executeCommand(commandString);
      resultAsString = commandResultToString(commandResult);
      writeToLog("Command Result :\n", resultAsString);
      assertTrue(Status.OK.equals(commandResult.getStatus()));
  
    }
    

  
  public void testCreateAndDestroyIndex() {
    setupSystem();
    /***
     * Basic Create and Destroy 
     */
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.CREATE_INDEX);
    csb.addOption(CliStrings.CREATE_INDEX__NAME, indexName);
    csb.addOption(CliStrings.CREATE_INDEX__EXPRESSION, "key");
    csb.addOption(CliStrings.CREATE_INDEX__REGION, "/StocksParReg");
    
    String commandString = csb.toString();
    writeToLog("Command String :\n ", commandString);
    CommandResult commandResult = executeCommand(commandString);
    String resultAsString = commandResultToString(commandResult);
    writeToLog("testCreateAndDestroyIndex", resultAsString);
    assertEquals(Status.OK, commandResult.getStatus());
    
    csb = new CommandStringBuilder(CliStrings.LIST_INDEX);
    commandString = csb.toString();
    commandResult = executeCommand(commandString);
    resultAsString = commandResultToString(commandResult);
    assertEquals(Status.OK, commandResult.getStatus());
    assertTrue(resultAsString.contains(indexName));
        
    csb = new CommandStringBuilder(CliStrings.DESTROY_INDEX);
    csb.addOption(CliStrings.DESTROY_INDEX__NAME , indexName);
    csb.addOption(CliStrings.DESTROY_INDEX__REGION, "/StocksParReg");
    commandString = csb.toString();
    writeToLog("Command String :\n ", commandString);

    commandResult = executeCommand(commandString);
    resultAsString = commandResultToString(commandResult);
    writeToLog("testCreateAndDestroyIndex", resultAsString);
    assertEquals(commandResult.getStatus(), Status.OK);
    
    commandResult = executeCommand(CliStrings.LIST_INDEX);
    resultAsString = commandResultToString(commandResult);
    assertEquals(commandResult.getStatus(), Status.OK);
    assertFalse(resultAsString.contains(indexName));
      
  }
  
  public void testCreateAndDestroyIndexOnMember() {
    setupSystem();
    /***
     * Basic Create and Destroy 
     */
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.CREATE_INDEX);
    csb.addOption(CliStrings.CREATE_INDEX__NAME, indexName);
    csb.addOption(CliStrings.CREATE_INDEX__EXPRESSION, "key");
    csb.addOption(CliStrings.CREATE_INDEX__REGION, "/StocksParReg");
    csb.addOption(CliStrings.CREATE_INDEX__MEMBER, VM1Name);
    csb.addOption(CliStrings.CREATE_INDEX__TYPE, "key");
    
    String commandString = csb.toString();
    CommandResult commandResult = executeCommand(commandString);
    String resultAsString = commandResultToString(commandResult);
    writeToLog("Command String :\n ", commandString);
    writeToLog("testCreateAndDestroyIndexOnMember", resultAsString);
   
    assertEquals(Status.OK, commandResult.getStatus());
    
    csb = new CommandStringBuilder(CliStrings.LIST_INDEX);
    commandString = csb.toString();
    commandResult = executeCommand(commandString);
    resultAsString = commandResultToString(commandResult);
    writeToLog("Command String :\n ", commandString);
    writeToLog("testCreateAndDestroyIndexOnMember", resultAsString);
    assertEquals(Status.OK, commandResult.getStatus());
    assertTrue(resultAsString.contains(indexName));
    assertTrue(resultAsString.contains(VM1Name));
    
        
    csb = new CommandStringBuilder(CliStrings.DESTROY_INDEX);
    csb.addOption(CliStrings.DESTROY_INDEX__NAME, indexName);
    csb.addOption(CliStrings.DESTROY_INDEX__REGION, "/StocksParReg");
    csb.addOption(CliStrings.DESTROY_INDEX__MEMBER, VM1Name);
    commandString = csb.toString();
    commandResult = executeCommand(commandString);
    resultAsString = commandResultToString(commandResult);
    writeToLog("Command String :\n ", commandString);
    writeToLog("testCreateAndDestroyIndexOnMember", resultAsString);
    assertEquals(commandResult.getStatus(), Status.OK);
    
    commandResult = executeCommand(CliStrings.LIST_INDEX);
    resultAsString = commandResultToString(commandResult);
    writeToLog("Command String :\n ", commandString);
    writeToLog("testCreateAndDestroyIndexOnMember", resultAsString);
    assertEquals(commandResult.getStatus(), Status.OK);
    assertFalse(resultAsString.contains(VM1Name));
  }
  
  public void testCreateAndDestroyIndexOnGroup() {
    setupSystem();
    /***
     * Basic Create and Destroy 
     */
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.CREATE_INDEX);
    csb.addOption(CliStrings.CREATE_INDEX__NAME, indexName);
    csb.addOption(CliStrings.CREATE_INDEX__EXPRESSION, "key");
    csb.addOption(CliStrings.CREATE_INDEX__REGION, "/StocksParReg");
    csb.addOption(CliStrings.CREATE_INDEX__TYPE, "hash");
    csb.addOption(CliStrings.CREATE_INDEX__GROUP, group1);
    
    String commandString = csb.toString();
    CommandResult commandResult = executeCommand(commandString);
    String resultAsString = commandResultToString(commandResult);
    writeToLog("Command String :\n ", commandString);
    writeToLog("testCreateAndDestroyIndexOnGroup", resultAsString);
    assertEquals(Status.OK, commandResult.getStatus());
    
    csb = new CommandStringBuilder(CliStrings.LIST_INDEX);
    commandString = csb.toString();
    commandResult = executeCommand(commandString);
    resultAsString = commandResultToString(commandResult);
    assertEquals(Status.OK, commandResult.getStatus());
    assertEquals(true, resultAsString.contains(indexName));
    assertEquals(true, resultAsString.contains(VM1Name));
        
    csb = new CommandStringBuilder(CliStrings.DESTROY_INDEX);
    csb.addOption(CliStrings.DESTROY_INDEX__NAME, indexName);
    csb.addOption(CliStrings.DESTROY_INDEX__REGION, "/StocksParReg");
    csb.addOption(CliStrings.DESTROY_INDEX__GROUP, group1);
    commandString = csb.toString();
    commandResult = executeCommand(commandString);
    resultAsString = commandResultToString(commandResult);
    writeToLog("Command String :\n ", commandString);
    writeToLog("testCreateAndDestroyIndexOnGroup", resultAsString);
    assertEquals(commandResult.getStatus(), Status.OK);
    
    commandResult = executeCommand(CliStrings.LIST_INDEX);
    resultAsString = commandResultToString(commandResult);
    assertEquals(commandResult.getStatus(), Status.OK);
    assertFalse(resultAsString.contains(VM1Name));
    
    /***
     * In case of a partitioned region , the index might get created on a 
     * member which hosts the region and is not the member of the group1
     */
    if (resultAsString.contains(indexName)) {
      csb = new CommandStringBuilder(CliStrings.DESTROY_INDEX);
      csb.addOption(CliStrings.DESTROY_INDEX__NAME , indexName);
      csb.addOption(CliStrings.DESTROY_INDEX__REGION, "/StocksParReg");
      commandString = csb.toString();
      commandResult = executeCommand(commandString);
      resultAsString = commandResultToString(commandResult);
      assertEquals(commandResult.getStatus(), Status.OK);
      
      commandResult = executeCommand(CliStrings.LIST_INDEX);
      resultAsString = commandResultToString(commandResult);
      writeToLog("Command String :\n ", commandString);
      writeToLog("testCreateAndDestroyIndexOnGroup", resultAsString);

      assertEquals(commandResult.getStatus(), Status.OK);
      assertFalse(resultAsString.contains(indexName));
      assertTrue(resultAsString.contains(CliStrings.LIST_INDEX__INDEXES_NOT_FOUND_MESSAGE));
    }
  }
  
  public void testCreateAndDestroyIndexWithIncorrectInput() {
    setupSystem();
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.CREATE_INDEX);
    csb.addOption(CliStrings.CREATE_INDEX__NAME, indexName);
    csb.addOption(CliStrings.CREATE_INDEX__EXPRESSION, "key");
    csb.addOption(CliStrings.CREATE_INDEX__REGION, "/StocksParReg");
    csb.addOption(CliStrings.CREATE_INDEX__TYPE, "hash");
    String commandString = csb.toString();
    CommandResult commandResult = executeCommand(commandString);
    String resultAsString = commandResultToString(commandResult);
    
    assertEquals(commandResult.getStatus(), Status.OK);
    
    //CREATE the same index 
    csb = new CommandStringBuilder(CliStrings.CREATE_INDEX);
    csb.addOption(CliStrings.CREATE_INDEX__NAME, indexName);
    csb.addOption(CliStrings.CREATE_INDEX__EXPRESSION, "key");
    csb.addOption(CliStrings.CREATE_INDEX__REGION, "/StocksParReg");
    csb.addOption(CliStrings.CREATE_INDEX__TYPE, "hash");
    
    commandString = csb.toString();
    commandResult = executeCommand(commandString);
    resultAsString = commandResultToString(commandResult);
    assertTrue(commandResult.getStatus().equals(Status.ERROR));
    //assertTrue(resultAsString.contains(CliStrings.format(CliStrings.CREATE_INDEX__NAME__CONFLICT, indexName)));
    writeToLog("Command String :\n ", commandString);
    writeToLog("testCreateAndDestroyIndexWithIncorrectInput", resultAsString);

    
    //Create index on a wrong regionPath
    csb = new CommandStringBuilder(CliStrings.CREATE_INDEX);
    csb.addOption(CliStrings.CREATE_INDEX__NAME, indexName);
    csb.addOption(CliStrings.CREATE_INDEX__EXPRESSION, "key");
    csb.addOption(CliStrings.CREATE_INDEX__REGION, "/StocsParReg");
    csb.addOption(CliStrings.CREATE_INDEX__TYPE, "hash");
    
    commandString = csb.toString();
    commandResult = executeCommand(commandString);
    resultAsString = commandResultToString(commandResult);
    writeToLog("Command String :\n ", commandString);
    writeToLog("testCreateAndDestroyIndexWithIncorrectInput", resultAsString);
    assertTrue(commandResult.getStatus().equals(Status.ERROR));
    //assertTrue(resultAsString.contains(CliStrings.format(CliStrings.CREATE_INDEX__INVALID__REGIONPATH, "/StocsParReg")));

    
    //Create index with wrong expression 
    csb = new CommandStringBuilder(CliStrings.CREATE_INDEX);
    csb.addOption(CliStrings.CREATE_INDEX__NAME, "Id2");
    csb.addOption(CliStrings.CREATE_INDEX__EXPRESSION, "rey");
    csb.addOption(CliStrings.CREATE_INDEX__REGION, "/StocksParReg");
    csb.addOption(CliStrings.CREATE_INDEX__TYPE, "hash");
    
    commandString = csb.toString();
    commandResult = executeCommand(commandString);
    resultAsString = commandResultToString(commandResult);
    writeToLog("Command String :\n ", commandString);
    writeToLog("testCreateAndDestroyIndexWithIncorrectInput", resultAsString);
    assertTrue(commandResult.getStatus().equals(Status.ERROR));
    
    //Create index with wrong type 
    csb = new CommandStringBuilder(CliStrings.CREATE_INDEX);
    csb.addOption(CliStrings.CREATE_INDEX__NAME, indexName);
    csb.addOption(CliStrings.CREATE_INDEX__EXPRESSION, "key");
    csb.addOption(CliStrings.CREATE_INDEX__REGION, "/StocksParReg");
    csb.addOption(CliStrings.CREATE_INDEX__TYPE, "bash");
    
    commandString = csb.toString();
    commandResult = executeCommand(commandString);
    resultAsString = commandResultToString(commandResult);
    writeToLog("Command String :\n ", commandString);
    writeToLog("testCreateAndDestroyIndexWithIncorrectInput", resultAsString);
    assertTrue(resultAsString.contains(CliStrings.CREATE_INDEX__INVALID__INDEX__TYPE__MESSAGE));
    assertTrue(commandResult.getStatus().equals(Status.ERROR));
    
    //Destroy index with incorrect indexName 
    csb = new CommandStringBuilder(CliStrings.DESTROY_INDEX);
    csb.addOption(CliStrings.DESTROY_INDEX__NAME , "Id2");
    commandString = csb.toString();
    commandResult = executeCommand(commandString);
    resultAsString = commandResultToString(commandResult);
    writeToLog("Command String :\n ", commandString);
    writeToLog("testCreateAndDestroyIndexWithIncorrectInput", resultAsString);
    assertTrue(commandResult.getStatus().equals(Status.ERROR));
    assertTrue(resultAsString.contains(CliStrings.format(CliStrings.DESTROY_INDEX__INDEX__NOT__FOUND, "Id2")));

    //Destroy index with incorrect region 
    csb = new CommandStringBuilder(CliStrings.DESTROY_INDEX);
    csb.addOption(CliStrings.DESTROY_INDEX__NAME , indexName);
    csb.addOption(CliStrings.DESTROY_INDEX__REGION, "Region");
    commandString = csb.toString();
    commandResult = executeCommand(commandString);
    resultAsString = commandResultToString(commandResult);
    writeToLog("Command String :\n ", commandString);
    writeToLog("testCreateAndDestroyIndexWithIncorrectInput", resultAsString);
    assertTrue(commandResult.getStatus().equals(Status.ERROR));
    assertTrue(resultAsString.contains(CliStrings.format(CliStrings.DESTROY_INDEX__REGION__NOT__FOUND, "Region")));

    //Destroy index with incorrect memberName
    csb = new CommandStringBuilder(CliStrings.DESTROY_INDEX);
    csb.addOption(CliStrings.DESTROY_INDEX__NAME , indexName);
    csb.addOption(CliStrings.DESTROY_INDEX__REGION, "Region");
    csb.addOption(CliStrings.DESTROY_INDEX__MEMBER, "wrongOne");
    commandString = csb.toString();
    commandResult = executeCommand(commandString);
    resultAsString = commandResultToString(commandResult);
    writeToLog("Command String :\n ", commandString);
    writeToLog("testCreateAndDestroyIndexWithIncorrectInput", resultAsString);
    assertTrue(commandResult.getStatus().equals(Status.ERROR));
    
    //Destroy index with no option
    csb = new CommandStringBuilder(CliStrings.DESTROY_INDEX);
    commandString = csb.toString();
    commandResult = executeCommand(commandString);
    resultAsString = commandResultToString(commandResult);
    writeToLog("Command String :\n ", commandString);
    writeToLog("testCreateAndDestroyIndexWithIncorrectInput", resultAsString);
    assertTrue(commandResult.getStatus().equals(Status.ERROR));
  }
  
  public void testDestroyIndexWithoutIndexName(){
    setupSystem();
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.CREATE_INDEX);
    csb.addOption(CliStrings.CREATE_INDEX__NAME, indexName);
    csb.addOption(CliStrings.CREATE_INDEX__EXPRESSION, "key");
    csb.addOption(CliStrings.CREATE_INDEX__REGION, "/StocksParReg");
    csb.addOption(CliStrings.CREATE_INDEX__TYPE, "hash");
    String commandString = csb.toString();
    CommandResult commandResult = executeCommand(commandString);
    String resultAsString = commandResultToString(commandResult);
    assertEquals(commandResult.getStatus(), Status.OK);
    
    csb = new CommandStringBuilder(CliStrings.LIST_INDEX);
    commandString = csb.toString();
    commandResult = executeCommand(commandString);
    resultAsString = commandResultToString(commandResult);
    assertEquals(Status.OK, commandResult.getStatus());
    assertEquals(true, resultAsString.contains(indexName));
    assertEquals(true, resultAsString.contains(VM1Name));
    
    csb = new CommandStringBuilder(CliStrings.DESTROY_INDEX);
    csb.addOption(CliStrings.DESTROY_INDEX__GROUP, group1);
    commandString = csb.toString();
    commandResult = executeCommand(commandString);
    resultAsString = commandResultToString(commandResult);
    assertEquals(Status.OK, commandResult.getStatus());
    
    csb = new CommandStringBuilder(CliStrings.LIST_INDEX);
    commandString = csb.toString();
    commandResult = executeCommand(commandString);
    resultAsString = commandResultToString(commandResult);
    assertEquals(Status.OK, commandResult.getStatus());
    
    csb = new CommandStringBuilder(CliStrings.CREATE_INDEX);
    csb.addOption(CliStrings.CREATE_INDEX__NAME, indexName);
    csb.addOption(CliStrings.CREATE_INDEX__EXPRESSION, "key");
    csb.addOption(CliStrings.CREATE_INDEX__REGION, "/StocksParReg");
    csb.addOption(CliStrings.CREATE_INDEX__TYPE, "hash");
    commandString = csb.toString();
    commandResult = executeCommand(commandString);
    resultAsString = commandResultToString(commandResult);
    assertEquals(Status.OK, commandResult.getStatus());
    
    csb = new CommandStringBuilder(CliStrings.DESTROY_INDEX);
    csb.addOption(CliStrings.DESTROY_INDEX__REGION, "StocksParReg");
    commandString = csb.toString();
    commandResult = executeCommand(commandString);
    resultAsString = commandResultToString(commandResult);
    assertEquals(Status.OK, commandResult.getStatus());
    
    csb = new CommandStringBuilder(CliStrings.LIST_INDEX);
    commandString = csb.toString();
    commandResult = executeCommand(commandString);
    resultAsString = commandResultToString(commandResult);
    assertEquals(Status.OK, commandResult.getStatus());
    assertTrue(resultAsString.contains(CliStrings.LIST_INDEX__INDEXES_NOT_FOUND_MESSAGE));
  }
  
  private void writeToLog(String text, String resultAsString) {
    getLogWriter().info(testName + "\n");
    getLogWriter().info(resultAsString);
  }
  private void setupSystem() {
    disconnectAllFromDS();
    createDefaultSetup(null);
    final String parRegName = "StocksParReg";
    
    final VM manager = Host.getHost(0).getVM(0);
    final VM vm1 = Host.getHost(0).getVM(1);
    
    manager.invoke(new SerializableCallable() {
      public Object call() {
        Region parReg = createParReg(parRegName, getCache(), String.class, Stock.class);
        parReg.put("VMW", new Stock("VMW", 98));
        return parReg.put("APPL",new Stock("APPL", 600));
      }
    });
    
    vm1.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Properties props = new Properties();
        props.setProperty(DistributionConfig.NAME_NAME, VM1Name);
        props.setProperty(DistributionConfig.GROUPS_NAME, group1);
        getSystem(props);
        Region parReg = createParReg(parRegName, getCache(), String.class, Stock.class);
        parReg.put("MSFT", new Stock("MSFT", 27));
        return parReg.put("GOOG",new Stock("GOOG", 540));
      }
    });
  }
  
  private void setupSystemPersist() {
    disconnectAllFromDS();
    createDefaultSetup(null);
    final String parRegName = "StocksParReg";
    
    final VM manager = Host.getHost(0).getVM(0);
    final VM vm1 = Host.getHost(0).getVM(1);
    
    manager.invoke(new SerializableCallable() {
      public Object call() {
        Region parReg = createParReg(parRegName, getCache(), String.class, Stock.class);
        parReg.put("VMW", new Stock("VMW", 98));
        Region parRegPers = createParRegWithPersistence(parRegPersName, "testCreateIndexDiskstore1", "testCreateIndexDiskDir1");
        Region repRegPers = createRepRegWithPersistence(repRegPersName, "testCreateIndexDiskstore1", "testCreateIndexDiskDir1");
        return parReg.put("APPL",new Stock("APPL", 600));
      }
    });
    
    vm1.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Properties props = new Properties();
        props.setProperty(DistributionConfig.NAME_NAME, VM1Name);
        props.setProperty(DistributionConfig.GROUPS_NAME, group1);
        getSystem(props);
        Region parReg = createParReg(parRegName, getCache(), String.class, Stock.class);
        parReg.put("MSFT", new Stock("MSFT", 27));
        Region parRegPers = createParRegWithPersistence(parRegPersName, "testCreateIndexDiskstore2", "testCreateIndexDiskDir2");
        Region repRegPers = createRepRegWithPersistence(repRegPersName, "testCreateIndexDiskstore2", "testCreateIndexDiskDir2");
        return parReg.put("GOOG",new Stock("GOOG", 540));
      }
    });
  }
  @Override
  public void tearDown2() throws Exception {
    super.tearDown2();
  }
}
