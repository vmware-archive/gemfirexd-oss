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
package management.operations.ops;

import static management.util.HydraUtil.logInfo;
import hydra.DistributedConnectionMgr;
import hydra.Log;
import hydra.ProcessMgr;
import hydra.TestConfig;

import java.util.Iterator;
import java.util.Set;

import management.operations.OperationPrms;
import management.operations.OperationsBlackboard;
import management.operations.RegionKeyValueConfig;
import management.operations.events.EntryOperationEvents;
import management.util.HydraUtil;
import management.util.RegionUtil;
import pdx.compat.Operations;
import util.RandomValues;
import util.TestException;
import util.TestHelper;

import com.gemstone.gemfire.cache.CacheLoaderException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.TimeoutException;

/**
 * This class repeats functionality of EventTest.java but written for suiting
 * M&M test needs. Test behaviour is controlled by OperationsPrms following
 * properties entryOperations lowerThreshold lowerThresholdOperations
 * upperThreshold upperThresholdOperations objectType : default is valueHolder
 * 
 * Use this class for performing Entry operations on given Region :
 * doEntryOperation()
 * 
 * Second mode also added where you can manually perform operations the way
 * you want like, it will not be random as returned by ONEOF hydra operator
 * Thus it allows complete control over entry operations. Entry values like
 * object put or removed is determined by RegionKeyValueConfig. You can
 * implement your implementation of it to have complete control over what
 * values are put, updated or removed in region.
 * 
 * TODO : 
 *   Logging all actions in HydraLog and Blackboard - DONE 
 *   Surround or region operations using try-catch block and assert unexpected exceptions - DONE 
 *   Support capturing event to random recorder instead of Blackboard so
 *      that it can be validated right-away using Expectations - DONE 
 *   Add validations - DONE
 *   Add manual mode - DONE
 *   Add key generation strategy. - DONE 
 *   Further separate logic where EntryOperation might touch keys outside 
 *      configured RegionKeyValueConfig : TBD
 * 
 * 
 * @author tushark
 * 
 */
public class EntryOperations {

  /**
   * Copied from EventTest
   */
  static protected final int ADD_OPERATION = 1;
  static protected final int UPDATE_OPERATION = 2;
  static protected final int INVALIDATE_OPERATION = 3;
  static protected final int DESTROY_OPERATION = 4;
  static protected final int READ_OPERATION = 5;
  static protected final int LOCAL_INVALIDATE_OPERATION = 6;
  static protected final int LOCAL_DESTROY_OPERATION = 7;
  static protected final int REGION_CLOSE_OPERATION = 8;
  static protected final int CLEAR_OPERATION = 9;
  static protected final int PUT_IF_ABSENT_OPERATION = 10;
  static protected final int REMOVE_OPERATION = 11;
  static protected final int REPLACE_OPERATION = 12;

  protected static final String createCallbackPrefix = "Create event originated in pid ";
  protected static final String updateCallbackPrefix = "Update event originated in pid ";
  protected static final String invalidateCallbackPrefix = "Invalidate event originated in pid ";
  protected static final String destroyCallbackPrefix = "Destroy event originated in pid ";
  protected static final String regionInvalidateCallbackPrefix = "Region invalidate event originated in pid ";
  protected static final String regionDestroyCallbackPrefix = "Region destroy event originated in pid ";

  protected static final String memberIdString = " memberId=";
  protected static final String VmIDStr = "VmId_";

  // Value of EventPrms.lowerThreshold
  protected int lowerThreshold;

  // Value of EventPrms.upperThreshold
  protected int upperThreshold;

  protected RandomValues randomValues = null;
  protected EntryOperationEvents operationRecorder = null;

  protected String opPrefix = "EntryOperations:";

  protected Region region;

  protected RegionKeyValueConfig keyValueConfig;   

  /*-
   * public EntryOperations(){ lowerThreshold =
   * TestConfig.tab().intAt(OperationPrms.lowerThreshold, -1); upperThreshold =
   * TestConfig.tab().intAt(OperationPrms.upperThreshold, Integer.MAX_VALUE);
   * randomValues = new RandomValues(); operationRecorder =
   * OperationsBlackboard.getBB(); }
   */

  public void setKeyValueConfig(RegionKeyValueConfig keyValueConfig) {
    this.keyValueConfig = keyValueConfig;
  }

  public EntryOperations(Region region) {
    lowerThreshold = TestConfig.tab().intAt(OperationPrms.lowerThreshold, -1);
    upperThreshold = TestConfig.tab().intAt(OperationPrms.upperThreshold,
        Integer.MAX_VALUE);
    randomValues = new RandomValues();
    operationRecorder = OperationsBlackboard.getBB();
    this.region = region;
  }

  /*-
   * public EntryOperations(Operations op){ lowerThreshold =
   * TestConfig.tab().intAt(OperationPrms.lowerThreshold, -1); upperThreshold =
   * TestConfig.tab().intAt(OperationPrms.upperThreshold, Integer.MAX_VALUE);
   * randomValues = new RandomValues(); this.operationRecorder = op; }
   */

  public EntryOperations(Region r, EntryOperationEvents op) {
    lowerThreshold = TestConfig.tab().intAt(OperationPrms.lowerThreshold, -1);
    upperThreshold = TestConfig.tab().intAt(OperationPrms.upperThreshold,
        Integer.MAX_VALUE);
    randomValues = new RandomValues();
    this.region = r;
    this.operationRecorder = op;
  }

  public int getOperationMaintainSize() {
    int whichOp = getOperation(OperationPrms.entryOperations);
    int size = region.size();
    if (size >= upperThreshold) {
      whichOp = getOperation(OperationPrms.upperThresholdOperations);
    } else if (size <= lowerThreshold) {
      whichOp = getOperation(OperationPrms.lowerThresholdOperations);
    }
    return whichOp;
  }

  public int getOperation(Long whichPrm) {
    int op = 0;
    String operation = TestConfig.tab().stringAt(whichPrm);
    if ("add".equals(operation))
      op = ADD_OPERATION;
    else if ("update".equals(operation))
      op = UPDATE_OPERATION;
    else if ("invalidate".equals(operation))
      op = INVALIDATE_OPERATION;
    else if ("destroy".equals(operation))
      op = DESTROY_OPERATION;
    else if ("read".equals(operation))
      op = READ_OPERATION;
    else if ("localInvalidate".equals(operation))
      op = LOCAL_INVALIDATE_OPERATION;
    else if ("localDestroy".equals(operation))
      op = LOCAL_DESTROY_OPERATION;
    /*
     * else if (operation.equals("close")) op = REGION_CLOSE_OPERATION; else if
     * ("clear".equals(operation)) op = CLEAR_OPERATION;
     */
    else if ("putIfAbsent".equals(operation))
      op = PUT_IF_ABSENT_OPERATION;
    else if ("remove".equals(operation))
      op = REMOVE_OPERATION;
    else if ("replace".equals(operation))
      op = REPLACE_OPERATION;
    else
      throw new TestException("Unknown entry operation: " + operation);
    return op;
  }
  
  public void add(){
    int op = ADD_OPERATION;
    try {        
        if (opAllowed(op)){
          String callback = getCallback();
          addObject(callback);
        }else
          throw new TestException("op " + opCodeToOpName(op) + " not allowed for region " + region.getFullPath());
      } catch (Exception e) {
        handleException("Exception during " + opCodeToOpName(op), e, region);
        /*-
        if (HydraUtil.isConcurrentTest())
          logInfo("Region " + region.getFullPath()
              + " is destroyed. Expected. Continuing the test ");
        else
          throw new TestException("RegionDestroyedException ", e);
      } catch (Exception e) {
        throw new TestException("Unknown Exception ", e);*/
      }
  }
  
 

  public void putIfAbsent(){
    int op = PUT_IF_ABSENT_OPERATION;
    try {      
      if (opAllowed(op)){
        String callback = getCallback();
        putIfAbsent(callback);
      }else
        throw new TestException("op " + opCodeToOpName(op) + " not allowed for region " + region.getFullPath());
    } catch (Exception e) {
      handleException("Exception during " + opCodeToOpName(op), e, region);
    }    
  }
  
  public void update(){
    int op = UPDATE_OPERATION;
    try {      
      if (opAllowed(op)){
        String callback = getCallback();
        updateObject(callback);
      }else
        throw new TestException("op " + opCodeToOpName(op) + " not allowed for region " + region.getFullPath());
    } catch (Exception e) {
      handleException("Exception during " + opCodeToOpName(op), e, region);
    }
  }
  
  public void replace(){
    int op = REPLACE_OPERATION;
    try {      
      if (opAllowed(op)){
        String callback = getCallback();
        replace(callback);
      }else
        throw new TestException("op " + opCodeToOpName(op) + " not allowed for region " + region.getFullPath());
    } catch (Exception e) {
      handleException("Exception during " + opCodeToOpName(op), e, region);
    }    
  }
  
  public void read(){
    int op = READ_OPERATION;
    try {      
      if (opAllowed(op)){
        String callback = getCallback();
        readObject(callback);
      }else
        throw new TestException("op " + opCodeToOpName(op) + " not allowed for region " + region.getFullPath());
    } catch (Exception e) {
      handleException("Exception during " + opCodeToOpName(op), e, region);
    }   
  }
  
  public void invalidateLocal(){
    int op = LOCAL_INVALIDATE_OPERATION;
    try {      
      if (opAllowed(op)){
        String callback = getCallback();
        invalidateLocalObject(callback);
      }else
        throw new TestException("op " + opCodeToOpName(op) + " not allowed for region " + region.getFullPath());
    } catch (Exception e) {
      handleException("Exception during " + opCodeToOpName(op), e, region);
    }    
  }
  
  public void invalidate(){
    int op = INVALIDATE_OPERATION;
    try {      
      if (opAllowed(op)){
        String callback = getCallback();
        invalidateLocalObject(callback);
      }else
        throw new TestException("op " + opCodeToOpName(op) + " not allowed for region " + region.getFullPath());
    } catch (Exception e) {
      handleException("Exception during " + opCodeToOpName(op), e, region);
    }   
  }
  
  public void destroyLocal(){
    int op = LOCAL_DESTROY_OPERATION;
    try {      
      if (opAllowed(op)){
        String callback = getCallback();
        destroyLocalObject(callback);
      }else
        throw new TestException("op " + opCodeToOpName(op) + " not allowed for region " + region.getFullPath());
    } catch (Exception e) {
      handleException("Exception during " + opCodeToOpName(op), e, region);
    }    
  }
  
  public void destroy(){
    int op = DESTROY_OPERATION;
    try {      
      if (opAllowed(op)){
        String callback = getCallback();
        destroyObject(callback);
      }else
        throw new TestException("op " + opCodeToOpName(op) + " not allowed for region " + region.getFullPath());
    } catch (Exception e) {
      handleException("Exception during " + opCodeToOpName(op), e, region);
    }    
  }
  
  public void remove(){
    int op = REMOVE_OPERATION;
    try {      
      if (opAllowed(op)){
        String callback = getCallback();
        remove(callback);
      }else
        throw new TestException("op " + opCodeToOpName(op) + " not allowed for region " + region.getFullPath());
    } catch (Exception e) {
      handleException("Exception during " + opCodeToOpName(op), e, region);
    }    
  }

  protected void addObject(Object callback) {
    Object value;
    String key;
    // key = NameFactory.getNextPositiveObjectName();
    key = (String) keyValueConfig.getNextKey();
    logInfo(opPrefix + " Adding object in region named : " + region.getName()
        + " key=" + key + " callback " + callback);
    value = getObjectToAdd(key);
    region.create(key, value, callback);
    RegionUtil.checkRegionKeyExistsValueExists(region, key, value);
    operationRecorder.entryAdded(region.getName(), key, value);
  }

  private void putIfAbsent(Object callback) {
    Object value;
    String key;
    key = getKeyWithProbability_25();
    logInfo(opPrefix + " putting object IfAbsent in region named : "
        + region.getName() + " key=" + key + " callback " + callback);
    value = getObjectToAdd(key);
    region.putIfAbsent(key, value);
    RegionUtil.checkRegionKeyExists(region, key);
    operationRecorder.entryPutIfAbsent(region.getName(), key, value);
  }

  private void updateObject(Object callback) {
    Object value;
    String key;
    Set set = region.keys();
    Iterator iter = set.iterator();
    if (!iter.hasNext()) {
      Log.getLogWriter().info("updateObject: No names in region");
      return;
    }
    key = (String) iter.next();
    logInfo(opPrefix + " Updating object in region named : " + region.getName()
        + " key=" + key + " callback " + callback);
    value = getUpdateObject(key);
    Object oldValue = region.put(key, value, callback);
    RegionUtil.checkRegionKeyExistsValueExists(region, key, value);
    operationRecorder.entryUpdated(region.getName(), key, value);
  }

  private void replace(Object callback) {
    Object value;
    String key;
    // key = NameFactory.getNextPositiveObjectName();
    key = (String) keyValueConfig.getNextKey();
    value = getUpdateObject(key);
    logInfo(opPrefix + " Replacing object in region named : "
        + region.getName() + " key=" + key + " callback " + callback);
    Object replaced = region.replace(key, value);
    if (replaced != null)
      RegionUtil.checkRegionKeyExistsValueExists(region, key, value);
    operationRecorder.entryReplaced(region.getName(), key, value);
  }

  private void readObject(Object callback) {
    Object name = getExistingKey();
    if (name == null) {
      Log.getLogWriter().info("readObject: No Key in region");
      return;
    }
    logInfo(opPrefix + " Reading object in region named : " + region.getName()
        + " key=" + name + " callback " + callback);
    try {
      Object object = region.get(name);
      Log.getLogWriter().info(
          "readObject: got value for name " + name + ": "
              + TestHelper.toString(object));
      operationRecorder.entryRead(region.getName(), (String) name);
    } catch (CacheLoaderException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } catch (TimeoutException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
  }

  private void invalidateLocalObject(Object callback) {
    Object key;
    Object name = getExistingKey();
    if (name == null) {
      Log.getLogWriter().info("invalidateLocal: No Key in region");
      return;
    }
    logInfo(opPrefix + " invalidateLocal object in region named : "
        + region.getName() + " key=" + name + " callback " + callback);
    try {
      region.localInvalidate(name);
      RegionUtil.checkRegionValueDoesNotExists(region, name);
      operationRecorder.entryInvalidatedLocal(region.getName(), (String) name);
    } catch (com.gemstone.gemfire.cache.EntryNotFoundException e) {
      Log.getLogWriter().info(
          "Caught " + e
              + " (expected with concurrent execution); continuing with test");
    }
  }

  private void invalidateObject(Object callback) {
    Object key = getExistingKey();
    if (key == null) {
      Log.getLogWriter().info("invalidate: No Key in region");
      return;
    }
    logInfo(opPrefix + " invalidate object in region named : "
        + region.getName() + " key=" + key + " callback " + callback);
    try {
      region.invalidate(key);
      RegionUtil.checkRegionValueDoesNotExists(region, key);
      operationRecorder.entryInvalidated(region.getName(), (String) key);
    } catch (com.gemstone.gemfire.cache.EntryNotFoundException e) {
      Log.getLogWriter().info(
          "Caught " + e
              + " (expected with concurrent execution); continuing with test");
    }
  }

  private void destroyLocalObject(Object callback) {
    Object key;
    key = getExistingKey();
    if (key == null) {
      Log.getLogWriter().info("destroyLocal: No Key in region");
      return;
    }
    logInfo(opPrefix + " destroying locally object in region named : "
        + region.getName() + " key=" + key + " callback " + callback);
    try {
      region.localDestroy(key);
      RegionUtil.checkRegionKeyDoesNotExistsValueDoesNotExists(region, key);
      operationRecorder.entryDestroyedLocal(region.getName(), (String) key);
    } catch (com.gemstone.gemfire.cache.EntryNotFoundException e) {
      Log.getLogWriter().info(
          "Caught " + e
              + " (expected with concurrent execution); continuing with test");
    }
  }

  private void destroyObject(Object callback) {
    Object key;
    key = getExistingKey();
    if (key == null) {
      Log.getLogWriter().info("destroy: No Key in region");
      return;
    }
    logInfo(opPrefix + " destroying object in region named : "
        + region.getName() + " key=" + key + " callback " + callback);
    try {
      region.destroy(key);
      RegionUtil.checkRegionKeyDoesNotExistsValueDoesNotExists(region, key);
      operationRecorder.entryDestroyed(region.getName(), (String) key);
    } catch (com.gemstone.gemfire.cache.EntryNotFoundException e) {
      Log.getLogWriter().info(
          "Caught " + e
              + " (expected with concurrent execution); continuing with test");
    }
  }

  private void remove(Object callback) {
    Object key = getExistingKey();
    if (key == null) {
      Log.getLogWriter().info("remove: No Key in region");
      return;
    }
    logInfo(opPrefix + " Removing object in region named : " + region.getName()
        + " key=" + key + " callback " + callback);
    try {
      region.remove(key);
      RegionUtil.checkRegionKeyDoesNotExistsValueDoesNotExists(region, key);
      operationRecorder.entryRemoved(region.getName(), (String) key);
    } catch (com.gemstone.gemfire.cache.EntryNotFoundException e) {
      Log.getLogWriter().info(
          "Caught " + e
              + " (expected with concurrent execution); continuing with test");
    }
  }

  protected void doOp(int op, Object callback) {
    switch (op) {
    case ADD_OPERATION:
      addObject(callback);
      break;
    case INVALIDATE_OPERATION:
      invalidateObject(callback);
      break;
    case DESTROY_OPERATION:
      destroyObject(callback);
      break;
    case UPDATE_OPERATION:
      updateObject(callback);
      break;
    case READ_OPERATION:
      readObject(callback);
      break;
    case LOCAL_INVALIDATE_OPERATION:
      invalidateLocalObject(callback);
      break;
    case LOCAL_DESTROY_OPERATION:
      destroyLocalObject(callback);
      break;
    case PUT_IF_ABSENT_OPERATION:
      putIfAbsent(callback);
      break;
    case REMOVE_OPERATION:
      remove(callback);
      break;
    case REPLACE_OPERATION:
      replace(callback);
      break;
    default: {
      throw new TestException("Unknown operation " + op);
    }
    }
  }
  
  private String getCallback(){
    String memberId = null;
    if (DistributedConnectionMgr.getConnection() != null)
      memberId = DistributedConnectionMgr.getConnection()
          .getDistributedMember().toString();
    String callback = createCallbackPrefix + ProcessMgr.getProcessId()
        + memberIdString + memberId;    
    return callback;
  }

  public void doEntryOperation() {
    String callback = getCallback();
    int op = getOperationMaintainSize();
    try {
      if (opAllowed(op))
        doOp(op, callback);
    } catch (RegionDestroyedException e) {
      if (HydraUtil.isConcurrentTest())
        logInfo("Region " + region.getFullPath()
            + " is destroyed. Expected. Continuing the test ");
      else
        throw new TestException("RegionDestroyedException ", e);
    } catch (Exception e) {
      throw new TestException("Unknown Exception ", e);
    }
  }

  protected boolean opAllowed(int op) {
    RegionAttributes rAttr = region.getAttributes();
    if (rAttr.getDataPolicy().equals(DataPolicy.REPLICATE)
        && (op == LOCAL_INVALIDATE_OPERATION || op == LOCAL_DESTROY_OPERATION))
      return false;
    else
      return true;
  }

  protected Object getObjectToAdd(String name) {
    return keyValueConfig.getValueForKey(name);
    /*
     * String type = OperationPrms.getObjectType();
     * if("valueHolder".equals(type)){ ValueHolder anObj = new ValueHolder(name,
     * randomValues); return anObj; }else{ int index=-1; String array[] =
     * name.split("_"); index = Integer.parseInt(array[1]); Object typedObject =
     * ObjectHelper.createObject(type, index); return typedObject; }
     */
  }

  protected Object getUpdateObject(String name) {

    return keyValueConfig.getUpdatedValueForKey(region, name);
    /*
     * String type = OperationPrms.getObjectType();
     * if("valueHolder".equals(type)){ ValueHolder anObj = null; ValueHolder
     * newObj = null; try { anObj = (ValueHolder)region.get(name); } catch
     * (CacheLoaderException e) { throw new
     * TestException(TestHelper.getStackTrace(e)); } catch (TimeoutException e)
     * { throw new TestException(TestHelper.getStackTrace(e)); } newObj =
     * (ValueHolder) ((anObj == null) ? new ValueHolder(name, randomValues) :
     * anObj.getAlternateValueHolder(randomValues)); return newObj; }else{
     * Object val = ObjectHelper.createObject( type, 0 ); return val; }
     */
  }

  /**
   * Returns key existing in the region for put with probability 0.25 Return new
   * key with p=.75
   */
  public String getKeyWithProbability_25() {
    String name = null;
    int randInt = TestConfig.tab().getRandGen().nextInt(1, 100);
    if (randInt <= 25) {
      Set aSet = region.keySet();
      if (aSet.size() > 0) {
        Iterator it = aSet.iterator();
        if (it.hasNext()) {
          name = (String) it.next();
        }
      }
    }
    if (name == null)
      // name = NameFactory.getNextPositiveObjectName();
      name = (String) keyValueConfig.getNextKey();
    return name;
  }

  public String getExistingKey() {
    return (String) keyValueConfig.getUsedKey(region);
    /*
     * Set set = region.keys(); if (set.size() == 0) {
     * Log.getLogWriter().info("getExistingKey: No names in region"); return
     * null; } long maxNames = NameFactory.getPositiveNameCounter(); if
     * (maxNames <= 0) {
     * Log.getLogWriter().info("getExistingKey: max positive name counter is " +
     * maxNames); return null; } String name =
     * NameFactory.getObjectNameForCounter
     * (TestConfig.tab().getRandGen().nextInt(1, (int)maxNames)); return name;
     */
  }

  private void handleException(String string, Exception e, Region r) {
    if ((HydraUtil.isConcurrentTest() && e instanceof RegionDestroyedException)) {
      logInfo(string
          + " RegionDestroy expected during concurrent test. Contnuing test ");
    }
    if (e instanceof RegionDestroyedException
        && !r.getAttributes().getScope().equals(Scope.LOCAL)) {
      // region destroy is propogated to this jvm
      logInfo(string
          + " Region is already destroyed probably due to remote destroy, continuing with test");
    } else {
      throw new TestException(string, e);
    }
  }
  
  private String opCodeToOpName(int op) {
    switch(op){
      case ADD_OPERATION : return "add";
      case UPDATE_OPERATION : return "update";
      case INVALIDATE_OPERATION : return "invalidate";
      case DESTROY_OPERATION : return "destroy";
      case READ_OPERATION : return "read";
      case LOCAL_INVALIDATE_OPERATION :return "invalidateLocal"; 
      case LOCAL_DESTROY_OPERATION : return "destroyLocal";
      case REGION_CLOSE_OPERATION : return "close";
      case CLEAR_OPERATION : return "clear";
      case PUT_IF_ABSENT_OPERATION : return "putIfAbsent";
      case REMOVE_OPERATION : return "remove";
      case REPLACE_OPERATION : return "replace";
      default : return "unknown";
    }
  }

}
