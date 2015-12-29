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
package orderPolicy;

import hydra.*;
import hydra.blackboard.*;

import java.util.*;

import util.TestException;
import util.TestHelper;
import util.ValueHolder;
import wan.*;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.DistributedSystem;

/**
 * CacheListener which maintains a local hashMap of callbackArgument/value pairs and 
 * verifies that events (for the same callbackArgument) have values which increment by 1.
 * Note that the keys in this local HashMap are callbackArguments (vmId + tid) and the values are the 
 * operationNumber by that threadId.
 * 
 * @author Lynn Hughes-Godfrey
 * @since 6.6.2
 * 
 */
public class SequentialValuesForTidListener extends util.AbstractListener implements CacheListener, Declarable {
  /**
   * A local map to store callback (vmid+tid)/value (opNum per tid) pairs based on incoming events
   */
  private static final Map latestValues = new HashMap();

  /**
   * Handle afterCreate event
   * 
   * @param event -
   *          the entry event received in callback
   */
  public void afterCreate(EntryEvent event) {
    synchronized (latestValues) {
      Object key = event.getKey();
      ValueHolder vh = (ValueHolder)event.getNewValue();
      Integer value = (Integer)vh.modVal;
      String callback = (String)event.getCallbackArgument();
      Log.getLogWriter().info("tidListener.afterCreate(): " + key + ":" + value + ":" + callback);
      if (value == null) {
        throwException("value in afterCreate cannot be null: key = " + key);
        return;
      }
      if (value.intValue() != 0) {   // first value will be 0
        validateIncreasingOpNumForTid(callback, value);
      }
      latestValues.put(callback, value);
    }
  }

  /**
   * Handle afterUpdate event
   * 
   * @param event -
   *          the entry event received in callback
   */
  public void afterUpdate(EntryEvent event) {
    synchronized (latestValues) {
      Object key = event.getKey();
      ValueHolder vh = (ValueHolder)event.getNewValue();
      Integer newValue = (Integer)vh.modVal;

      vh = (ValueHolder)event.getOldValue();
      Integer oldValue = (Integer)vh.modVal;
      String callback = (String)event.getCallbackArgument();
      Log.getLogWriter().info("tidListener.afterUpdate(): " + key + ":" + newValue + ":" + callback);
  
      if (newValue == null) {
        throwException("newValue in afterUpdate cannot be null: key = " + key);
        return;
      }
  
      validateIncreasingOpNumForTid(callback, newValue);
      latestValues.put(callback, newValue);
    }
  }

  /**
   * Handle afterInvalidate event
   *
   * @param event - the entry event received in callback
   */
  public void afterInvalidate(EntryEvent event) {
    synchronized (latestValues) {
      Object key = event.getKey();
      String callback = (String)event.getCallbackArgument();
      Log.getLogWriter().info("tidListener.afterInvalidate(): " + key + ":" + null + ":" + callback);
      ValueHolder vh = (ValueHolder)event.getOldValue();
      Integer oldValue = (Integer)vh.modVal;
      if (oldValue != null) {
        latestValues.put(callback, oldValue);
      }
    }
  }

  /**
   * Handle destroyEvent
   * 
   * @param event - the entry event received in callback
   */
  public void afterDestroy(EntryEvent event) {
    synchronized (latestValues) {
      Object key = event.getKey();
      String callback = (String)event.getCallbackArgument();
      Log.getLogWriter().info("tidListener.afterDestroy(): " + key + ":" + null + ":" + callback);
      ValueHolder vh = (ValueHolder)event.getOldValue();
      Integer value = (Integer)vh.modVal;
      if (value != null) {
        latestValues.put(callback, value);
      }
    }
  }

  /**
   * This method verifies that the given <code>newValue</code>for the key (callbackArg) 
   * is exactly one more than that in the <code>latestValues</code> map. If the
   * oldValue in <code>latestValues</code> map is null or the above validation
   * fails, {@link #throwException(String)} is called to update the blackboard.
   * 
   * @param key - key of the callback event
   * @param newValue - key of the callback event
   */
  private void validateIncreasingOpNumForTid(Object key, Integer newValue) {
    Integer oldValue = (Integer)latestValues.get(key);
    if (oldValue == null) {
//      throwException("oldValue in latestValues cannot be null: key = " + key + " and newVal = " + newValue);
      return;
    }

    long diff = newValue.intValue() - oldValue.intValue();
    // difference will be less than or equal to one. If not then throw an exception
    if (diff > 1) {
      throwException("Expected incremental increase between event old value and new value (representing the operation counter for thread = " + key + ").  previousOperationCounter = " + oldValue + ", newOperationCounter = " + newValue + " (difference = " + diff + ")");
      return;
    }
  }

  /**
   * Utility method to write an Exception string to the Event Blackboard and
   * to also throw an exception containing the same string.
   *
   * @param errStr String to log, post to EventBB and throw
   * @throws TestException containing the passed in String
   *
   * @see util.TestHelper.checkForEventError
   */
  protected void throwException(String errStr) {
    StringBuffer qualifiedErrStr = new StringBuffer();
    qualifiedErrStr.append("Exception reported in vm_" + RemoteTestModule.getMyVmid() + "_" + RemoteTestModule.getMyClientName() + "\n");
    qualifiedErrStr.append(errStr);
    errStr = qualifiedErrStr.toString();
    Blackboard bb = WANBlackboard.getInstance();
    hydra.blackboard.SharedMap aMap = bb.getSharedMap();
    // capture the 1st Exception only
    bb.getSharedLock().lock();
    if (aMap.get(TestHelper.EVENT_ERROR_KEY) == null) {
       aMap.put(TestHelper.EVENT_ERROR_KEY, errStr + " " + TestHelper.getStackTrace());
    }
    bb.getSharedLock().unlock();
    Log.getLogWriter().info(errStr);
    //throw new TestException(errStr);
  }

  // not used
  public void afterRegionLive(RegionEvent event) {
    logCall("afterRegionLive", event);
  }

  public void afterRegionDestroy(RegionEvent event) {
    logCall("afterRegionDestroy", event);
  }

  public void afterRegionInvalidate(RegionEvent event) {
    logCall("afterRegionInvalidate", event);
  }

  public void afterRegionClear(RegionEvent event) {
    logCall("afterRegionClear", event);
  }

  public void afterRegionCreate(RegionEvent event) {
    logCall("afterRegionCreate", event);
  }

  public void close() {
    logCall("close", null);
  }

  public void init(java.util.Properties prop) {
    logCall("init(Properties)", null);
  }
}
