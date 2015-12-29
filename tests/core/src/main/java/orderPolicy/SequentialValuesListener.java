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
 * CacheListener which verifies that events (for the same key) have values which increment by 1
 * using only the events (oldValue/newValue).
 *
 * Note: HA tests allow duplicates (for replayed events).
 * 
 * @author Lynn Hughes-Godfrey
 * @since 6.6.2
 * 
 */
public class SequentialValuesListener extends util.AbstractListener implements CacheListener, Declarable {

  /**
   * Handle afterCreate event
   * 
   * @param event -
   *          the entry event received in callback
   */
  public void afterCreate(EntryEvent event) {
    logCall("afterCreate", event);

    Object key = event.getKey();
    ValueHolder vh = (ValueHolder)event.getNewValue();
    Integer newValue = vh.getModVal();
    if (newValue == null) {
      throwException("value in afterCreate cannot be null: key = " + key);
      return;
    }
    if (newValue.intValue() != 0) {   // first value will be 0
      throwException("Expected value of 1 for afterCreate with " + key + ", but found newValue = " + newValue); 
    }
  }

  /**
   * Handle afterUpdate event
   * 
   * @param event -
   *          the entry event received in callback
   */
  public void afterUpdate(EntryEvent event) {
    logCall("afterUpdate", event);

    Object key = event.getKey();
    ValueHolder newVH = (ValueHolder)event.getNewValue();
    ValueHolder oldVH = (ValueHolder)event.getOldValue();
    Integer newValue = newVH.getModVal();
    Integer oldValue = oldVH.getModVal();
  
    if (newValue == null) {
      throwException("newValue in afterUpdate cannot be null: key = " + key);
      return;
    }

    if (oldValue == null) {
      throwException("oldValue in afterUpdate cannot be null: key = " + key);
      return;
    }
  
    long diff = newValue.intValue() - oldValue.intValue();
    if (diff > 1) {  // missed an event
      throwException("Expected incremental increase between oldValue and newValue for key = " + key + " and oldValue = " + oldValue + ", but found newValue = " + newValue + " (difference = " + diff + ")");
      return;
    } 
  }

  /**
   * Handle afterInvalidate event
   *
   * @param event - the entry event received in callback
   */
  public void afterInvalidate(EntryEvent event) {
    logCall("afterInvalidate", event);
  }

  /**
   * Handle destroyEvent
   * 
   * @param event - the entry event received in callback
   */
  public void afterDestroy(EntryEvent event) {
    logCall("afterDestroy", event);
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

    hydra.blackboard.SharedMap aMap = WANBlackboard.getInstance().getSharedMap();
    aMap.put(TestHelper.EVENT_ERROR_KEY, errStr + " " + TestHelper.getStackTrace());
    Log.getLogWriter().info(errStr);
    throw new TestException(errStr);
  }

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
