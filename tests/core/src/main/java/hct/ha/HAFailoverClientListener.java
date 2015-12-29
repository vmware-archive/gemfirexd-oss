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
package hct.ha;


import hydra.DistributedSystemHelper;
import hydra.Log;
import hydra.blackboard.SharedMap;

import java.util.*;

import util.TestException;
import util.TestHelper;

import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.distributed.DistributedSystem;

/**
 * This class is a <code>CacheListener</code> implementation attached to the
 * cache-clients for test validations. The callback methods validate the order
 * of the data coming via the events.This listener is used for failover test
 * cases
 * 
 * @author Dinesh Patel
 * @author Mitul Bid
 * 
 */
public class HAFailoverClientListener extends CacheListenerAdapter
{
  /**
   * A local map to store the last received values for the keys in the callback
   * events.
   */
  private static final Map latestValues = new HashMap();
  /**
   * This method performs the following tasks:<br>
   * 1)Increment the count of afterCreate in Validator<br>
   * 2)Verify that value received in the event is not null as Feeder always does
   * create() on a key with a Long value. <br>
   * 3)If the value is not zero( meaning the first create() call on this key),
   * validate that the new value is exactly one more than the previous value.<br>
   * 4)Update the latest value map for the event key with the new value
   * received.
   * 
   * @param event -
   *          the entry event received in callback
   */
  public void afterCreate(EntryEvent event)
  {
    synchronized (latestValues) {
    Validator.createCount++;
    String key = (String)event.getKey();
    if (key.equals(Feeder.LAST_KEY)) {
      HAClientQueue.lastKeyReceived = true;
      Log.getLogWriter().info("'last_key' received at client");
    }
    Long value = (Long)event.getNewValue();
    if (value == null) {
      throwException("value in afterCreate cannot be null: key = " + key);
      return;
    }
    if (value.longValue() != 0) {
      validateNewValue(key, value);
    }
    latestValues.put(key, value);
    }
  }

  /**
   * This method performs the following tasks:<br>
   * 1)Increment the count of afterUpdate in Validator<br>
   * 2)Verify that value received in the event is not null as Feeder always
   * generates update on a key with a Long value one more than the previous one
   * on this key.<br>
   * 3)If the oldValue received in the event is null,it implies that Feeder
   * invalidated this key in previous operation. In this case, validate that the
   * newValue received in the event is one more than that stored in the local
   * <code>latestValues</code> map<br>
   * 4)If the oldValue received in the event is not null, validate that the
   * newValue received in the event is one more than the oldValue received in
   * the event.<br>
   * 4)Update the latest value map for the event key with the new value
   * received.
   * 
   * @param event -
   *          the entry event received in callback
   */
  public void afterUpdate(EntryEvent event)
  {
    synchronized (latestValues) {
    Validator.updateCount++;
    String key = (String)event.getKey();
    Long newValue = (Long)event.getNewValue();
    Long oldValue = (Long)event.getOldValue();

    if (newValue == null) {
      throwException("newValue in afterUpdate cannot be null: key = " + key);
      return;
    }

    if (oldValue == null) {
      validateNewValue(key, newValue);
    }
    else {
      long diff = newValue.longValue() - oldValue.longValue();
      if (diff != 1) {
        throwException("difference expected in newValue and oldValue is less than  1, but was not for key = "
            + key + " & newVal = " + newValue + " oldValue = " + oldValue);
        return;
      }
    }
    latestValues.put(key, newValue);
    }
  }

  /**
   * This method performs the following tasks:<br>
   * 1)Increment the count of afterInvalidate in Validator<br>
   * 2)Verify that oldValue received in the event is not null as Feeder does
   * invalidate() on only those keys which have non-null values.<br>
   * 3)Update the latest value map for the event key with the oldValue received.
   * 
   * @param event -
   *          the entry event received in callback
   */
  public void afterInvalidate(EntryEvent event)
  {
    synchronized (latestValues) {
    Validator.invalidateCount++;
    String key = (String)event.getKey();
    Long oldValue = (Long)event.getOldValue();
    if (oldValue != null) {
      latestValues.put(key, oldValue);
    }
    }
  }

  /**
   * This method performs the following tasks:<br>
   * 1)Increment the count of afterDestroy in Validator<br>
   * 2)If the oldValue in the event is not null, update the latest value map for
   * the event key with the oldValue received. If the value is null, it implies
   * that Feeder did a invalidate() in its previous operation on this key and
   * hence no need to update the local <code>latestValues</code> map.
   * 
   * @param event -
   *          the entry event received in callback
   */
  public void afterDestroy(EntryEvent event)
  {
    synchronized (latestValues) {
    Validator.destroyCount++;
    String key = (String)event.getKey();
    Long value = (Long)event.getOldValue();
    if (value != null) {
      latestValues.put(key, value);
    }
    }
  }

  /**
   * This method verifies that the given <code>newValue</code>for the key is
   * exactly one more than that in the <code>latestValues</code> map. If the
   * oldValue in <code>latestValues</code> map is null or the above validation
   * fails, {@link #throwException(String)} is called to update the blackboard.
   * 
   * @param key -
   *          key of the callback event
   * @param newValue -
   *          key of the callback event
   */
  private void validateNewValue(String key, Long newValue)
  {
    Long oldValue = (Long)latestValues.get(key);
    if (oldValue == null) {
      throwException("oldValue in latestValues cannot be null: key = " + key
          + " & newVal = " + newValue);
      return;
    }
    long diff = newValue.longValue() - oldValue.longValue();
    // difference will be less than or equal to one. If not then throw an
    // exception
    if (diff > 1) {
      throwException("difference expected in newValue and oldValue to be less than 1, but is was "
          + diff + " for key = " + key + " & newVal = " + newValue);
      return;
    }
  }

  /**
   * This method increments the number of exceptions occured counter in the
   * blackboard and put the reason string against the exception number in the
   * shared map.
   * 
   * @param reason -
   *          string description of the cause of the exception.
   */
  public static void throwException(String reason)
  {
	  	/* here we are taking the exception reason string from each client who is caling this method and ptting 
	  	 * this reason string in  the arraylist of corresponding client.
	  	 * Now put this arraylist of reasons as a value against the client name as key in a shared map.
	  	 * 
	  	 */
	  	ArrayList reasonArray = null;
		DistributedSystem ds = DistributedSystemHelper.getDistributedSystem();
		String clientName = "CLIENT_"+ds.getName();
		SharedMap shMap = HAClientQueueBB.getBB().getSharedMap();
		if(!shMap.containsKey(clientName)){
			reasonArray = new ArrayList();
		}else{
			reasonArray = (ArrayList) shMap.get(clientName);
		}
		reasonArray.add(reason);
		shMap.put(clientName, reasonArray);
		long counter = HAClientQueueBB.getBB().getSharedCounters().incrementAndRead(HAClientQueueBB.NUM_EXCEPTION);
		Log.getLogWriter().info("Exception : " + counter+ "\n"+TestHelper.getStackTrace(new TestException(reason)));
  }
}
