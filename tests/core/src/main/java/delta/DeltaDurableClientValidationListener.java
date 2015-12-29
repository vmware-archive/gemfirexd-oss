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
package delta;

import hct.ha.Feeder;
import hydra.DistributedSystemHelper;
import hydra.Log;
import hydra.blackboard.SharedMap;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import util.TestException;
import util.TestHelper;

import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.distributed.DistributedSystem;

/**
 * Delta propagation in durable client; contains the logic for number of times
 * full object recieved and for the fromDeltaCounter on key
 * 
 * @author aingle
 * @since 6.1
 */
public class DeltaDurableClientValidationListener extends CacheListenerAdapter
    implements Serializable {

  /*
   * load values from shared map
   */
  static final Map latestValues = new HashMap();

  static Map durableKeyMap = new HashMap(); 
  /**
   * This method performs the following tasks:<br>
   * 1)Increment the count of afterCreate in Validator<br>
   * 2)Verify that value received in the event is not null as Feeder always does
   * create() on a key with a Long value. <br>
   * 3)If the value's integer attribute is not zero( meaning the first create()
   * call on this key), validate that the new value's integer attribute is
   * exactly one more than the previous value.<br>
   * 4)Update the latest value map for the event key with the new value
   * received.
   * 
   * @param event -
   *          the entry event received in callback
   */
  public void afterCreate(EntryEvent event) {
    synchronized (latestValues) {
      Validator.createCount++;
      String key = (String)event.getKey();
      if (key.equals(Feeder.LAST_KEY)) {
        DeltaPropagation.lastKeyReceived = true;
        Log.getLogWriter().info("'last_key' received at client");
        return;
      }

      if (event.getNewValue() instanceof Long) // for mix feed mode
        return;

      DeltaTestObj value = (DeltaTestObj)event.getNewValue();
      if (value == null) {
        throwException("value in afterCreate cannot be null: key = " + key);
        return;
      }

      if (value.getIntVar() != 0 && value.getFromDeltaCounter() != 0) {
        validateIncrementByOne(key, value);
      }
      Log.getLogWriter().info(
          "create entry in Map key " + key + " : value : " + value
              + " : puting into map : " + value.getIntVar());
      latestValues.put(key, value.getIntVar());
      CounterHolder chold = (CounterHolder)durableKeyMap.get(key + "#" + DeltaPropagation.VmDurableId);
      if(chold==null){
        chold = new CounterHolder(0, 0);
        Log.getLogWriter().info("create chold is null for key " + key + "#" + DeltaPropagation.VmDurableId);
      }else{
        chold.fullObjectCounter++;
        Log.getLogWriter().info("create chold is not null for key " + key + "#" + DeltaPropagation.VmDurableId);
      }
      durableKeyMap.put(
          key + "#" + DeltaPropagation.VmDurableId, chold);
    }
  }

  /**
   * This method performs the following tasks:<br>
   * 1)Increment the count of afterUpdate in Validator<br>
   * 2)Verify that value received in the event is not null as Feeder always
   * generates update on a key with a value's integer attribute is one more than
   * the previous one on this key.<br>
   * 3)If the oldValue received in the event is null,it implies that Feeder
   * invalidated this key in previous operation. In this case, validate that the
   * newValue received in the event is one more than that stored in the local
   * <code>latestValues</code> map<br>
   * 4)If the oldValue received in the event is not null, validate that the
   * newValue's integer attribute received in the event is one more than the
   * oldValue's integer attribute received in the event.<br>
   * 5)Update the latest value map for the event key with the new value
   * received.
   * 
   * @param event -
   *          the entry event received in callback
   */
  public void afterUpdate(EntryEvent event) {
    synchronized (latestValues) {
      Validator.updateCount++;

      if (event.getNewValue() instanceof Long) // for mix feed mode
        return;

      String key = (String)event.getKey();
      DeltaTestObj newValue = (DeltaTestObj)event.getNewValue();
      DeltaTestObj oldValue = (DeltaTestObj)event.getOldValue();
      if (newValue == null) {
        throwException("newValue in afterUpdate cannot be null: key = " + key);
        return;
      }
      CounterHolder chold = (CounterHolder)durableKeyMap.get(key + "#" + DeltaPropagation.VmDurableId);

      if (newValue.getFromDeltaCounter() == 0) {
        if (chold == null) {
          chold = new CounterHolder(0, 1);
        }
        else {
          chold.fullObjectCounter++;
        }
      }
      else if (oldValue == null) {
        validateIncrementByOne(key, newValue);
        chold.fromDeltaCounter++;
      }
      else {
        long diff = newValue.getIntVar()
            - ((Integer)latestValues.get(key)).intValue();
        if (diff != 1) {
          throwException("difference expected in newValue and oldValue is less than  1, but was not for key = "
              + key
              + " & newVal = "
              + newValue
              + " oldValue = "
              + ((Integer)latestValues.get(key)).intValue());
          return;
        }
        chold.fromDeltaCounter++;
      }
      Log.getLogWriter().info(
          "update entry in Map key " + key + " : value : " + newValue
              + " : puting into map : " + newValue.getIntVar());
      latestValues.put(key, newValue.getIntVar());
      Log.getLogWriter().info(
          "update entry in shared Map key " + key + "#"
              + DeltaPropagation.VmDurableId + " : value : " + chold);
      durableKeyMap.put(
          key + "#" + DeltaPropagation.VmDurableId, chold);
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
  public void afterInvalidate(EntryEvent event) {
    synchronized (latestValues) {
      Validator.invalidateCount++;

      if (event.getOldValue() instanceof Long) // for mix feed mode
        return;

      String key = (String)event.getKey();
      DeltaTestObj oldValue = (DeltaTestObj)event.getOldValue();

      if (oldValue == null) {
        throwException("oldValue in afterInvalidate cannot be null : key = "
            + key);
        return;
      }
      Log.getLogWriter().info(
          "invalidate entry in Map key " + key + " : value : " + oldValue
              + " : puting into map : " + oldValue.getIntVar());
      latestValues.put(key, oldValue.getIntVar());
      Log.getLogWriter().info(
          "invalidate entry in shared Map key " + key + "#"
              + DeltaPropagation.VmDurableId + " : value : null");
      durableKeyMap.put(
          key + "#" + DeltaPropagation.VmDurableId, null);
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
  public void afterDestroy(EntryEvent event) {
    synchronized (latestValues) {
      Validator.destroyCount++;

      if (event.getOldValue() instanceof Long) // for mix feed mode
        return;

      String key = (String)event.getKey();
      DeltaTestObj value = (DeltaTestObj)event.getOldValue();

      if (value != null) {
        latestValues.put(key, value.getIntVar());
        Log.getLogWriter().info(
            "key " + key + " received at client old value : " + value);
      }
      Log.getLogWriter().info(
          "destroy entry in shared Map key " + key + "#"
              + DeltaPropagation.VmDurableId);
      durableKeyMap.remove(
          key + "#" + DeltaPropagation.VmDurableId);
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
  private void validateIncrementByOne(String key, DeltaTestObj newValue) {
    Integer oldValue = (Integer)latestValues.get(key);
    if (oldValue == null) {
      throwException("oldValue in latestValues cannot be null: key = " + key
          + " & newVal = " + newValue);
      return;
    }
    long diff = newValue.getIntVar() - oldValue.intValue();
    if (diff != 1) {
      throwException("difference expected in newValue and oldValue is 1, but is was "
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
  public static void throwException(String reason) {
    ArrayList reasonArray = null;
    DistributedSystem ds = DistributedSystemHelper.getDistributedSystem();
    String clientName = "CLIENT_" + ds.getName();
    SharedMap shMap = DeltaPropagationBB.getBB().getSharedMap();
    if (!shMap.containsKey(clientName)) {
      reasonArray = new ArrayList();
    }
    else {
      reasonArray = (ArrayList)shMap.get(clientName);
    }
    reasonArray.add(reason);
    shMap.put(clientName, reasonArray);
    DeltaPropagationBB.getBB().getSharedCounters().increment(
        DeltaPropagationBB.NUM_EXCEPTION);
    Log.getLogWriter().info(
        "Exception : " + TestHelper.getStackTrace(new TestException(reason)));

  }

  /*
   * used to hold counter for a key -- fromDeltaCounter + fullObjectCounter =
   * toDeltaCounter
   */
  class CounterHolder implements Serializable {

    private long fromDeltaCounter;

    private long fullObjectCounter;

    public CounterHolder(long fmc, long foc) {
      this.fromDeltaCounter = fmc;
      this.fullObjectCounter = foc;
    }

    public long getFromDeltaCounter() {
      return fromDeltaCounter;
    }

    public long getFullObjectCounter() {
      return fullObjectCounter;
    }

    public void setFromDeltaCounter(long fromDeltaCounter) {
      this.fromDeltaCounter = fromDeltaCounter;
    }

    public void setFullObjectCounter(long fullObjectCounter) {
      this.fullObjectCounter = fullObjectCounter;
    }

    public long getTotalSum() {
      return (this.fromDeltaCounter + this.fullObjectCounter);
    }

    public String toString() {
      return "CounterHolder [fromDeltaCounter " + this.fromDeltaCounter
          + " fullObjectCounter " + this.fullObjectCounter + "]";
    }
  }
}
