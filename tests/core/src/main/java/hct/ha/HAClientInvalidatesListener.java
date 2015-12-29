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

import java.util.HashMap;
import java.util.Map;

import hydra.Log;

import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;

/**
 * This class is a <code>CacheListener</code> implementation attached to the
 * cache-clients for test validations. It just signals the validations that the
 * last_key expected at the client has been received and validation can proceed
 * further. Also, it validates that the <code>key</code>s from a particular
 * thread have been received in sequence.
 */
public class HAClientInvalidatesListener extends CacheListenerAdapter
{
  /**
   * A local map to store the last received values for the keys in the callback
   * events.
   */
  private final Map latestValues = new HashMap();

  /**
   * A local map to store the keyID that was last updated for the threads in the
   * callback events.
   */
  private final Map latestKeyForAThread = new HashMap();

  public void afterInvalidate(EntryEvent event)
  {
    String key = (String)event.getKey();
    if (key.equals(Feeder.LAST_KEY)) {
      HAClientQueue.lastKeyReceived = true;
      Log.getLogWriter().info("'last_key' received at client");
    }
    
    validateKeySequenceForAThread((String)event.getKey());
  }
  
  /**
   * This method verifies that a particular thread operates on keys in sequence.
   * How: 1) Retrieve the threadID and the newKeyID from the <code>key</code>.<br>
   * e.g. <code>key="Thread_3_2"</code> then <code>threadID=3</code> and
   * <code>sequenceID=2</code><br>
   * 2) If the sequenceID thus retrieved is <b>not</b> zero, validate that this
   * sequenceID is one greater than the previous sequenceID stored in the
   * <code>latestKeyForAThread</code> map, and if the sequenceID is zero, then
   * validate that the previous sequenceID stored in the
   * <code>latestKeyForAThread</code> map is one less than
   * <code>Feeder.PUT_KEY_RANGE</code><br>
   * 3) Update the <code>latestKeyForAThread</code> map for the thread with
   * the sequenceID decoded.<br>
   * 4) If the decoding process throws exception,
   * {@link #throwException(String)} is called to update the blackboard.
   * 
   * @param key -
   *          key of the callback event
   * @return boolean - 
   *          false if validation fails, true otherwise.
   */
  boolean validateKeySequenceForAThread(String key) {
    try {
      if (!Feeder.LAST_KEY.equals(key)) {
        String threadID = key.substring(key.indexOf("_") + 1, key
            .lastIndexOf("_"));
        Long sequenceID = Long.valueOf(key.substring(key.lastIndexOf("_") + 1));

        if (sequenceID.longValue() != 0) {
          if (!validateIncrementByOne(threadID, sequenceID, latestKeyForAThread)) {
            return false;
          }
        }
        else if (latestKeyForAThread.containsKey(threadID)) { // this thread has
          // already been seen
          // by this listener.
          if (!validateIncrementByOne(threadID, new Long(Feeder.PUT_KEY_RANGE),
              latestKeyForAThread)) {
            return false;
          }
        }

        latestKeyForAThread.put(threadID, sequenceID);
      }
    }
    catch (IndexOutOfBoundsException ioobe) {
      throwException("IndexOutOfBoundsException while decoding the key: " + key
          + " to get threadID or sequenceID");
      return false;
    }
    catch (NumberFormatException nfe) {
      throwException("NumberFormatException while decoding the key: " + key
          + " to get threadID or sequenceID");
      return false;
    }
    return true;
  }

  /**
   * This method verifies that the given <code>newValue</code>for the
   * <code>key</code> is exactly one more than that in the <code>map</code>.
   * If the oldValue in <code>map</code> is null or the above validation
   * fails, {@link #throwException(String)} is called to update the blackboard.
   * 
   * @param key -
   *          key of the callback event
   * @param newValue -
   *          new value of the callback event
   * @param map -
   *          map to be searched into for the <code>key</code>
   * @return boolean - 
   *          false if validation fails, true otherwise.
   */
  private boolean validateIncrementByOne(String key, Long newValue, Map map) {
    Long oldValue = (Long)map.get(key);
    if (oldValue == null) {
      throwException("oldValue in map cannot be null: key = " + key
          + " & newVal = " + newValue);
      return false;
    }
    long diff = newValue.longValue() - oldValue.longValue();
    if (diff != 1) {
      throwException("difference expected in newValue and oldValue is 1, but was "
          + diff + " for key = " + key + " & newVal = " + newValue);
      return false;
    }
    return true;
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

    long exceptionNumber = HAClientQueueBB.getBB().getSharedCounters()
        .incrementAndRead(HAClientQueueBB.NUM_EXCEPTION);

    HAClientQueueBB.getBB().getSharedMap().put(new Long(exceptionNumber),
        reason);
  }

}
