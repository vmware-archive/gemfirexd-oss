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

import hydra.Log;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import util.TestException;

import com.gemstone.gemfire.DeltaTestImpl;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.internal.cache.LocalRegionHelper;

import delta.DeltaDurableClientValidationListener.CounterHolder;

/**
 * 
 * @author aingle
 * @since 6.1
 */
public class Validator {
  public static long createCount = 0;

  public static long updateCount = 0;

  public static long invalidateCount = 0;

  public static long destroyCount = 0;

  // added to keep track of Delta's
  public static long createDeltaCount = 0;

  public static long updateDeltaCount = 0;

  public static long createNonDeltaCount = 0;

  public static long updateNonDeltaCount = 0;

  public static HashMap keyToMap = new HashMap();

  /**
   * Validates the various entry operations count. In this case, number of non
   * delta events added by Feeder should be equal to the events received by
   * client.
   * 
   * @throws TestException
   * @since 6.1
   */
  public static void validateOpCountsForOld() {
    long createCountFromBB = DeltaPropagationBB.getBB().getSharedCounters()
        .read(DeltaPropagationBB.NUM_NON_DELTA_CREATE);
    long updateCountFromBB = DeltaPropagationBB.getBB().getSharedCounters()
        .read(DeltaPropagationBB.NUM_NON_DELTA_UPDATE);
    long lastKeyCreateCount = DeltaPropagationBB.getBB().getSharedCounters()
        .read(DeltaPropagationBB.NUM_CREATE);

    // temporaryily print

    Log.getLogWriter().info(
        " createCountFromBB " + (createCountFromBB + lastKeyCreateCount)
            + " createCount " + createNonDeltaCount);
    Log.getLogWriter().info(
        " updateCountFromBB " + updateCountFromBB + " updateCount "
            + updateNonDeltaCount);
    long diff = (createCountFromBB + lastKeyCreateCount) - createNonDeltaCount;
    if (diff != 0) {
      throw new TestException(diff + " creates not received by client");
    }

    diff = updateCountFromBB - updateNonDeltaCount;
    if (diff != 0) {
      throw new TestException(diff + " updates not received by client");
    }
  }

  /**
   * Validates the various entry operations count. In this case, number of
   * events includes both Delta and Non Delta added by Feeder should be equal to
   * the events received by client.
   * 
   * @throws TestException
   * @since 6.1
   */
  public static void validateOpCountsForCurr() {
    long updateCountFromBB = DeltaPropagationBB.getBB().getSharedCounters()
        .read(DeltaPropagationBB.NUM_DELTA_UPDATE);
    long updateNonDeltaFromBB = DeltaPropagationBB.getBB().getSharedCounters()
        .read(DeltaPropagationBB.NUM_UPDATE);
    long updateNonDelta = DeltaPropagationBB.getBB().getSharedCounters().read(
        DeltaPropagationBB.NUM_NON_DELTA_UPDATE);

    long createDeltaFromBB = DeltaPropagationBB.getBB().getSharedCounters()
        .read(DeltaPropagationBB.NUM_DELTA_CREATE);
    long create = DeltaPropagationBB.getBB().getSharedCounters().read(
        DeltaPropagationBB.NUM_CREATE);
    long createNonDelta = DeltaPropagationBB.getBB().getSharedCounters().read(
        DeltaPropagationBB.NUM_NON_DELTA_CREATE);

    // temporaryily print
    Log.getLogWriter().info(
        " createCountFromBB " + (create + createDeltaFromBB + createNonDelta)
            + " createCount " + createCount);
    Log.getLogWriter().info(
        " updateCountFromBB "
            + (updateCountFromBB + updateNonDeltaFromBB + updateNonDelta)
            + " updateCount " + updateCount);

    long diff = (create + createDeltaFromBB + createNonDelta) - createCount; // to
                                                                              // negate
                                                                              // last
                                                                              // key
                                                                              // counted
                                                                              // twice
    if (diff != 0) {
      throw new TestException(diff + " creates not received by client");
    }

    diff = (updateCountFromBB + updateNonDeltaFromBB + updateNonDelta)
        - updateCount;
    if (diff != 0) {
      throw new TestException(diff + " updates not received by client");
    }
    validateToAndFromDeltaCount();
  }

  /**
   * Validates the various entry operations count. In this case, number of
   * events includes both Delta and Non Delta added by Feeder should be equal to
   * the events received by cacheless client- number of time constructor called.
   * 
   * @throws TestException
   * @since 6.1
   */
  public static void validateDataReceivedCacheLessClient() {
    long updateCountFromBB = DeltaPropagationBB.getBB().getSharedCounters()
        .read(DeltaPropagationBB.NUM_DELTA_UPDATE);
    long updateNonDeltaFromBB = DeltaPropagationBB.getBB().getSharedCounters()
        .read(DeltaPropagationBB.NUM_UPDATE);
    long updateNonDelta = DeltaPropagationBB.getBB().getSharedCounters().read(
        DeltaPropagationBB.NUM_NON_DELTA_UPDATE);

    long createDeltaFromBB = DeltaPropagationBB.getBB().getSharedCounters()
        .read(DeltaPropagationBB.NUM_DELTA_CREATE);
    long create = DeltaPropagationBB.getBB().getSharedCounters().read(
        DeltaPropagationBB.NUM_CREATE);
    long createNonDelta = DeltaPropagationBB.getBB().getSharedCounters().read(
        DeltaPropagationBB.NUM_NON_DELTA_CREATE);

    Log.getLogWriter().info(
        "createDeltaFromBB " + createDeltaFromBB + "createNonDelta "
            + createNonDelta + "create " + create + "updateCountFromBB "
            + updateCountFromBB + "updateNonDelta " + updateNonDelta
            + "updateNonDeltaFromBB " + updateNonDeltaFromBB + "create+1 "
            + (updateCount + updateCount));

    long diff = (createDeltaFromBB + createNonDelta + create
        + updateCountFromBB + updateNonDelta + updateNonDeltaFromBB)
        - (createCount + updateCount); // to negate last key counted twice

    if (diff != 0) {
      throw new TestException(diff + " creates not received by client");
    }

    /*
     * Log.getLogWriter().info( " objects received " + (DeltaTestObj.getCount()) + "
     * full object request " + CacheClientUpdater.fullValueRequested);
     * 
     * 
     * if (DeltaTestObj.getCount() <= 0) { // fine level loggin and value //
     * part is deserialised just to // tests objects are reaching to // cache
     * less client throw new TestException("Creates/Updates missed by client");
     *  }
     */

  }

  /**
   * validate usuage of Delta feature
   * 
   * @throws TestException
   * @since 6.1
   */
  public static void validateDeltaFeature() {
    if (!DeltaTestImpl.deltaFeatureUsed())
      throw new TestException("Delta feature not used");
  }

  /**
   * validate usuage of from Delta feature
   * 
   * @throws TestException
   * @since 6.1
   */
  public static void validateFromDeltaFeature() {
    if (!DeltaTestImpl.fromDeltaFeatureUsed())
      throw new TestException("FromDelta feature not used");
  }

  /**
   * validate usuage of to Delta feature
   * 
   * @throws TestException
   * @since 6.1
   */
  public static void validateToDeltaFeature() {
    if (!DeltaTestImpl.toDeltaFeatureUsed())
      throw new TestException("ToDelta feature not used");
  }

  /**
   * Checks the blackboard for exception count. If the count is greater than
   * zero, all the corresponding exception msgs from the sharedmap are appended
   * in a string and thrown in a TestException. This is used to catch exceptions
   * occuring in threads other than hydra threads ( For eg. CacheClientUpdater
   * thread).
   * 
   */
  public static String checkBlackBoardForException() {
    long exceptionCount = DeltaPropagationBB.getBB().getSharedCounters().read(
        DeltaPropagationBB.NUM_EXCEPTION);
    if (exceptionCount > 0) {
      StringBuffer reason = new StringBuffer();
      reason.append("\n");
      int reasonCount = 0;
      Map reasonMap = DeltaPropagationBB.getBB().getSharedMap().getMap();
      Set reasonEntrySet = reasonMap.entrySet();
      Iterator reasonEntrySetIteraor = reasonEntrySet.iterator();
      while (reasonEntrySetIteraor.hasNext()) {
        Entry reasonEntry = (Entry)reasonEntrySetIteraor.next();
        Object reasonKey = reasonEntry.getKey();
        if (reasonKey instanceof String) {
          if (((String)reasonKey).startsWith("CLIENT_")) {
            ArrayList reasonArray = (ArrayList)reasonEntry.getValue();
            reason.append(reasonKey + "\n");
            for (int i = 0; i < reasonArray.size(); i++) {
              reasonCount++;
              reason.append("Reason for exception no. " + reasonCount + " : ");
              reason.append(reasonArray.get(i));
              reason.append("\n");
            }
          }
        }
      }
      return reason.toString();
    }
    else
      return null;
  }

  /**
   * Validates the various entry operations count in case of no failover. In
   * this case, number of events added by Feeder should be equal to the events
   * received by client.
   * 
   */
  public static void validateOpCountsForNoFailover() {
    long createCountFromBB = DeltaPropagationBB.getBB().getSharedCounters()
        .read(DeltaPropagationBB.NUM_CREATE);
    long updateCountFromBB = DeltaPropagationBB.getBB().getSharedCounters()
        .read(DeltaPropagationBB.NUM_UPDATE);
    long invalidateCountFromBB = DeltaPropagationBB.getBB().getSharedCounters()
        .read(DeltaPropagationBB.NUM_INVALIDATE);
    long destroyCountFromBB = DeltaPropagationBB.getBB().getSharedCounters()
        .read(DeltaPropagationBB.NUM_DESTROY);

    // temporaryily print

    Log.getLogWriter().info(
        " createCountFromBB " + createCountFromBB + " createCount "
            + createCount);
    Log.getLogWriter().info(
        " updateCountFromBB " + updateCountFromBB + " updateCount "
            + updateCount);
    Log.getLogWriter().info(
        " invalidateCountFromBB " + invalidateCountFromBB + " invalidateCount "
            + invalidateCount);
    Log.getLogWriter().info(
        " destroyCountFromBB " + destroyCountFromBB + " destroyCount "
            + destroyCount);

    long diff = createCountFromBB - createCount;
    if (diff != 0) {
      throw new TestException(diff + " creates not received by client");
    }

    diff = updateCountFromBB - updateCount;
    if (diff != 0) {
      throw new TestException(diff + " updates not received by client");
    }

    diff = invalidateCountFromBB - invalidateCount;
    if (diff != 0) {
      throw new TestException(diff + " invalidates not received by client");
    }

    diff = destroyCountFromBB - destroyCount;
    if (diff != 0) {
      throw new TestException(diff + " destroys not received by client");
    }
    validateToAndFromDeltaCount();
  }

  /**
   * Validates the various entry operations count in case of failover. In this
   * case, number of events added by Feeder should be less than or equal to the
   * events received by client ( since client can get duplicates during
   * failovers.
   * 
   */
  public static void validateOpCountsForFailover() {
    long createCountFromBB = DeltaPropagationBB.getBB().getSharedCounters()
        .read(DeltaPropagationBB.NUM_CREATE);
    long updateCountFromBB = DeltaPropagationBB.getBB().getSharedCounters()
        .read(DeltaPropagationBB.NUM_UPDATE);
    long invalidateCountFromBB = DeltaPropagationBB.getBB().getSharedCounters()
        .read(DeltaPropagationBB.NUM_INVALIDATE);
    long destroyCountFromBB = DeltaPropagationBB.getBB().getSharedCounters()
        .read(DeltaPropagationBB.NUM_DESTROY);

    long diff = createCountFromBB - createCount;
    boolean validationFailed = false;
    StringBuffer failureMsg = new StringBuffer();

    if (diff != 0) {
      validationFailed = true;
      failureMsg.append(diff + " creates not received by client \n");
    }

    diff = updateCountFromBB - updateCount;
    if (diff != 0) {
      validationFailed = true;
      failureMsg.append(diff + " updates not received by client \n");
    }

    diff = invalidateCountFromBB - invalidateCount;
    if (diff != 0) {
      validationFailed = true;
      failureMsg.append(diff + " invalidates not received by client \n");
    }

    diff = destroyCountFromBB - destroyCount;
    if (diff != 0) {
      validationFailed = true;
      failureMsg.append(diff + " destroys not received by client \n");
    }
    if (validationFailed) {
      throw new TestException(failureMsg.toString());
    }
    validateToAndFromDeltaCount();
  }

  public static void validateOpCounts() {
    long createCountFromBB = DeltaPropagationBB.getBB().getSharedCounters()
        .read(DeltaPropagationBB.NUM_CREATE);
    long updateCountFromBB = DeltaPropagationBB.getBB().getSharedCounters()
        .read(DeltaPropagationBB.NUM_UPDATE);
    long invalidateCountFromBB = DeltaPropagationBB.getBB().getSharedCounters()
        .read(DeltaPropagationBB.NUM_INVALIDATE);
    long destroyCountFromBB = DeltaPropagationBB.getBB().getSharedCounters()
        .read(DeltaPropagationBB.NUM_DESTROY);

    long diff = createCountFromBB - createCount;
    boolean validationFailed = false;

    StringBuffer failureMsg = new StringBuffer();

    if (diff != 0) {
      validationFailed = true;
      failureMsg
          .append("Expected : "
              + createCountFromBB
              + "create events from feeder should be received by the client, but actual number of create events received by the client are :"
              + createCount + "\n");
    }

    diff = updateCountFromBB - updateCount;
    if (diff != 0) {
      validationFailed = true;
      failureMsg
          .append("Expected : "
              + updateCountFromBB
              + "update events from feeder should be received by the client, but actual number of update events received by the client are :"
              + updateCount + "\n");
    }

    diff = invalidateCountFromBB - invalidateCount;
    if (diff != 0) {
      validationFailed = true;
      failureMsg
          .append("Expected : "
              + invalidateCountFromBB
              + "invalidate events from feeder should be received by the client, but actual number of invalidate events received by the client are :"
              + invalidateCount + "\n");
    }

    diff = destroyCountFromBB - destroyCount;
    if (diff != 0) {
      validationFailed = true;
      failureMsg
          .append("Expected : "
              + destroyCountFromBB
              + "destroy events from feeder should be received by the client, but actual number of destroy events received by the client are :"
              + destroyCount + "\n");
    }
    if (validationFailed) {
      throw new TestException(failureMsg.toString());
    }
  }

  /*
   * Along with various entry operations count validation, additional validation
   * is done to validate toDelta and fromDelta counter( i.e. number of todelta
   * to be equal to fromDelta).
   * 
   */
  public static void validateOpCountsForDefaultClient() {
    validateOpCounts();
    validateToAndFromDeltaCount();
  }

  /*
   * Along with various entry operations count validation, additional validation
   * is done to validate fromDeltaInvokations counter. It should be zero for the
   * client with data policy empty.
   * 
   */

  public static void validateOpCountForEmptyClient() {
    validateOpCounts();
    if (keyToMap.isEmpty()) {
      throw new TestException("keyToMap : " + keyToMap.isEmpty()
          + " object do not have entries to verify");
    }
    // for number of fromDelta to be equal to zero
    for (Iterator i = keyToMap.keySet().iterator(); i.hasNext();) {
      String key = (String)i.next();
      Long obj1 = (Long)keyToMap.get(key);
      if (obj1 != null) {
        Log.getLogWriter().info("key :" + key + " fromDeltaFromEdge: " + obj1);
        if (obj1.longValue() != 0) {
          throw new TestException(
              "For a client with dataPolicy empty,fromDeltaInvocations expected to be zero, but found fromDeltaInvocations for a key :"
                  + key + " are " + obj1.longValue());
        }
        else {
          Log
              .getLogWriter()
              .info(
                  " Validation succeeded....For a client with dataPolicy empty,fromDeltaInvocations expected to be zero and found fromDeltaInvocations for a key :"
                      + key + " are " + obj1.longValue());
        }
      }
    }
  }

  /*
   * validate toDelta and fromDelta counter
   * 
   */

  public static void validateToAndFromDeltaCount() {
    Map shM = DeltaPropagationBB.getBB().getSharedMap().getMap();

    if (shM.isEmpty() || keyToMap.isEmpty()) {
      throw new TestException("ShM :" + shM.isEmpty() + " keyToMap : "
          + keyToMap.isEmpty() + " object donot have entries to verify");
    }
    // for number of todelta to be equal to fromDelta
    for (Iterator i = keyToMap.keySet().iterator(); i.hasNext();) {
      String key = (String)i.next();
      Long obj1 = (Long)keyToMap.get(key);
      Long obj2 = (Long)shM.get(key);
      if (obj1 != null && obj2 != null) {
        Log.getLogWriter().info(
            "key :" + key + " fromDeltaFromEdge: " + obj1
                + " toDeltaFromFeeder " + obj2);
        if (obj1.longValue() != obj2.longValue())
          throw new TestException("For key :" + key
              + " : difference in toDeltaCounter and fromDeltaCounter is :  "
              + (obj2 - obj1));
      }
    }
  }

  /*
   * validate overflow
   */
  public static void validateOverflow() {
    LocalRegionHelper.isOverflowUsedOnBridge();
  }

  /*
   * validate toDelta and fromDelta counter for durable
   * 
   */
  public static void validateToAndFromDeltaCountForDurable() {
    HashMap keyValue = (HashMap)DeltaDurableClientValidationListener.durableKeyMap;
    Map shM = DeltaPropagationBB.getBB().getSharedMap().getMap();
    if (shM.isEmpty() || keyValue.isEmpty()) {
      throw new TestException("ShM :" + shM.isEmpty() + " keyValue : "
          + keyValue.isEmpty() + " object donot have entries to verify");
    }
    // for number of todelta to be equal to fromDelta
    for (Iterator i = keyValue.keySet().iterator(); i.hasNext();) {
      String key = (String)i.next();
      if (key.contains("#")) {
        String[] keyObj = key.split("#");
        CounterHolder obj2 = (CounterHolder)keyValue.get(key);
        Long obj1 = (Long)shM.get(keyObj[0]);
        if (obj1 != null && obj2 != null) {
          Log.getLogWriter().info(
              "key :" + key
                  + " DurableCounter[fromDeltaCounter, fullObjectCounter] : "
                  + obj2 + " keyObj :" + keyObj[0] + " toDeltaFromFeeder "
                  + obj1);
          if (obj1.longValue() > obj2.getTotalSum()) // acccept duplicates,
            // exception raised if there
            // is loss of event
            throw new TestException(" For key :" + key
                + " : difference in toDeltaCounter and fromDeltaCounter is : "
                + (obj2.getTotalSum() - obj1));
        }
      }
    }
  }
}
