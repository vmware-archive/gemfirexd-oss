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
import hydra.GemFireDescription;
import hydra.Log;
import hydra.TestConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import util.TestException;

import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.internal.SocketCreator;
import com.gemstone.gemfire.internal.cache.LocalRegionHelper;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;

import delta.DeltaPropagationBB;

public class Validator
{
  public static long createCount = 0;

  public static long updateCount = 0;

  public static long invalidateCount = 0;

  public static long destroyCount = 0;

  /**
   * Validate the number of invalidates received. It should be greater (because
   * of duplicates) or equal. If number of invalidates received is lesser than
   * an exception is thrown
   * Since there exists only single ConnectionProxy in the VM the method below
   * will work. Otherwise it will be better to use ConnectionProxy to get
   * the stats data for targetted invalidates received by a ConnectionProxy
   * 
   */
  public static void validateNumberOfInvalidatesReceived()

  {
    HAClientQueue.waitForLastKeyReceivedAtClient();
    Log.getLogWriter().info("going to validate...");
   /* Statistics stats[] = InternalDistributedSystem.getAnyInstance()
        .findStatisticsByType(ConnectionProxyImpl.cacheClientUpdaterStats);
    long invalidatesRecordedByStats = 0L;
    for (int i = 0; i < stats.length; i++) {
      invalidatesRecordedByStats = invalidatesRecordedByStats
          + stats[i].getLong(stats[i].nameToDescriptor("invalidates"));
    }*/
    
    long invalidatesRecordedByStats =HAClientQueue.mypool.getInvalidateCount();

    long putsByFeeder = HAClientQueueBB.getBB().getSharedCounters()
        .read(HAClientQueueBB.NUM_UPDATE);

    if (putsByFeeder != invalidatesRecordedByStats) {
      throw new TestException("number of puts ( " + putsByFeeder
          + ") is greater than the number of invalidates ( "
          + invalidatesRecordedByStats + ")  received ");
    }
    Log.getLogWriter().info("successfully validated...");
  }

  /**
   * Validate the number of invalidates received. It should be equal. If number
   * of invalidates received is not equal, an exception is thrown.
   * 
   */
  public static void validateNumberOfInvalidatesReceivedWithoutFailover()

  {
    HAClientQueue.waitForLastKeyReceivedAtClient();
    /*Log.getLogWriter().info("going to validate...");
    Statistics stats[] = InternalDistributedSystem.getAnyInstance()
        .findStatisticsByType(ConnectionProxyImpl.cacheClientUpdaterStats);
    long invalidatesRecordedByStats = 0L;
    for (int i = 0; i < stats.length; i++) {
      invalidatesRecordedByStats = invalidatesRecordedByStats
          + stats[i].getLong(stats[i].nameToDescriptor("invalidates"));
    }*/
    long invalidatesRecordedByStats =HAClientQueue.mypool.getInvalidateCount();

    long putsByFeeder = HAClientQueueBB.getBB().getSharedCounters()
        .read(HAClientQueueBB.NUM_UPDATE);

    if (putsByFeeder != invalidatesRecordedByStats) {
      throw new TestException("number of puts ( " + putsByFeeder
          + ") is not equal to the number of invalidates ( "
          + invalidatesRecordedByStats + ")  received ");
    }
    Log.getLogWriter().info("successfully validated...");
  }

  /**
   * validation for the hydra without failover and conflation enabled
   * notifybySubscription = false validation scheme is : number of invalidates
   * received at the client should be equal to number of puts - number of events
   * conflated
   */

  public static void validateInvalidatesConflationEnabledWithNoFailover()
  {
    //Not pausing here now as already done is previous closetask 'putHAStatsInBlackboard'
    //pauseBeforeValidation();
    PoolImpl proxy = HAClientQueue.mypool;
    ClientProxyMembershipID cpm =  proxy.getProxyID();    
    String proxyIdStr = cpm.toString();    
    Log.getLogWriter().info("Proxy String : " + proxyIdStr);

    HashMap mp = (HashMap)HAClientQueueBB.getBB().getSharedMap()
        .get(proxyIdStr);
    
    Long numOfPuts = (Long)mp.get("eventsPut");
    Long numOfConflated = (Long)mp.get("eventsConflated");
    Long numOfMissRemovals = (Long)mp.get("numVoidRemovals");
    Long numOfRemovals = (Long)mp.get("eventsRemoved");
    
    /*Statistics stats[] = InternalDistributedSystem.getAnyInstance()
        .findStatisticsByType(ConnectionProxyImpl.cacheClientUpdaterStats);
    long invalidatesRecordedByStats = 0L;

    for (int i = 0; i < stats.length; i++) {
      invalidatesRecordedByStats = invalidatesRecordedByStats
          + stats[i].getLong(stats[i].nameToDescriptor("invalidates"));
    }*/
    
    long invalidatesRecordedByStats =HAClientQueue.mypool.getInvalidateCount();
    Log.getLogWriter().info("statsMap : "+mp.toString()+", invalidates recvd="+invalidatesRecordedByStats);
    Log.getLogWriter().info("value of numOfPuts : " + numOfPuts);
    Log.getLogWriter().info("value of numOfConflated : " + numOfConflated);
    Log.getLogWriter().info("value of numOfmiss : " + numOfMissRemovals);
    Log.getLogWriter()
        .info("no of invalidates : " + invalidatesRecordedByStats);
    Log.getLogWriter().info(
        "value of numRemoved " + (Long)mp.get("eventsRemoved"));

    if (numOfPuts.intValue() < invalidatesRecordedByStats) {
      throw new TestException(
          "No of invalidates received by client VM is less than no. of puts on feeder : expected="
              + numOfPuts + " ; received=" + invalidatesRecordedByStats);
    }
    
    if ((numOfRemovals.intValue() + numOfMissRemovals.intValue()) != invalidatesRecordedByStats) {
      throw new TestException(
          "No of events sent to client by server does not match the number received by client: expected="
              + (numOfRemovals.intValue() + numOfMissRemovals.intValue())
              + " ; received=" + invalidatesRecordedByStats);
    }
  }

  /**
   * validation for the hydra test with failover and conflation enabled.
   * notifybySubscription = false. validation scheme is : Number of invalidates
   * receives at the client side should be greater than number of puts
   */

  public static void validateInvalidatesConflationEnabledWithFailover()
  {
    HAClientQueue.waitForLastKeyReceivedAtClient();

    long numOfPuts = HAClientQueueBB.getBB().getSharedCounters().read(
        HAClientQueueBB.NUM_UPDATE);
    /*Statistics stats[] = InternalDistributedSystem.getAnyInstance()
        .findStatisticsByType(ConnectionProxyImpl.cacheClientUpdaterStats);
    long invalidatesRecordedByStats = 0L;

    for (int i = 0; i < stats.length; i++) {
      invalidatesRecordedByStats = invalidatesRecordedByStats
          + stats[i].getLong(stats[i].nameToDescriptor("invalidates"));
      Log.getLogWriter().info(
          "loop no " + i + " : " + invalidatesRecordedByStats);
    }*/
    
    long invalidatesRecordedByStats =HAClientQueue.mypool.getInvalidateCount();

    Log.getLogWriter().info("value of numOfPuts : " + numOfPuts);
    Log.getLogWriter()
        .info("no of invalidates : " + invalidatesRecordedByStats);

    if (numOfPuts < invalidatesRecordedByStats) {
      throw new TestException(
          "No of invalidates received by client VM is more than no. of puts on feeder");
    }
  }

  /**
   * Checks the blackboard for exception count. If the count is greater than
   * zero, all the corresponding exception msgs from the sharedmap are appended
   * in a string and thrown in a TestException. This is used to catch exceptions
   * occuring in threads other than hydra threads ( For eg. CacheClientUpdater
   * thread).
   * 
   */
  public static String checkBlackBoardForException()
  {
    long exceptionCount = HAClientQueueBB.getBB().getSharedCounters().read(
        HAClientQueueBB.NUM_EXCEPTION);
    if (exceptionCount > 0) {
			StringBuffer reason = new StringBuffer();
			reason.append("\n");
			int reasonCount = 0;
			Map reasonMap = HAClientQueueBB.getBB().getSharedMap().getMap();
			Set reasonEntrySet = reasonMap.entrySet();
			Iterator reasonEntrySetIteraor = reasonEntrySet.iterator();
			while(reasonEntrySetIteraor.hasNext()){
				Entry reasonEntry = (Entry) reasonEntrySetIteraor.next();
				Object reasonKey = reasonEntry.getKey();
				if(reasonKey instanceof String) {
					if(((String) reasonKey).startsWith("CLIENT_")){
						ArrayList reasonArray = (ArrayList) reasonEntry.getValue();
						reason.append(reasonKey + "\n");
						for(int i = 0 ;i <reasonArray.size();i++){			
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
  public static void validateOpCountsForNoFailover()
  {
    long createCountFromBB = HAClientQueueBB.getBB().getSharedCounters().read(
        HAClientQueueBB.NUM_CREATE);
    long updateCountFromBB = HAClientQueueBB.getBB().getSharedCounters().read(
        HAClientQueueBB.NUM_UPDATE);
    long invalidateCountFromBB = HAClientQueueBB.getBB().getSharedCounters()
        .read(HAClientQueueBB.NUM_INVALIDATE);
    long destroyCountFromBB = HAClientQueueBB.getBB().getSharedCounters().read(
        HAClientQueueBB.NUM_DESTROY);
    
    //temporaryily print
    
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
  }

  /**
   * Validates the various entry operations count in case of failover. In this
   * case, number of events added by Feeder should be less than or equal to the
   * events received by client ( since client can get duplicates during
   * failovers.
   * 
   */
  public static void validateOpCountsForFailover()
  {
    long createCountFromBB = HAClientQueueBB.getBB().getSharedCounters().read(
        HAClientQueueBB.NUM_CREATE);
    long updateCountFromBB = HAClientQueueBB.getBB().getSharedCounters().read(
        HAClientQueueBB.NUM_UPDATE);
    long invalidateCountFromBB = HAClientQueueBB.getBB().getSharedCounters()
        .read(HAClientQueueBB.NUM_INVALIDATE);
    long destroyCountFromBB = HAClientQueueBB.getBB().getSharedCounters().read(
        HAClientQueueBB.NUM_DESTROY);

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
  }

  /**
   * Validates the various entry operations count in case of failover. In this
   * case, number of events added by Feeder should be less than or equal to the
   * events received by client ( since client can get duplicates during
   * failovers.
   * 
   */
  public static void validateOpCountsNoFailoverConflationEnabled()
  {
    long createCountFromBB = HAClientQueueBB.getBB().getSharedCounters().read(
        HAClientQueueBB.NUM_CREATE);
    long updateCountFromBB = HAClientQueueBB.getBB().getSharedCounters().read(
        HAClientQueueBB.NUM_UPDATE);
    long invalidateCountFromBB = HAClientQueueBB.getBB().getSharedCounters()
        .read(HAClientQueueBB.NUM_INVALIDATE);
    long destroyCountFromBB = HAClientQueueBB.getBB().getSharedCounters().read(
        HAClientQueueBB.NUM_DESTROY);

    HAClientQueueBB.getBB().printSharedCounters(); 
    
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
    boolean validationFailed = false;
    StringBuffer failureMsg = new StringBuffer();

    if (diff < 0) {
      validationFailed = true;
      failureMsg.append(diff + " creates not received by client" + " createCountFromBB = " + createCountFromBB + " createCount" + createCount +" \n");
    }

    diff = updateCountFromBB - updateCount;
    if (diff < 0) {
      validationFailed = true;
      failureMsg.append(diff + " updates not received by client \n");
    }

    diff = invalidateCountFromBB - invalidateCount;
    if (diff < 0) {
      validationFailed = true;
      failureMsg.append(diff + " invalidates not received by client \n");
    }

    diff = destroyCountFromBB - destroyCount;
    if (diff < 0) {
      validationFailed = true;
      failureMsg.append(diff + " destroys not received by client \n");
    }
    if (validationFailed) {
      throw new TestException(failureMsg.toString());
    }
  }

  /**
   * Waits for some time before going for validations.
   * 
   */
  public static void pauseBeforeValidation()
  {
    Log.getLogWriter().info("going to sleep before validating...");
    try {
      Thread.sleep(180000);
    }
    catch (Exception e) {
      // ignore
    }
  }

 public static void checkForConflation()
 {

    PoolImpl proxy = HAClientQueue.mypool;
    ClientProxyMembershipID cpm =  proxy.getProxyID();    
    String proxyIdStr = cpm.toString();
    Log.getLogWriter().info("Proxy String : " + proxyIdStr);
    if (DistributedSystemHelper.getGemFireDescription()
        .getEnableNetworkPartitionDetection()) {
      proxyIdStr = proxyIdStr.substring(proxyIdStr.lastIndexOf("(") + 1,
          proxyIdStr.indexOf(":"));
    }
    HashMap mp = (HashMap)HAClientQueueBB.getBB().getSharedMap()
            .get(proxyIdStr);
    Long numOfPuts = (Long)mp.get("eventsPut");
    Long numOfConflated = (Long)mp.get("eventsConflated");
    Long numOfMissRemovals = (Long)mp.get("numVoidRemovals");
    
    Log.getLogWriter().info("value of numOfConflated : " + numOfConflated);

    long totalConflation  = HAClientQueueBB.getBB().getSharedCounters().read(
        HAClientQueueBB.NUM_GLOBAL_CONFLATE); 
    Log.getLogWriter().info("Total Conflation : " + totalConflation);
      if (totalConflation == 0)
      {      throw new TestException(
          "No conflation done - Test Issue - needs tuning "); }
      
      
 }
 



  /**
   * The following task is used for client Q conflation tests. Similar to
   * checkForConflation task but the task fails if the individual client events
   * are not conflated. (checkForConflation validation pass if there is atleast
   * one event conflated in the total test.
   */
  public static void checkClientConflation() {

    PoolImpl pool = HAClientQueue.mypool;
    ClientProxyMembershipID cpm = pool.getProxyID();
    String proxyIdStr = cpm.toString();
    Log.getLogWriter().info("Proxy String : " + proxyIdStr);

    HashMap mp = (HashMap)HAClientQueueBB.getBB().getSharedMap()
        .get(proxyIdStr);

    Long numOfPuts = (Long)mp.get("eventsPut");
    Long numOfConflated = (Long)mp.get("eventsConflated");
    Long numOfMissRemovals = (Long)mp.get("numVoidRemovals");

    Log.getLogWriter().info("value of numOfConflated : " + numOfConflated);

    long totalConflation = HAClientQueueBB.getBB().getSharedCounters().read(
        HAClientQueueBB.NUM_GLOBAL_CONFLATE);
    Log.getLogWriter().info("Total Conflation : " + totalConflation);
    if (numOfConflated.equals(new Long(0))) {
      throw new TestException("No conflation done - Test Issue - needs tuning ");
    }
  }

  /**
   * This task is for the client Q conflation tests. This is to verify that no
   * conflation happened for this client events.
   */
  public static void verifyNoConflation() {
    PoolImpl pool = HAClientQueue.mypool;
    ClientProxyMembershipID cpm = pool.getProxyID();
    String proxyIdStr = cpm.toString();
    Log.getLogWriter().info("Proxy String : " + proxyIdStr);

    HashMap mp = (HashMap)HAClientQueueBB.getBB().getSharedMap()
        .get(proxyIdStr);

    Long numOfPuts = (Long)mp.get("eventsPut");
    Long numOfConflated = (Long)mp.get("eventsConflated");
    Long numOfMissRemovals = (Long)mp.get("numVoidRemovals");

    Log.getLogWriter().info("value of numOfConflated : " + numOfConflated);

    long totalConflation = HAClientQueueBB.getBB().getSharedCounters().read(
        HAClientQueueBB.NUM_GLOBAL_CONFLATE);
    Log.getLogWriter().info("Total Conflation : " + totalConflation);
    if (!numOfConflated.equals(new Long(0))) {
      throw new TestException("Expected no conflation to happen but has "
          + numOfConflated + " events");
    }

  }
  
  /**
   * This method is an CLOSETASK and does the following : <br>
   * toggle ha overflow status flag in shared black board
   * @since 5.7
   */
  public static void toggleHAOverflowFlag() {
    LocalRegionHelper.isHAOverflowFeaturedUsedInPrimaryPutOnShareBB();
  }
  
  /**
   * This method is an CLOSETASK and does the following : <br>
   * use ha overflow status flag in share black board for <br>
   * inference on HAOverFlow use in test
   * 
   * @throws TestException
   * @since 5.7
   */  
  public static void checkHAOverFlowUsedOnPrimary() {
    int status = (int)HAClientQueueBB.getBB().getSharedCounters().read(
        HAClientQueueBB.HA_OVERFLOW_STATUS);
    // validate Overflow happened
    if (status == 0) {
      throw new TestException(
          "Test issue : Test need tuning - no overflow happened");
    }
  }
  
}