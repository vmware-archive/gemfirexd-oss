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

package hct;

import java.util.Iterator;
import hydra.*;
import hydra.blackboard.SharedCounters;
import util.*;

import com.gemstone.gemfire.cache.*;

/**
 * A version of the <code>SerialBridgeNotify</code> that performs operations
 * serially.  
 * @author Girish Thombare, Suyog Bhokare
 * 
 */
public class SerialClientQueue extends SerialBridgeNotify {

	//	 Instance Variables
	protected static boolean isConflationEnabled;

	protected static SerialClientQueue bridgeClientConflation;

	private static String CLIENT_NAME = "Client_" + ProcessMgr.getProcessId();

	/**
	 * Function to populate region with know keys and values. If conflation is enabled 
	 * than donot populate the region.
	 */
	public static void HydraTask_populateRegion() {
		isConflationEnabled = TestConfig.tab().booleanAt(
				hct.ClientQueuePrms.conflationEnabled, false);
		if (isConflationEnabled) {
			return;
		}
		BridgeNotify.HydraTask_populateRegion();
	}

	/**
	 * This function performs various operation like create,put,destroy ect.
	 * If conflation is enabled then do only batch put on single known key.
	 */
	public static void HydraTask_doEntryOperations() {
		isConflationEnabled = TestConfig.tab().booleanAt(
				hct.ClientQueuePrms.conflationEnabled, false);
		if (!isConflationEnabled)
			BridgeNotify.HydraTask_doEntryOperations();
		else {
			bridgeClientConflation = new SerialClientQueue();
			Region rootRegion = CacheHelper.getCache().getRegion(
					BridgeNotify.REGION_NAME);
			bridgeClientConflation.addObjectForConflation(rootRegion);
		}
	}

	/**
	 * This function used for the data validation. All the cients should have same value 
	 * for the known key after batch put.
	 * @param name
	 */
	private void checkDataCounters(String name) {
		String anObj = (String) BridgeNotifyBB.getBB().getSharedMap().get(
				"Conflation_key");
		String key = getConflationKey(name);
		Iterator itr = EventCountersBB.getBB().getSharedMap().getMap().keySet()
				.iterator();
		while (itr.hasNext()) {
			Object clientKey = itr.next();
			if (clientKey instanceof String) {
				if (((String) clientKey).indexOf("Client_") != -1) {
					String actualObj = (String) EventCountersBB.getBB()
							.getSharedMap().get(clientKey);
					Log.getLogWriter().info(
							"Excepted Object : " + anObj + "  Actual Object : "
									+ actualObj);
					if (!anObj.equals(actualObj)) {
						String errStr = "Data Validation Failed";
						throw new TestException(errStr.toString());
					}
				}
			}
		}
	}

	/**
	 * This function performs batch put on the known keys. After batch put function wait 
	 * for time which is configurable with parameter timeToWaitForConflation.
	 * @param aRegion
	 */
	protected void addObjectForConflation(Region aRegion) {

		BridgeNotifyBB.incrementCounter("BridgeNotifyBB.NUM_CREATE",
				BridgeNotifyBB.NUM_CREATE);
		String name = NameFactory.getNextPositiveObjectName();
		try {
			aRegion.put(name, "value");
			BridgeNotifyBB.getBB().getSharedMap().put("Conflation_key",
					"dummyVlaue");

		} catch (Exception e) {
			throw new TestException(TestHelper.getStackTrace(e));
		}

		long timeToWaitForConflation = TestConfig.tab().longAt(
				hct.ClientQueuePrms.timeToWaitForConflation, 60000);
		String anObj = null;
		for (int i = 0; i < 100; i++) {
			//anObj = getObjectToAdd(name);
			anObj = "val_" + i;
			try {
				aRegion.put(name, anObj);
				BridgeNotifyBB.getBB().getSharedMap().put("Conflation_key",
						anObj);
				BridgeNotifyBB.incrementCounter("BridgeNotifyBB.NUM_UPDATE",
						BridgeNotifyBB.NUM_UPDATE);

			} catch (Exception e) {
				throw new TestException(TestHelper.getStackTrace());

			}
		}
		try {
			Thread.sleep(timeToWaitForConflation);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		checkDataCounters(name);
		checkUpdateCounters();
	}

	/**
	 * This function performs events validation. Due to conflation, number of events 
	 * received should be less than that of expected evnts.
	 */
private void checkUpdateCounters()
	{
		SharedCounters counters = BridgeNotifyBB.getBB().getSharedCounters();
		long numUpdate = counters.read(BridgeNotifyBB.NUM_UPDATE);
		int numVmsWithList = getNumVMsWithListeners();
		Log.getLogWriter().info("num VMs with listener installed: " + numVmsWithList);
		long expectedEvents = (numUpdate * numVmsWithList);
		long actualEvents = EventCountersBB.getBB().getSharedCounters().read(EventCountersBB.getBB().getSharedCounter("numAfterUpdateEvents_isNotExp"));
		
		Log.getLogWriter().info("ActualEvents : " + actualEvents + "  ExpectedEvents : " +  expectedEvents);
		if(actualEvents >= expectedEvents)
		{
			String errStr = "Event Validation Failed";
			throw new TestException(errStr.toString());
		}
		
	}
	private String getConflationKey(String key) {
		return CLIENT_NAME + "_" + key;
	}
}
