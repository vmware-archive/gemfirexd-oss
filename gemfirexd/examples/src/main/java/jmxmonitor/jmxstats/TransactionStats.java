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
package examples.jmxmonitor.jmxstats;

import java.util.LinkedList;
import java.util.Map;

import javax.management.ObjectName;

/*
 * This is the example showing how to implement a class to retrieve the GemFireXD
 * stats related to the transactions, and monitor the transaction rate and 
 * latency
 */
public class TransactionStats extends AbstractGemFireXDStats {
	/* define the variables which MBeans to access for the stats */
	private static final String CACHEPERF_STATS_MBEAN = "CachePerfStats";
	private static final String CACHEPERF_STATS_MBEAN_NAME = "cachePerfStats";
	
	/* define the variables which operations to MBeans in order to obtain the stats */
	private static final String GETSTATS_OPERATION = "manageStat";	
	
	/* define the variables for signatures of the operations */
	private static String[] SIGNATURE = new String[] {"java.lang.String"};

	/*
	 * define the stats (txCommits, txRollbacks, txSuccessLifeTime
	 * txRollbackLifeTime) this class wants to monitor
	 */
	private static final String TX_COMMITS = "txCommits";
	private static final String TX_ROLLBACKS = "txRollbacks";
	private static final String TX_SUCCESS_LIFETIME = "txSuccessLifeTime";
	private static final String TX_ROLLBACK_LIFETIME = "txRollbackLifeTime";
	
	/* the variables storing the current stats values fetched from the MBeans */
	private Integer txCommits = null;
	private Integer txRollbacks = null;
	private Long txSuccessLifeTime = null;
	private Long txRollbackLifeTime = null;

	/* the variables storing the derived stats values we want to monitor */
	private double txRollbackLatency = 0;	
	private double txCommitLatencyThreshold = 0;
	
	/*
	 * the lists storing the history of the stats (txCommits, txRollbacks) and 
	 * the calculated metrics (historyTxCommitLatency, historytxRollbackLatency)
	 * the history sizes could be controlled by the StatsHistorySize specified 
	 * in the configuration file jmx-monitor.xml
	 */
	private LinkedList<Integer> historyTxCommitRate = new LinkedList<Integer>();
	private LinkedList<Integer> historyTxRollbackRate = new LinkedList<Integer>();
	private LinkedList<Double> historyTxCommitLatency = new LinkedList<Double>();
	private LinkedList<Double> historytxRollbackLatency = new LinkedList<Double>();
	
	/* the variables used in the class */
	private boolean gotCurrentStats = false;	
	private double txCommitLatency = 0;

	/*
	 * Implement the method how to retrieved the transaction stats from the
	 * mbeans
	 */
	public void pollStats() {		
		try {
			gotCurrentStats = false;
			// The desired stats are the attributes of the mbean with type
			// of CachePerfStats and name of CachePerfStats. It is only one
			// of the MBeans with type of CachePerfStats which have been
			// retrieved.
			ObjectName[] cachePerfStatsList = (ObjectName[]) mbs.invoke(
					memberObject, GETSTATS_OPERATION,
					new String[] { CACHEPERF_STATS_MBEAN }, SIGNATURE);
			for (ObjectName cachePerfStats : cachePerfStatsList) {
				if (cachePerfStats.getKeyProperty("name").equals(
						CACHEPERF_STATS_MBEAN_NAME)) {
					txCommits = (Integer) mbs.getAttribute(cachePerfStats,
							TX_COMMITS);
					txRollbacks = (Integer) mbs.getAttribute(cachePerfStats,
							TX_ROLLBACKS);
					txSuccessLifeTime = (Long) mbs.getAttribute(cachePerfStats,
							TX_SUCCESS_LIFETIME) / 1000; // convert to ms
					txRollbackLifeTime = (Long) mbs.getAttribute(
							cachePerfStats, TX_ROLLBACK_LIFETIME) / 1000;
					
					// calculate the derived stats txCommitLatency,
					// txRollbackLatency
					txCommitLatency = (txCommits == 0 ? 0 : txSuccessLifeTime
							* 1.0 / txCommits);
					txRollbackLatency = (txRollbacks == 0 ? 0
							: txRollbackLifeTime * 1.0 / txRollbacks);
					gotCurrentStats = true;
					break;
				}
			}	
		} catch (Exception e) {
			System.out.println("Error: Not able to get the statistics. " + e);
		}

		// store the stats in the history. only when all the stats are
		// successfully be retrieved, we store them in the history
		if (gotCurrentStats) {
			historyTxCommitRate.add(txCommits);
			historyTxRollbackRate.add(txRollbacks);
			historyTxCommitLatency.add(txCommitLatency);
			historytxRollbackLatency.add(txRollbackLatency);
			fixStatsHistorySize(historyTxCommitRate);
			fixStatsHistorySize(historyTxRollbackRate);
			fixStatsHistorySize(historyTxCommitLatency);
			fixStatsHistorySize(historytxRollbackLatency);
		}
	}

	/* Implement the method how to check the stats from the mbeans, and invoke
	 * notifications through the specified notifier if certain conditions are
	 * met
	 **/
	public void checkStats() {
		//this example is to raise warn if the txCommitLatency reaches to the
		//threshold (say 10.0 ms) as specified in the jmx-monitor.xml: 
		//<NotifcationThreshold name="txCommitLatency" value="10.0" />		
		if (gotCurrentStats && notifier != null) {
			if (txCommitLatencyThreshold != 0
					&& txCommitLatency >= txCommitLatencyThreshold) {
				notifier.warn("", "MEMBER " + memberObject.getKeyProperty("id")
						+ ":\n\t" + "TxCommitLatency is " + txCommitLatency
						+ ", and exceeds the threshold "
						+ txCommitLatencyThreshold + "ms");
			}
		}
	}
	
	/* Create the string containing all current stats */
	public String printOutCurrentStats() {
		StringBuffer buffer = new StringBuffer().append(TAB)
				.append("===").append(statsType).append("===").append(TAB)
				.append(TX_COMMITS).append("->").append(txCommits).append(TAB)
				.append(TX_ROLLBACKS).append("->").append(txRollbacks).append(TAB)
				.append(TX_SUCCESS_LIFETIME).append("->").append(txSuccessLifeTime).append("ms").append(TAB)
				.append(TX_ROLLBACK_LIFETIME).append("->").append(txRollbackLifeTime).append("ms").append(TAB);
		return buffer.toString();
	}

	/*
	 * Set the properties used by this instance. These properties are usually in
	 * the jmx-monitor.xml regarding the thresholds, history size etc. 
	 * For example 
	 * <StatsNotification notifier-id="ConsoleStatsNotifer">
	 * 		<NotifcationThreshold name="txCommitLatency" value="10.0" />
	 * </StatsNotification> 
	 * <StatsHistorySize>20</StatsHistorySize>
	 */
	@Override
	public void setProperties(Map<String, String> propertyMap) {
		super.setProperties(propertyMap);
		if (propertyMap != null) {
			try {
				txCommitLatencyThreshold = Double.valueOf(propertyMap.get("txCommitLatency"));
			} catch(NumberFormatException nfe) {
				System.out.println("txCommitLatencyThreshold is not set correctly");
			}			
		}		
	}
}