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
 * stats related to the tenured heap usages, and monitor the critical events
 */
public class ResourceManagerStats extends AbstractGemFireXDStats {
	/* define the variables which MBeans to access for the stats */
	private static final String RESOURCE_MANAGER_STATS_MBEAN = "ResourceManagerStats";
	
	/* define the variables which operations to MBeans in order to obtain the stats */
	private static final String GETSTATS_OPERATION = "manageStat";	
	
	/* define the variables for signatures of the operations */
	private static String[] SIGNATURE = new String[] {"java.lang.String"};
	
	/*
	 * define the stats (criticalThreshold, heapCriticalThreshold,
	 * tenuredHeapUsed) this class wants to monitor
	 */
	private static final String CRITICAL_THRESHOLD = "criticalThreshold";
	private static final String HEAP_CRITICAL_EVENTS = "heapCriticalEvents";
	private static final String TENUREHEAP_USED = "tenuredHeapUsed";
	
	/* the variables storing the current stats values fetched from the MBeans */	
	private Long criticalThreshold = null;
	private Long tenuredHeapUsed = null;
	private Integer heapCriticalEvents = null;
	
	/*
	 * the lists storing the history of the stats, their sizes could be
	 * controlled by the StatsHistorySize specified in the configuration file
	 */
	private LinkedList<Long> historyCriticalThreshold = new LinkedList<Long>();
	private LinkedList<Long> historyTenuredHeapUsed = new LinkedList<Long>();
	private LinkedList<Integer> historyHeapCriticalEvents = new LinkedList<Integer>();
	
	
	/* the variables used in the class */
	private boolean gotCurrentStats = false;	
	private double tenuredHeapUsedPercent = 0.0;
	
	/* Implement the method how to retrieved the heap stats from the mbeans */
	public void pollStats() {		
		try {
			gotCurrentStats = false;
			// The stats are the attributes of the mbean, only one step is
			// needed to retrieved them
			ObjectName[] resourceManagerStats = (ObjectName[]) mbs.invoke(
					memberObject, GETSTATS_OPERATION,
					new String[] { RESOURCE_MANAGER_STATS_MBEAN }, SIGNATURE);
			if (resourceManagerStats != null) {
				criticalThreshold = (Long) mbs.getAttribute(
						resourceManagerStats[0], CRITICAL_THRESHOLD);
				tenuredHeapUsed = (Long) mbs.getAttribute(
						resourceManagerStats[0], TENUREHEAP_USED);
				heapCriticalEvents = (Integer) mbs.getAttribute(
						resourceManagerStats[0], HEAP_CRITICAL_EVENTS);
				
				gotCurrentStats = true;
			}
		} catch (Exception e) {
			System.out.println("Error: Not able to get the statistics. " + e);
		}
		//store the stats in the history. only when all the stats are successfully
		//be retrieved, we store them in the history
		if (gotCurrentStats) {
			historyCriticalThreshold.add(criticalThreshold);
			historyTenuredHeapUsed.add(tenuredHeapUsed);
			historyHeapCriticalEvents.add(heapCriticalEvents);
			fixStatsHistorySize(historyCriticalThreshold);
			fixStatsHistorySize(historyTenuredHeapUsed);
			fixStatsHistorySize(historyHeapCriticalEvents);
		}
	}
	
	/* Implement the method how to check the stats from the mbeans, and invoke
	 * notifications through the specified notifier if certain conditions are
	 * met
	 **/
	public void checkStats() {
		//this example is to raise warn if the criticalThreshold is not able to
		//be captured in the mbean, and the tenured heap usage exceeds the
		//threshold (say 60%) as specified in the jmx-monitor.xml: 
		//<NotifcationThreshold name="TenuredHeapUsedPercent" value="0.6" />
		if (gotCurrentStats && notifier != null) {
			if (criticalThreshold == 0) {
				notifier.warn("", "MEMBER " + memberObject.getKeyProperty("id") + ":\n\t" 
						+ "critical threshold is not captured");
			} else {
				double heapUsedPercentage = tenuredHeapUsed*1.0/criticalThreshold*100;
				if (heapUsedPercentage > tenuredHeapUsedPercent*100) {
					notifier.warn("", "MEMBER " + memberObject.getKeyProperty("id") + ":\n\t" 
							+ heapUsedPercentage + "% of critical threshold was reached, exceeds " 
							+ tenuredHeapUsedPercent*100 + "% of the CriticalThreshold.");
				}
			}
			
			//raise alert if the HeapCriticalEvents is increasing
			int lastHeapCritialEventIndex = historyHeapCriticalEvents.size();
			if (((lastHeapCritialEventIndex > 1) 
					&& (heapCriticalEvents > historyHeapCriticalEvents.get(lastHeapCritialEventIndex - 1)))
					|| (lastHeapCritialEventIndex == 1 && heapCriticalEvents > 0)) {
				notifier.alert("", "MEMBER " + memberObject.getKeyProperty("id") + ":\n" 
						+"\tHeapCriticalEvents were invoked.");
			}
		}
	}
	
	/* Create the string containing all current stats */
	public String printOutCurrentStats() {
		StringBuffer buffer = new StringBuffer().append(TAB)
				.append("===").append(statsType).append("===").append(TAB)
				.append(TENUREHEAP_USED).append("->").append(tenuredHeapUsed).append(TAB)
				.append(CRITICAL_THRESHOLD).append("->").append(criticalThreshold).append(TAB)
				.append(HEAP_CRITICAL_EVENTS).append("->").append(heapCriticalEvents).append(TAB);
		return buffer.toString();
	}
	
	/*
	 * Set the properties used by this instance. These properties are usually in
	 * the jmx-monitor.xml regarding the thresholds, history size etc. 
	 * For example
	 * <StatsNotification notifier-id="ConsoleStatsNotifer">
	 * 		<NotifcationThreshold name="TenuredHeapUsedPercent" value="0.6" />
	 * 		<NotifcationThreshold name="HeapCriticalEvents" /> 
	 * </StatsNotification>
	 * <StatsHistorySize>10</StatsHistorySize>
	 */
	@Override
	public void setProperties(Map<String, String> propertyMap) {
		super.setProperties(propertyMap);
		if (propertyMap != null) {
			try {
				tenuredHeapUsedPercent = Double.valueOf(propertyMap.get("TenuredHeapUsedPercent"));
			} catch(NumberFormatException nfe) {
				System.out.println("TenuredHeapUsedPercent is not set correctly");
			}			
		}
	}
}