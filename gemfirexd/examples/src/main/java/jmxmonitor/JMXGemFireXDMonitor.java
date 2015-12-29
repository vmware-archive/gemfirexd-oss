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
package examples.jmxmonitor;

import java.text.SimpleDateFormat;
import java.util.*;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import examples.jmxmonitor.jmxstats.GemFireXDStats;
import examples.jmxmonitor.notifier.StatsNotifier;;

public class JMXGemFireXDMonitor extends TimerTask {
	/** The protocol used by JMX service */
	private static final String JMX_PROTOCOL = "rmi";
	
	/** The name which the RMIConnector registered under JNDI */
	private static final String JNDI_NAME = "/jmxconnector";

	/** The default configuration file used by this monitor */
	private static final String MONITOR_CONFIG_FILE = "jmx-monitor.xml";
	
	/** The timer to schedule the monitor run */
	private static final Timer TIMER = new Timer("GemFireXD Member Monitor");

	/** The format for the timestamp used in output */
	private static final SimpleDateFormat sdfDate = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss.SSS");

	/** The JMX Agent's RMI host, defined in the configuration file */
	private final String agentHost;

	/** The JMX Agent's RMI port, defined in the configuration file */
	private final String agentPort;

	/**
	 * The run mode of this monitor, can be once and continuous, and defined in
	 * the configuration file
	 */
	private final String runMode;

	/**
	 * The duration this monitor will run, defined in the configuration file
	 */
	private final long runDuration;
	
	/**
	 * The interval that the monitor polls the members for stats. It is defined
	 * in the configuration file, otherwise will use system refreshInterval
	 */
	private long pollInterval;
	
	/** The map of the customized java classes used for notification */
	private final Map<String, StatsNotifier> statsNotifierMap;
	
	/** The map of the customized java classes for member stats */
	private final Map<String, Class<GemFireXDStats>> memberStatsClassMap;
	
	/** The map to store the member stats instance's properties */
	private final Map<String, Map<String, String>> memberStatsClassPropertiesMap;

	/** The JMX MBean server connection */
	private MBeanServerConnection mbs;

	/** The GemFireXD Distributed System Bean */
	private ObjectName systemName;
	
	/** The map of the stats beans for a member*/
	private final Map<ObjectName, Map<String, GemFireXDStats>> memberStats;
	
	/** The map of the stats beans for the whole system*/
	//Not implemented yet in this example
	//private final Map<ObjectName, Map<String, GemFireXDStats>> systemStats;

	public static void main(String[] args) throws Exception {
		String configFile = (args.length ==0 ? MONITOR_CONFIG_FILE : args[0]);
		JMXGemFireXDMonitor monitor = new JMXGemFireXDMonitor(configFile);
		
		monitor.connectToAgent();
		
		if (monitor.runMode.equalsIgnoreCase("once")) {
			TIMER.schedule(monitor, 0);
		} else {
			TIMER.schedule(monitor, 0, monitor.pollInterval);
			if (monitor.runDuration !=0) {
				Thread.sleep(monitor.runDuration);
				TIMER.cancel();
			}
		}
	}

	/**
	 * TimerTask run method
	 */
	public void run() {
		try {
			refreshStats();
			if (runMode.equalsIgnoreCase("once")) {
				TIMER.cancel();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * JMXGemFireXDMonitor constructor: build the monitor based on the
	 * configurations specified in the configuration xml file
	 */
	private JMXGemFireXDMonitor(String configFile) {
		MonitorConfigHelper configHelper = new MonitorConfigHelper(configFile);
		try {
			configHelper.parseMonitorConfiguration();
		} catch (JMXGemFireXDMonitorException e) {
			e.printStackTrace();
			System.exit(1);
		}

		agentHost = configHelper.getAgentHost();
		agentPort = configHelper.getAgentPort();
		pollInterval = configHelper.getPollInterval();
		runMode = configHelper.getRunMode();
		runDuration = configHelper.getRunDuration();
		statsNotifierMap = configHelper.getStatsNotifierMap();
		memberStatsClassMap = configHelper.getMemberStatsClassMap();
		memberStatsClassPropertiesMap = configHelper.getMemberStatsClassPropertiesMap();

		memberStats = new HashMap<ObjectName, Map<String, GemFireXDStats>>();
	}

	/**
	 * Builds the JMX service url, connects to the GemFireXD JMX Agent, then
	 * accesses the distributed system MBean
	 */
	private void connectToAgent() throws Exception {
		String rmiServerHost = agentHost.equalsIgnoreCase("localhost") ? ""
				: agentHost;

		// Create the JMXServiceURL
		//e.g. service:jmx:rmi://agent-host/jndi/rmi://agent-host:agent-port/jmxconnector
		StringBuilder builder = new StringBuilder();
		builder.append("service:jmx:").append(JMX_PROTOCOL).append("://")
				.append(rmiServerHost).append("/jndi/rmi://").append(agentHost)
				.append(":").append(agentPort).append(JNDI_NAME);
		String urlString = builder.toString();
		JMXServiceURL url = new JMXServiceURL(urlString);

		// Create the JMXConnector
		JMXConnector jmxConnector = JMXConnectorFactory.connect(url);

		mbs = jmxConnector.getMBeanServerConnection();
		ObjectName agentName = new ObjectName("GemFire:type=Agent");
		log("Using " + agentName);

		//access the DistributedSystem MBean
		systemName = (ObjectName) mbs.invoke(agentName,
				"manageDistributedSystem", new Object[0], new String[0]);
		log("Connected to " + systemName);

		long refreshInterval = (Integer) mbs.getAttribute(systemName,
				"refreshInterval");
		if (pollInterval == 0) {
			pollInterval = refreshInterval * 1000; // to millsecond
			log("Info: Monitor pollInterval was not configured, ");
			log("use the system refresh interval to poll stats");
		}

		if (pollInterval < refreshInterval) {
			log("Info: Monitor pollInterval is shorter than system refresh interval");
		}
	}

	/**
	 * Gets the refreshed stats of each member from the distributed system, also
	 * updates to only keep the stats of the members currently in the system
	 */
	private void refreshStats() throws Exception {
		// Get current members
		List<ObjectName> memberNames = getCurrentMembers();
		for (ObjectName memberName : memberNames) {
			StringBuffer buffer = new StringBuffer()
			.append("Statistic resources for member ")
			.append(memberName.getKeyProperty("id")).append(':');
			
			log("Getting statistic resources for " + memberName);
			
			// if the member is new one, instantiates its stats mbeans,
			// otherwise, finds the stats mbeans associated with this member,
			// then refresh the stats
			if (!memberStats.containsKey(memberName)) {
				Map<String, GemFireXDStats> individualStats = new HashMap<String, GemFireXDStats>();

				for (Map.Entry<String, Class<GemFireXDStats>> entry : memberStatsClassMap.entrySet()) {
					String key = entry.getKey();
					GemFireXDStats gemfirexdStats = entry.getValue().newInstance();
					gemfirexdStats.setMBeanServerConnection(mbs);
					gemfirexdStats.setMemberMBean(memberName);
					gemfirexdStats.setNotifier(statsNotifierMap.get(memberStatsClassPropertiesMap.get(key).get("notifier-id")));
					gemfirexdStats.setProperties(memberStatsClassPropertiesMap.get(key));
					gemfirexdStats.setStatType(key);
					gemfirexdStats.pollStats();
					gemfirexdStats.checkStats();
					buffer.append(gemfirexdStats.printOutCurrentStats());
					individualStats.put(key, gemfirexdStats);
				}
				memberStats.put(memberName, individualStats);
			} else {
				Map<String, GemFireXDStats> individualStats = memberStats.get(memberName);
				for (Map.Entry<String, GemFireXDStats> entry : individualStats.entrySet()) {
					GemFireXDStats gemfirexdStats = entry.getValue();
					gemfirexdStats.pollStats();
					gemfirexdStats.checkStats();	
					buffer.append(gemfirexdStats.printOutCurrentStats());
				}
			}		
			
			logWithTimestamp(buffer.toString());			
		}

		// Remove the departed members from map
		for (Iterator<ObjectName> i = memberStats.keySet().iterator(); i
				.hasNext();) {
			ObjectName currentMemberName = i.next();
			if (!memberNames.contains(currentMemberName)) {
				i.remove();
			}
		}
	}

	/**
	 * Gets the members in the distributed system, they can be of CacheVMs (e.g.
	 * server, locator), or MemberApplications (e.g. peer client)
	 */
	private List<ObjectName> getCurrentMembers() throws Exception {
		List<ObjectName> allMembers = new ArrayList<ObjectName>();
		ObjectName[] members = null;
		members = (ObjectName[]) mbs.invoke(systemName,
				"manageSystemMemberApplications", new Object[0], new String[0]);
		if (members != null) {
			allMembers.addAll(Arrays.asList(members));
		}
		members = (ObjectName[]) mbs.invoke(systemName, "manageCacheVms",
				new Object[0], new String[0]);
		if (members != null) {
			allMembers.addAll(Arrays.asList(members));
		}
		return allMembers;
	}

	/**
	 * Adds the timestamp to the log entry
	 */
	private static void logWithTimestamp(String message) {
	    log("[" + sdfDate.format(new Date()) +"] " + message);
	}
	
	private static void log(String message) {
	    System.out.println(message);
	}	
}
