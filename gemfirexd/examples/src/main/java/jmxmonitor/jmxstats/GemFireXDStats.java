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

import java.util.Map;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;

import examples.jmxmonitor.notifier.StatsNotifier;

/*
 * The interface all the classes created to monitor certain stats should implement
 */
public interface GemFireXDStats {
	/*
	 * Polls the stats from the member mbean.
	 */
	void pollStats();

	/*
	 * Checks the refreshed stats, and probably send out notification if the
	 * stats meet certain condition.
	 */
	void checkStats();

	/*
	 * Print out the current stats
	 */
	String printOutCurrentStats();

	/*
	 * Called in JMXGemFireXDMonitor to assign the MBeanServerConnection to any
	 * new instance of GemFireXDStats class, the MBeanServerConnection is used to
	 * fetch the stats from the mbeans
	 * 
	 * @param mbs the JMX agent MBean server connection
	 */
	void setMBeanServerConnection(MBeanServerConnection mbs);

	/*
	 * Called in JMXGemFireXDMonitor to specify the member whose stats are to be
	 * retrieved in this new instance of GemFireXDStats class
	 * 
	 * @param memberObject the objectName of the GemFireXD member
	 */
	void setMemberMBean(ObjectName memberObject);

	/*
	 * Called in JMXGemFireXDMonitor to set the notifier used by this new instance
	 * of GemFireXDStats class
	 * 
	 * @param notifier the notifier to send stats notification
	 */
	void setNotifier(StatsNotifier notifier);

	/*
	 * Called in JMXGemFireXDMonitor to pass any special properties defined in the
	 * configuration file to this new instance of GemFireXDStats class, they may
	 * include the thresholds etc
	 * 
	 * @param propertyMap the properties of this instance
	 */
	void setProperties(Map<String, String> propertyMap);

	/*
	 * Specifies the stats type this new instance is associated with
	 * 
	 * @param statsType the type of stats this instance to monitor
	 */
	void setStatType(String statsType);
}