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

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;

import examples.jmxmonitor.notifier.StatsNotifier;

/*
 * An abstract class providing some default implementations to some common
 * methods for classes of type  GemFireXDStats.
 */
public abstract class AbstractGemFireXDStats implements GemFireXDStats {
	protected static final String TAB = "\n\t\t\t";
	
	protected MBeanServerConnection mbs = null;
	protected ObjectName memberObject = null;
	protected StatsNotifier notifier = null;
	protected Map<String, String> propertyMap = null;
	protected String statsType = null;
	
	protected long historySize = 0;	
	
	public abstract void pollStats();
	public abstract void checkStats();
	public abstract String printOutCurrentStats();
	
	public void setMBeanServerConnection(MBeanServerConnection mbs) {
		this.mbs = mbs;
	}
	
	public void setMemberMBean(ObjectName memberObject) {
		this.memberObject = memberObject;
	}
	
	public void setNotifier(StatsNotifier notifier) {
		this.notifier = notifier;
	}

	public void setProperties(Map<String, String> propertyMap) {
		this.propertyMap = propertyMap;
		if (propertyMap != null) {
			try {
				historySize = Long.valueOf(propertyMap.get("StatsHistorySize"));
			} catch(NumberFormatException nfe) {
				System.out.println("StatsHistorySize is not set correctly");
			}		
		}		
	}

	public void setStatType(String statsType) {
		this.statsType = statsType;
	}

	/*
	 * Ensure that the stats history size does not exceed the specified limit
	 */
	protected void fixStatsHistorySize(LinkedList<?> statsHistory) {
		while (statsHistory.size() > this.historySize) {
			statsHistory.removeFirst();
		}
	}
}