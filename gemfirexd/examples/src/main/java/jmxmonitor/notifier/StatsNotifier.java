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
package examples.jmxmonitor.notifier;

/*
 * The interface all the classes created to notify the stats should implement
 */
public interface StatsNotifier {
	/*
	 * Delivers the information about the monitored statistics
	 * 
	 * @param subject the subject of the information
	 * @param msg the content of the information
	 */
	void info(String subject, String msg);

	/*
	 * Delivers the warn raised from the monitored statistics
	 * 
	 * @param subject the subject of the warn
	 * @param msg the content of the warn
	 */
	void warn(String subject, String msg);
	
	/*
	 * Delivers the alert raised from the monitored statistics
	 * 
	 * @param subject the subject of the alert
	 * @param msg the content of the alert
	 */	
	void alert(String subject, String msg);
}