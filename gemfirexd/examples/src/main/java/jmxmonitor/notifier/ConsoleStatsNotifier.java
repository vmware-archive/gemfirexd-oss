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
 * This is the example showing how to implement a class to deliver the
 * notification through console print out
 */
public class ConsoleStatsNotifier implements StatsNotifier {
	public void info(String subject, String msg) {
		System.out.println("Info: " + msg);
	}
	
	public void warn(String subject, String msg) {
		System.out.println("Warn: ============");
		System.out.println(msg);
		System.out.println("==================");
	}
	
	public void alert(String subject, String msg) {
		System.out.println("Alert! ===========");
		System.out.println(msg);
		System.out.println("==================");
	}
}