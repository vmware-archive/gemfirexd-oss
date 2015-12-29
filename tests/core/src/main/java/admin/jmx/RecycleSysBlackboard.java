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
package admin.jmx;

import hydra.blackboard.Blackboard;

public class RecycleSysBlackboard
	extends Blackboard 
{
	public RecycleSysBlackboard() { }

	public RecycleSysBlackboard(String name, String type) 
	{
		super(name, type, RecycleSysBlackboard.class);
	}
	
	public static synchronized RecycleSysBlackboard getInstance() 
	{
		if ( blackboard == null)
		{
			synchronized( RecycleSysBlackboard.class ) 
			{
				if (blackboard == null)
					blackboard = new RecycleSysBlackboard( "RecycleSysBlackboard", "RMI");
			}
			
			blackboard.getSharedCounters().zero(numOfPrimaryGateways);
			blackboard.getSharedCounters().zero(numOfSecondaryGateways);
			blackboard.getSharedCounters().zero(numOfGateways);
			blackboard.getSharedMap();
		}
		
		return blackboard;
	}
	
	public static RecycleSysBlackboard blackboard;
	public final static String SYSTEM_READY = "system ready";
	
	public static int numOfPrimaryGateways;
	public static int numOfSecondaryGateways;
	public static int numOfGateways;
}
