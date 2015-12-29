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
package com.gemstone.gemfire.internal.tools.gfsh.app.commands;

import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.internal.tools.gfsh.app.CommandExecutable;
import com.gemstone.gemfire.internal.tools.gfsh.app.Gfsh;
import com.gemstone.gemfire.internal.tools.gfsh.app.command.task.RegionPathTask;
import com.gemstone.gemfire.internal.tools.gfsh.command.CommandResults;
import com.gemstone.gemfire.internal.tools.gfsh.util.RegionUtil;

public class refresh implements CommandExecutable
{
	private Gfsh gfsh;
	
	public refresh(Gfsh gfsh)
	{
		this.gfsh = gfsh;
	}
	
	public void help()
	{
		gfsh.println("refresh [-?]");
		gfsh.println("     Refresh the entire local cache. It fetches region");
		gfsh.println("     information from all servers and updates local regions.");
		gfsh.println("     It creates new regions found in the servers in the local VM.");
		gfsh.println();
	}
	
	public void execute(String command) throws Exception
	{
		if (command.startsWith("refresh -?")) {
			help();
		} else {
			refresh();
		}
	}
	
	private void refresh()
	{
		if (gfsh.isConnected() == false) {
			gfsh.println("Error: gfsh is not connected to a server. Use the 'connect' command to connect first. aborting refresh");
		}
		
		CommandResults results = gfsh.getCommandClient().execute(new RegionPathTask(false, true));
		String[] regionPaths = (String[]) results.getDataObject();
		if (regionPaths != null) {
			Region region;
			for (int i = 0; i < regionPaths.length; i++) {
				if (gfsh.isLocator()) {
					region = RegionUtil.getRegion(regionPaths[i], Scope.LOCAL, DataPolicy.NORMAL, gfsh.getPool(), false);
				} else {
					region = RegionUtil.getRegion(regionPaths[i], Scope.LOCAL, DataPolicy.NORMAL, gfsh.getEndpoints());
				}
//				if (region != null) {
//					region.setUserAttribute(regionInfo);
//				}
			}
			gfsh.println("refreshed");
		}
		
		gfsh.refreshAggregatorRegion();
	}
}
