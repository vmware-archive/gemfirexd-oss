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

import java.util.LinkedList;
import java.util.List;

import com.gemstone.gemfire.internal.tools.gfsh.aggregator.AggregateResults;
import com.gemstone.gemfire.internal.tools.gfsh.app.CommandExecutable;
import com.gemstone.gemfire.internal.tools.gfsh.app.Gfsh;
import com.gemstone.gemfire.internal.tools.gfsh.app.aggregator.Aggregator;
import com.gemstone.gemfire.internal.tools.gfsh.app.command.task.data.MemberInfo;
import com.gemstone.gemfire.internal.tools.gfsh.app.function.GfshData;
import com.gemstone.gemfire.internal.tools.gfsh.app.function.GfshFunction;

public class gc implements CommandExecutable
{
	private Gfsh gfsh;
	
	public gc(Gfsh gfsh)
	{
		this.gfsh = gfsh;
	}
	
	public void help()
	{
		gfsh.println("gc [-g] [-m <member id>] | [-?]");
		gfsh.println("     Force gc on the connected server or all of the servers.");
		gfsh.println("     -g  Force gc globally on all servers.");
		gfsh.println("     -m <member id>  Force gc on the specified member. The member id can");
		gfsh.println("            be obtained by executing 'size -m' or 'ls -m'");
		gfsh.println();
	}
	
	public void execute(String command) throws Exception
	{
		if (command.startsWith("gc -?")) {
			help();
		} else if (command.startsWith("gc -g")) {
			gc(command);
		} else if (command.startsWith("gc -m")) {
			gc(command);
		} else {
			gfsh.println("Error: invalid gc option.");
		}
	}
	
	private void gc(String command) throws Exception
	{
		LinkedList<String> list = new LinkedList();
		gfsh.parseCommand(command, list);
		
		boolean isGlobal = false;
		String memberId = null;
		if (command.startsWith("gc -m")) {
			if (list.size() > 2) {
				memberId = list.get(2);
			}
		} else if (command.startsWith("gc -g")) {
			isGlobal = true;
		}
		
		if (isGlobal == false && memberId == null) {
			gfsh.println("Error: invalid option. 'gc -m' requires <member id>. Use 'size -m' or 'ls -m' to list member ids.");
			return;
		}
		
		String confirmation = gfsh.getLine("This command forc gc on the server(s).\nDo you want to proceed? (yes|no): ");
		if (confirmation.equalsIgnoreCase("yes") == false) {
			gfsh.println("Command aborted.");
			return;
		}
		
		Aggregator aggregator = gfsh.getAggregator();
		List<AggregateResults> results = (List<AggregateResults>)gfsh.getAggregator().aggregate(new GfshFunction("gc", gfsh.getCurrentPath(), memberId), gfsh.getAggregateRegionPath());
		int i = 1;
		for (AggregateResults aggregateResults : results) {
			GfshData data = (GfshData)aggregateResults.getDataObject();
			MemberInfo memberInfo = data.getMemberInfo();
			if (isGlobal || (memberId != null && memberId.equals(memberInfo.getMemberId()))) {
				gfsh.print(i + ". " + memberInfo.getMemberName() + "(" + memberInfo.getMemberId() + ")" + ": ");
				if (aggregateResults.getCode() == AggregateResults.CODE_ERROR) {
					gfsh.println("error - " + aggregateResults.getCodeMessage());
					if (gfsh.isDebug() && aggregateResults.getException() != null) {
						aggregateResults.getException().printStackTrace();
					}
				} else {
					gfsh.println("GC forced");
				}
			}
			i++;
		}
//		gfsh.getCommandClient().execute(new ForceGCTask());
	}
}
