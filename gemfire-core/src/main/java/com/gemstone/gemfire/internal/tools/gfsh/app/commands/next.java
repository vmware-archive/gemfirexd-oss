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

import java.util.List;

import com.gemstone.gemfire.internal.tools.gfsh.app.CommandExecutable;
import com.gemstone.gemfire.internal.tools.gfsh.app.Gfsh;
import com.gemstone.gemfire.internal.tools.gfsh.app.Nextable;

public class next implements CommandExecutable
{
	private Gfsh gfsh;
	private String command;
	private Object userData;
	
	public next(Gfsh gfsh)
	{
		this.gfsh = gfsh;
	}
	
	public void help()
	{
		gfsh.println("next | n [-?]");
		gfsh.println("     Fetch the next set of query results.");
		gfsh.println();
	}
	
	public void execute(String command) throws Exception
	{
		if (command.startsWith("next -?")) {
			help();
		} else {
			next();
		}
	}
	
	public void setCommand(String command, Object userData)
	{
		this.command = command;
		this.userData = userData;
	}
	
	public void setCommand(String command)
	{
		setCommand(command, null);
	}
	
	public String getCommand()
	{
		return command;
	}
	
	public Object getUserData()
	{
		return userData;
	}
	
	public List next() throws Exception
	{
		Nextable nextable = (Nextable)gfsh.getCommand(command);
		return nextable.next(userData);
	}
	
}
