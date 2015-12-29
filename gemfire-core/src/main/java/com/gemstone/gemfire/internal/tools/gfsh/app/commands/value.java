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

import com.gemstone.gemfire.internal.tools.gfsh.app.CommandExecutable;
import com.gemstone.gemfire.internal.tools.gfsh.app.Gfsh;

public class value implements CommandExecutable
{
	private Gfsh gfsh;
	
	public value(Gfsh gfsh)
	{
		this.gfsh = gfsh;
	}
	
	public void help()
	{
		gfsh.println("value [-?] <class name>");
		gfsh.println("     Set the value class to be used for the 'put' command.");
		gfsh.println("     Use the 'key' command to set the key class name.");
		gfsh.println();
	}
	
	public void execute(String command) throws Exception
	{
		if (command.startsWith("value -?")) {
			help();
		} else {
			value(command);
		}
	}
	
	private void value(String command) throws Exception
	{
		LinkedList list = new LinkedList();
		gfsh.parseCommand(command, list);
		if (list.size() < 2) {
			gfsh.println("value = " + gfsh.getValueClassName());
			gfsh.println("   Use value <class name> to set the value class");
		} else {
			if (list.size() > 1) {
				gfsh.setValueClass((String) list.get(1));
			}
		}
	}
	
}
