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

import java.util.ArrayList;

import com.gemstone.gemfire.internal.tools.gfsh.app.CommandExecutable;
import com.gemstone.gemfire.internal.tools.gfsh.app.Gfsh;

public class echo implements CommandExecutable
{
	private Gfsh gfsh;
	
	public echo(Gfsh gfsh)
	{
		this.gfsh = gfsh;
	}
	
	public void help()
	{
		gfsh.println("echo [true|false] [<message>] | [-?]");
		gfsh.println("     Toggle the echo setting. If echo is true then input");
		gfsh.println("     commands are echoed to stdout. If <message> is specified");
		gfsh.println("     it is printed without toggling echo. It expands properties.");
		gfsh.println();
	}
	
	public void execute(String command) throws Exception
	{
		if (command.startsWith("echo -?")) {
			help();
		} else {
			echo(command);
		}
	}
	
	private void echo(String command)
	{
		ArrayList<String> list = new ArrayList();
		gfsh.parseCommand(command, list);
		if (list.size() >= 2) {
			if (list.get(1).equalsIgnoreCase("true")) {
				gfsh.setEcho(true);
			} else if (list.get(1).equalsIgnoreCase("false")) {
				gfsh.setEcho(false);
			} else {
				// message
				// command is already trimmed. no need to trim
				int index = command.indexOf(' ');
				String message = command.substring(index+1);
				gfsh.println(message);
				return;
			}

		} else {
			gfsh.setEcho(!gfsh.isEcho());
		}
		
		gfsh.println("echo is " + (gfsh.isEcho() ? "true" : "false"));
	}
}
