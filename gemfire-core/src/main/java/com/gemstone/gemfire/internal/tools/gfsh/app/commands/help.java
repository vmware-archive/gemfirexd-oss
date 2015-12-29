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

import com.gemstone.gemfire.internal.tools.gfsh.app.CommandExecutable;
import com.gemstone.gemfire.internal.tools.gfsh.app.Gfsh;

public class help implements CommandExecutable
{
	private Gfsh gfsh;
	
	public help(Gfsh gfsh)
	{
		this.gfsh = gfsh;
	}
	
	public void help()
	{
		gfsh.println("help or ?");
		gfsh.println("     List command descriptions");
		gfsh.println();
	}
	
	public void execute(String command) throws Exception
	{
    
		if (command.startsWith("help -?")) {
			help();
		} else {
	    String[] splitted = command.split(" ");
	    if (splitted.length > 1) {
	      gfsh.showHelp(splitted[1]);
      } else {
        gfsh.showHelp();
      }
		}
	}
}
