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

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.tools.gfsh.app.CommandExecutable;
import com.gemstone.gemfire.internal.tools.gfsh.app.Gfsh;

public class cd implements CommandExecutable
{
	private Gfsh gfsh;
	
	private String previousPath;
	
	public cd(Gfsh gfsh)
	{
		this.gfsh = gfsh;
	}
	
	public void help()
	{
		gfsh.println("cd [-] | [-?] <region path>");
		gfsh.println("     Change region path.");
		gfsh.println("     - Change region path to the previous path.");
		gfsh.println();
	}
	
	public void execute(String command) throws Exception
	{
		if (command.startsWith("cd -?")) {
			help();
		} else if (command.equals("cd -")) {
			cd_prev();
		} else {
			cd(command);
		}
	}
	
	private void cd_prev()
	{
		chdir(previousPath);
	}
	
	private void cd(String command)
	{
		int index = command.indexOf(" ");
		if (index == -1) {
			chdir("/");
		} else {
			String newPath = command.substring(index).trim();
			chdir(newPath);
		}
	}
	
	private void chdir(String newPath)
	{
		if (newPath == null) {
			return;
		}
		
		String currentPath = gfsh.getCurrentPath();
		String fullPath = gfsh.getFullPath(newPath, currentPath);
		if (fullPath == null) {
			gfsh.println("Error: invalid region path");
		} else if (fullPath.equals("/")) {	
			gfsh.setCurrentRegion(null);
			gfsh.setCurrentPath(fullPath);
			previousPath = currentPath;
		} else {
			Region currentRegion = gfsh.getCache().getRegion(fullPath);
			if (currentRegion == null) {
				gfsh.println("Error: undefined region path " + fullPath);
				return;
			} else {
				gfsh.setCurrentPath(fullPath);
			}
			gfsh.setCurrentRegion(currentRegion);
			previousPath = currentPath;
		}
	}
}
