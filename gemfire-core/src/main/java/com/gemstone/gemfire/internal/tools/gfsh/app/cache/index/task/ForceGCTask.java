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
package com.gemstone.gemfire.internal.tools.gfsh.app.cache.index.task;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.internal.tools.gfsh.command.CommandResults;
import com.gemstone.gemfire.internal.tools.gfsh.command.CommandTask;

public class ForceGCTask implements CommandTask
{
	private static final long serialVersionUID = 1L;
	
	public ForceGCTask() {}
	
	public CommandResults runTask(Object userData)
	{
		Runtime.getRuntime().gc();//FindBugs - extremely dubious except in benchmarking code
		return new CommandResults();
	}

	public void fromData(DataInput in) throws IOException,
			ClassNotFoundException
	{
	}

	public void toData(DataOutput out) throws IOException
	{
	}
}
