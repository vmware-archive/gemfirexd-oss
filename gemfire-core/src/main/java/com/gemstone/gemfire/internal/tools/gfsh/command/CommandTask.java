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
package com.gemstone.gemfire.internal.tools.gfsh.command;

import com.gemstone.gemfire.DataSerializable;

/**
 * CommandTask is an interface that must be implemented for executing
 * command task. The server invokes CommandTask.runTask() and returns
 * its results to the client.
 * @author dpark
 *
 */
public interface CommandTask extends DataSerializable
{	
	/**
	 * Runs this task. A client executes CommandTak by calling
	 * CommandClient.execute(CommandTask task).
	 * @param userData The userData optionally provided by the cache server. The
	 *                 cache server may pass any user data to the command task.
	 * @return Returns the task results.
	 */
    public CommandResults runTask(Object userData);

}
