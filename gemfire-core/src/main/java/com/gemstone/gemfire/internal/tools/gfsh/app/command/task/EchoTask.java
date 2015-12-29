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
package com.gemstone.gemfire.internal.tools.gfsh.app.command.task;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.internal.tools.gfsh.command.CommandResults;
import com.gemstone.gemfire.internal.tools.gfsh.command.CommandTask;

/**
 * EchoTask returns itself back to the caller. CommandResults.getDataObject()
 * returns EchoTask.
 * @author dpark
 *
 */
public class EchoTask implements CommandTask {
	private static final long serialVersionUID = 1L;

	public static final byte ERROR_REGION_DESTROY = 1;
	
	private String message;

	public EchoTask() {
	}

	public EchoTask(String message) {
		this.message = message;
	}

	public CommandResults runTask(Object userData) {
		CommandResults results = new CommandResults();
		results.setDataObject(this);
		return results;
	}

	private void writeUTF(String value, DataOutput output) throws IOException
	{
		if (value == null) {
			output.writeUTF("\0");
		} else {
			output.writeUTF(value);
		}
	}

	private String readUTF(DataInput in) throws IOException
	{
		String value = in.readUTF();
		if (value.equals("\0")) {
			value = null;
		}
		return value;
	}
	
	public void fromData(DataInput input) throws IOException,
			ClassNotFoundException {
		message = readUTF(input);
	}

	public void toData(DataOutput output) throws IOException {
		writeUTF(message, output);
	}

}
