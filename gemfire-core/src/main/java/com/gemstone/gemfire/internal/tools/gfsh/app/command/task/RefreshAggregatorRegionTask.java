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

import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.tools.gfsh.command.AbstractCommandTask;
import com.gemstone.gemfire.internal.tools.gfsh.command.CommandResults;

public class RefreshAggregatorRegionTask extends AbstractCommandTask {
	private static final long serialVersionUID = 1L;

	// Default constructor required for serialization
	public RefreshAggregatorRegionTask() 
	{
	}
	
	private void initRegion()
	{
		PartitionedRegion pr = (PartitionedRegion)getCommandRegion().getSubregion("pr");
		if (pr == null) {
			return;
		}
		int totalBuckets = pr.getAttributes().getPartitionAttributes().getTotalNumBuckets();
		for (int i = 0; i < totalBuckets; i++) {
			pr.put(i, i);
			pr.remove(i);
		}
	}
	
	@Override
	public CommandResults runTask(Object userData)
	{
		new Thread(new Runnable() {
			public void run()
			{
				initRegion();
			}
		}).start();
		return null;
	}
	
	public void fromData(DataInput input) throws IOException,
			ClassNotFoundException 
	{
		super.fromData(input);
	}

	public void toData(DataOutput output) throws IOException {
		super.toData(output);
	}
}
