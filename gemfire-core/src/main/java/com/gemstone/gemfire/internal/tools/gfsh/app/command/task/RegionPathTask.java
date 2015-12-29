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

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.tools.gfsh.command.CommandResults;
import com.gemstone.gemfire.internal.tools.gfsh.command.CommandTask;
import com.gemstone.gemfire.internal.tools.gfsh.util.RegionUtil;

/**
 * RegionPathTask retrieves an entire list of region paths in the connected
 * server or in the entire distributed system in which the connected server
 * belongs.
 */
public class RegionPathTask implements CommandTask
{
	private static final long serialVersionUID = 1L;

	private boolean regionsInDistributedSystem = false;
	private boolean recursive = true;
	private String parentRegionPath = null;

	/**
	 * Returns all region paths in the entire distributed system. This
	 * constructor call is equivalent to new RegonPathTask(true, true, null);
	 */
	public RegionPathTask()
	{
	}

	/**
	 * Returns all region paths starting from the top level.
	 * 
	 * @param regionsInDistributedSystem
	 *            if false, returns all region paths found in the cache. If
	 *            true, returns all region paths found in the entire distributed
	 *            system.
	 * @param recursive
	 *            if true, returns all nested region paths, otherwise, returns
	 *            the top-level region paths
	 */
	public RegionPathTask(boolean regionsInDistributedSystem, boolean recursive)
	{
		this(regionsInDistributedSystem, recursive, null);
	}

	/**
	 * @param regionsInDistributedSystem
	 *            if false, returns all region paths found in the cache. If
	 *            true, returns all region paths found in the entire distributed
	 *            system.
	 * @param recursive
	 *            if true, returns all nested region paths, otherwise, returns
	 *            the top-level region paths
	 * @param parentRegionPath
	 *            the parent region path
	 */
	public RegionPathTask(boolean regionsInDistributedSystem, boolean recursive, String parentRegionPath)
	{
		this.regionsInDistributedSystem = regionsInDistributedSystem;
		this.recursive = recursive;
		this.parentRegionPath = parentRegionPath;
	}

	public CommandResults runTask(Object userData)
	{
		String[] regionPaths = null;
		Cache cache = CacheFactory.getAnyInstance();
		if (regionsInDistributedSystem) {

			// get region paths defined in this cache only

			if (parentRegionPath == null) {
				regionPaths = RegionUtil.getAllRegionPaths(CacheFactory.getAnyInstance(), recursive);
			} else {
				Region region = cache.getRegion(parentRegionPath);
				if (region != null) {
					regionPaths = RegionUtil.getAllRegionPaths(region, recursive);
				}
			}

		} else {

			// get region paths defined in all of the caches in the distributed
			// system

			if (parentRegionPath == null) {
				regionPaths = RegionUtil.getAllRegionPathsInDistributedSystem(cache.getDistributedSystem(), recursive);
			} else {
				Region region = cache.getRegion(parentRegionPath);
				if (region != null) {
					regionPaths = RegionUtil.getAllRegionPaths(region, recursive);
				}
			}

		}
		CommandResults results = new CommandResults(regionPaths);
		return results;
	}

	public boolean isRegionsInDistributedSystem()
	{
		return regionsInDistributedSystem;
	}

	public void setRegionsInDistributedSystem(boolean regionsInDistributedSystem)
	{
		this.regionsInDistributedSystem = regionsInDistributedSystem;
	}

	public boolean isRecursive()
	{
		return recursive;
	}

	public void setRecursive(boolean recursive)
	{
		this.recursive = recursive;
	}

	public String getParentRegionPath()
	{
		return parentRegionPath;
	}

	public void setParentRegionPath(String parentRegionPath)
	{
		this.parentRegionPath = parentRegionPath;
	}

	public void fromData(DataInput input) throws IOException, ClassNotFoundException
	{
		regionsInDistributedSystem = input.readBoolean();
		recursive = input.readBoolean();
		parentRegionPath = input.readUTF();
		if (parentRegionPath.equals("\0")) {
			parentRegionPath = null;
		}
	}

	public void toData(DataOutput output) throws IOException
	{
		output.writeBoolean(regionsInDistributedSystem);
		output.writeBoolean(recursive);
		if (parentRegionPath == null) {
			output.writeUTF("\0");
		} else {
			output.writeUTF(parentRegionPath);
		}
	}

}
