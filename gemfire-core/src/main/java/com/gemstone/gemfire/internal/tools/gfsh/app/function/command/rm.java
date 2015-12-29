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
package com.gemstone.gemfire.internal.tools.gfsh.app.function.command;

import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.tools.gfsh.aggregator.AggregateResults;
import com.gemstone.gemfire.internal.tools.gfsh.app.ServerExecutable;
import com.gemstone.gemfire.internal.tools.gfsh.app.function.GfshData;

public class rm implements ServerExecutable
{
	private byte code = AggregateResults.CODE_NORMAL;
	private String codeMessage = null;
	
	public Object execute(String command, String regionPath, Object arg) throws Exception
	{
		Region region = CacheFactory.getAnyInstance().getRegion(regionPath);
		if (region == null) {
			code = AggregateResults.CODE_ERROR;
			codeMessage = "Undefined region: " + regionPath;
			return null;
		}
		Object[] keys = (Object[])arg;
		
		for (int i = 0; i < keys.length; i++) {
			try {
				region.remove(keys[i]);
			} catch (Exception ex) {
				// ignore
			}
		}
		return new GfshData(null);
	}

	public byte getCode()
	{
		return code;
	}
	
	public String getCodeMessage()
	{
		return codeMessage;
	}
}
