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

import java.util.List;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.internal.tools.gfsh.aggregator.AggregateResults;
import com.gemstone.gemfire.internal.tools.gfsh.app.ServerExecutable;
import com.gemstone.gemfire.internal.tools.gfsh.app.cache.data.ListMessage;
import com.gemstone.gemfire.internal.tools.gfsh.app.cache.data.MapMessage;
import com.gemstone.gemfire.internal.tools.gfsh.app.function.GfshData;

public class ls implements ServerExecutable
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
		
		Cache cache = region.getCache();
		ListMessage topMessage = new ListMessage();
		if (command.startsWith("ls -c")) {
			List<CacheServer> cacheServerList = cache.getCacheServers();
			if (cacheServerList.size() > 0) {
				for (CacheServer cacheServer : cacheServerList) {
					MapMessage cacheServerMessage = new MapMessage();
					String groups[] = cacheServer.getGroups();
					if (groups.length > 0) {
						String groupsStr = "";
						for (int i = 0; i < groups.length; i++) {
							groupsStr += groups[i];
							if (i < groups.length - 1) {
								groupsStr += ", ";
							}
						}
						cacheServerMessage.put("ServerGroups", groupsStr);
					} else {
						cacheServerMessage.put("ServerGroups", "");
					}
					
					cacheServerMessage.put("BindAddress", cacheServer.getBindAddress());
					cacheServerMessage.put("HostnameForClients", cacheServer.getHostnameForClients());
					cacheServerMessage.put("LoadPollInterval", cacheServer.getLoadPollInterval());
					cacheServerMessage.put("MaxConnections", cacheServer.getMaxConnections());
					cacheServerMessage.put("MaximumMessageCount", cacheServer.getMaximumMessageCount());
					cacheServerMessage.put("MaximumTimeBetweenPings", cacheServer.getMaximumTimeBetweenPings());
					cacheServerMessage.put("MaxThreads", cacheServer.getMaxThreads());
					cacheServerMessage.put("MessageTimeToLive", cacheServer.getMessageTimeToLive());
					cacheServerMessage.put("NotifyBySubscription", cacheServer.getNotifyBySubscription());
					cacheServerMessage.put("Port", cacheServer.getPort());
					cacheServerMessage.put("SocketBufferSize", cacheServer.getSocketBufferSize());
					cacheServerMessage.put("TcpNoDelay", cacheServer.getTcpNoDelay());
					
					topMessage.add(cacheServerMessage);
				}
			}
		}
		
		return new GfshData(topMessage);
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
