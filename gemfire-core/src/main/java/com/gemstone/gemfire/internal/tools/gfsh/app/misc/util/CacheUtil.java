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
package com.gemstone.gemfire.internal.tools.gfsh.app.misc.util;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.util.GatewayHub;

public class CacheUtil
{
	public synchronized static void startAllHubs() throws IOException
	{
		Cache cache = CacheFactory.getAnyInstance();

		// start all hubs
		cache.getLogger().config("CacheUtil.startAllHubs(): Starting all gateway hubs");
		List list = cache.getGatewayHubs();
		if (list != null) {
			for (Iterator iterator = list.iterator(); iterator.hasNext();) {
				GatewayHub hub = (GatewayHub) iterator.next();
				hub.start(true);
			}
			
		}
		cache.getLogger().config("CacheUtil.startAllHubs(): Started all gateway hubs");
	}

	public synchronized static void stopAllHubs()
	{
		Cache cache = CacheFactory.getAnyInstance();
	
		// stop all hubs
		cache.getLogger().config("CacheUtil.startAllHubs(): Stopping all gateway hubs");
		List list = cache.getGatewayHubs();
		if (list != null) {
			for (Iterator iterator = list.iterator(); iterator.hasNext();) {
				GatewayHub hub = (GatewayHub) iterator.next();
				hub.stop();
			}
			
		}
		cache.getLogger().config("CacheUtil.startAllHubs(): All gateway hubs stopped");
	}
}
