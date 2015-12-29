/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.services.cache.ClockFactory

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

/*
 * Changes for GemFireXD distributed data platform (some marked by "GemStone changes")
 *
 * Portions Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
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

package com.pivotal.gemfirexd.internal.impl.services.cache;



import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.cache.CacheFactory;
import com.pivotal.gemfirexd.internal.iapi.services.cache.CacheManager;
import com.pivotal.gemfirexd.internal.iapi.services.cache.Cacheable;
import com.pivotal.gemfirexd.internal.iapi.services.cache.CacheableFactory;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;

import java.util.Properties;

/**
  Multithreading considerations: no need to be MT-safe, caller (module control)
  provides synchronization. Besides, this class is stateless.
*/

public class ClockFactory implements CacheFactory {

	/**
		Trace flag to display cache statistics
	*/
// GemStone changes BEGIN
	/* (original code)
	public static final String CacheTrace = SanityManager.DEBUG ? "CacheTrace" : null;
	*/
// GemStone changes END

	public ClockFactory() {
	}



	/*
	** Methods of CacheFactory
	*/

	public CacheManager newCacheManager(CacheableFactory holderFactory, String name, int initialSize, int maximumSize)
	{

		if (initialSize <= 0)
			initialSize = 1;

		return new Clock(holderFactory, name, initialSize, maximumSize, false);
	}
	
	public CacheManager newSizedCacheManager(CacheableFactory holderFactory, String name,
										int initialSize, long maximumSize)
	{

		if (initialSize <= 0)
			initialSize = 1;

		return new Clock(holderFactory, name, initialSize, maximumSize, true);
	}
}
