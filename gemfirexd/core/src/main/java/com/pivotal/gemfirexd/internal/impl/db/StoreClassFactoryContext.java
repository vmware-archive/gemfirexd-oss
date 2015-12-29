/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.db.StoreClassFactoryContext

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

package com.pivotal.gemfirexd.internal.impl.db;

import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.context.ContextManager;
import com.pivotal.gemfirexd.internal.iapi.services.loader.ClassFactory;
import com.pivotal.gemfirexd.internal.iapi.services.loader.ClassFactoryContext;
import com.pivotal.gemfirexd.internal.iapi.services.loader.JarReader;
import com.pivotal.gemfirexd.internal.iapi.services.locks.CompatibilitySpace;
import com.pivotal.gemfirexd.internal.iapi.services.property.PersistentSet;
import com.pivotal.gemfirexd.internal.iapi.store.access.AccessFactory;
import com.pivotal.gemfirexd.internal.iapi.store.access.TransactionController;

/**
*/
final class StoreClassFactoryContext extends ClassFactoryContext {

	private final AccessFactory store;
	private final JarReader	jarReader;

	StoreClassFactoryContext(ContextManager cm, ClassFactory cf, AccessFactory store, JarReader jarReader) {
		super(cm, cf);
		this.store = store;
		this.jarReader = jarReader;
	}

	public CompatibilitySpace getLockSpace() throws StandardException {
		if (store == null)
			return null;
		
// Gemstone changes begin.
		TransactionController tc = store.getTransaction(
		    getContextManager());
		if (tc == null || tc.isClosed()) {
		  return null;
		}
		return tc.getLockSpace();
// Gemstone changes end.
	}

	public PersistentSet getPersistentSet() throws StandardException {
		if (store == null)
			return null;
		
		// Gemstone changes begin.
		return store.getTransaction(getContextManager());
		// Gemstone changes end.
	}
	public JarReader getJarReader() {

		return jarReader;
	}
}

