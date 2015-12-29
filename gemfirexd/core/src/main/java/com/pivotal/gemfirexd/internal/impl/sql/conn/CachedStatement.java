/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.conn.CachedStatement

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

package com.pivotal.gemfirexd.internal.impl.sql.conn;

// GemStone changes BEGIN
// GemStone changes END






import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.jdbc.GemFireXDRuntimeException;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.cache.Cacheable;
import com.pivotal.gemfirexd.internal.iapi.services.context.ContextManager;
import com.pivotal.gemfirexd.internal.iapi.services.monitor.Monitor;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.PreparedStatement;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.impl.sql.GenericPreparedStatement;
import com.pivotal.gemfirexd.internal.impl.sql.GenericStatement;

/**
*/
public class CachedStatement implements Cacheable {

	private GenericPreparedStatement ps;
	private Object identity;

	public CachedStatement() {
	}

	/**
	 * Get the PreparedStatement that is associated with this Cacheable
	 */
	public GenericPreparedStatement getPreparedStatement() {
		return ps;
	}

	/* Cacheable interface */

	/**

	    @see Cacheable#clean
	*/
	public void clean(boolean forRemove) {
	}

	/**
	*/
	public Cacheable setIdentity(Object key) {

		identity = key;
		ps = new GenericPreparedStatement((GenericStatement) key);
		ps.setCacheHolder(this);

		return this;
	}

	/** @see Cacheable#createIdentity */
	public Cacheable createIdentity(Object key, Object createParameter) {
		if (SanityManager.DEBUG)
			SanityManager.THROWASSERT("Not expecting any create() calls");

		return null;

	}

	/** @see Cacheable#clearIdentity */
	public void clearIdentity() {

// GemStone changes BEGIN
	  // also remove from DependencyManager
	  try {
	    // we expect LCC to be non-null here since clearIdentity() will
	    // be invoked by the same thread that does an insert into the
	    // cache i.e. from GenericStatement#prepMinion()
	    final LanguageConnectionContext lcc = Misc
	        .getLanguageConnectionContext();
	    Misc.getMemStore().getDatabase().getDataDictionary()
	        .getDependencyManager().clearDependencies(lcc, this.ps);
	  } catch (StandardException ex) {
	    throw GemFireXDRuntimeException.newRuntimeException(
	        "CachedStatement#clearIdentity: unexpected exception in " +
	        "clearing the dependencies for " + this.ps, ex);
	  }
// GemStone changes END
		if (SanityManager.DEBUG)
			SanityManager.DEBUG("StatementCacheInfo","CLEARING IDENTITY: "+ps.getSource());
		ps.setCacheHolder(null);

		identity = null;
		ps = null;
	}

	/** @see Cacheable#getIdentity */
	public Object getIdentity() {
		return identity;
	}

	/** @see Cacheable#isDirty */
	public boolean isDirty() {
		return false;
	}

	/* Cacheable interface */
}
