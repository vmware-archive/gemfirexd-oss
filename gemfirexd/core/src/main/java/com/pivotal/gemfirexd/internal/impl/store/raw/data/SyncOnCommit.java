/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.store.raw.data.SyncOnCommit

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

package com.pivotal.gemfirexd.internal.impl.store.raw.data;






import com.pivotal.gemfirexd.internal.catalog.UUID;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.locks.Lockable;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.store.raw.ContainerHandle;
import com.pivotal.gemfirexd.internal.iapi.store.raw.ContainerKey;
import com.pivotal.gemfirexd.internal.iapi.store.raw.ContainerLock;
import com.pivotal.gemfirexd.internal.iapi.store.raw.LockingPolicy;
import com.pivotal.gemfirexd.internal.iapi.store.raw.Page;
import com.pivotal.gemfirexd.internal.iapi.store.raw.RecordHandle;
import com.pivotal.gemfirexd.internal.iapi.store.raw.data.RawContainerHandle;
import com.pivotal.gemfirexd.internal.iapi.store.raw.log.LogInstant;
import com.pivotal.gemfirexd.internal.iapi.store.raw.xact.RawTransaction;

import java.util.Observable;

/**
	Flush all pages for a table on a commit
*/

public class SyncOnCommit extends ContainerHandleActionOnCommit {

	public SyncOnCommit(ContainerKey identity) {

		super(identity);
	}

	public void update(Observable obj, Object arg) {

		if (SanityManager.DEBUG) {
			if (arg == null)
				SanityManager.THROWASSERT("still on observr list " + this);
		}

		if (arg.equals(RawTransaction.COMMIT)) {
			openContainerAndDoIt((RawTransaction) obj);
		}

		// remove this object if we are commiting, aborting or the container is being dropped
		if (arg.equals(RawTransaction.COMMIT) || arg.equals(RawTransaction.ABORT)
			|| arg.equals(identity)) {
			obj.deleteObserver(this);
		}
	}

	/**
		@exception StandardException Standard Derby error policy
	 */
	protected void doIt(BaseContainerHandle handle)
		throws StandardException {
		handle.container.flushAll();
	}
}
