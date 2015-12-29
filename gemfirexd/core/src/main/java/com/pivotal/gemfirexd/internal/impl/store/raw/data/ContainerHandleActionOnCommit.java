/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.store.raw.data.ContainerHandleActionOnCommit

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

/**
	An abstract class that opens the container at commit and delegates
	the actual work to a sub-class.
*/

public abstract class ContainerHandleActionOnCommit extends ContainerActionOnCommit {

	public ContainerHandleActionOnCommit(ContainerKey identity) {

		super(identity);
	}

	/*
	**	Methods of Observer
	*/

	/**
		Open the container and call the doIt method
	*/
	public void openContainerAndDoIt(RawTransaction xact) {

		BaseContainerHandle handle = null;
		try {
			handle = (BaseContainerHandle) xact.openContainer(identity, (LockingPolicy) null, 
				ContainerHandle.MODE_FORUPDATE | ContainerHandle.MODE_NO_ACTIONS_ON_COMMIT);

			// if the handle is null, the container may have been removed by a previous observer.
			if (handle != null) {
				try {
					doIt(handle);
				} catch (StandardException se) {
					xact.setObserverException(se);
				}
			}

		} catch (StandardException se) {

			// if we get this exception, then the container is readonly.
			// no problem if we can't open an closed temp container.
			if (identity.getSegmentId()  != ContainerHandle.TEMPORARY_SEGMENT)
				xact.setObserverException(se);
		} finally {
			if (handle != null)
				handle.close();
		}
	}

	protected abstract void doIt(BaseContainerHandle handle)
		throws StandardException;
}
