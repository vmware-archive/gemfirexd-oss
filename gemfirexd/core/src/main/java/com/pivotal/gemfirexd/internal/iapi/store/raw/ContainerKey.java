/*

   Derby - Class com.pivotal.gemfirexd.internal.iapi.store.raw.ContainerKey

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

package com.pivotal.gemfirexd.internal.iapi.store.raw;


import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

import com.pivotal.gemfirexd.internal.iapi.services.io.CompressedNumber;
import com.pivotal.gemfirexd.internal.iapi.services.locks.Latch;
import com.pivotal.gemfirexd.internal.iapi.services.locks.Lockable;
import com.pivotal.gemfirexd.internal.iapi.services.locks.VirtualLockTable;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.util.Matchable;

import java.util.Hashtable;

/**
	A key that identifies a Container within the RawStore.
	<BR> MT - Immutable
*/
public final class ContainerKey implements Matchable, Lockable
{
	private final long	segmentId;		// segment identifier
	private final long	containerId;	// container identifier

	/**
		Create a new ContainerKey
	*/
	public ContainerKey(long segmentId, long containerId) {
		this.segmentId = segmentId;
		this.containerId = containerId;
	}

	/**
		Return my identifier within the segment
	*/
	public long getContainerId() {
		return containerId;
	}

	/**
		Return my segment identifier
	*/
	public long getSegmentId() {
		return segmentId;
	}

	/*
	** Methods to read and write ContainerKeys.
	*/

	public void writeExternal(ObjectOutput out) throws IOException 
	{
		CompressedNumber.writeLong(out, segmentId);
		CompressedNumber.writeLong(out, containerId);
	}

	public static ContainerKey read(ObjectInput in) throws IOException
	{
		long sid = CompressedNumber.readLong(in);
		long cid = CompressedNumber.readLong(in);

		return new ContainerKey(sid, cid);
	}

	/*
	** Methods of Object
	*/

	public boolean equals(Object other) {
		if (other == this)
			return true;

		if (other instanceof ContainerKey) {
			ContainerKey otherKey = (ContainerKey) other;

			return (containerId == otherKey.containerId) &&
					(segmentId == otherKey.segmentId);
		} else {
			return false;
		}
	}

	public final int hashCode() {

		return (int) (segmentId ^ containerId);
	}

	public String toString() {

		return "Container(" + segmentId + ", " + containerId + ")";
	}

	/*
	** methods of Matchable
	*/

	public boolean match(Object key) {
		// instance of ContainerKey?
		if (equals(key))
			return true;

		if (key instanceof PageKey)
			return equals(((PageKey) key).getContainerId());

		if (key instanceof RecordHandle) {
			return equals(((RecordHandle) key).getContainerId());
		}
		return false;
	}
	/*
	** Methods of Lockable
	*/

	public void lockEvent(Latch lockInfo) {
	}
	 

	public boolean requestCompatible(Object requestedQualifier, Object grantedQualifier) {
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(requestedQualifier instanceof ContainerLock);
			SanityManager.ASSERT(grantedQualifier instanceof ContainerLock);
		}

		ContainerLock clRequested = (ContainerLock) requestedQualifier;
		ContainerLock clGranted  = (ContainerLock) grantedQualifier;

		return clRequested.isCompatible(clGranted);
	}

	/**
		This method will only be called if requestCompatible returned false.
		This results from two cases, some other compatabilty space has some
		lock that would conflict with the request, or this compatability space
		has a lock tha
	*/
	public boolean lockerAlwaysCompatible() {
		return true;
	}

	public void unlockEvent(Latch lockInfo) {
	}

	/**
		This lockable wants to participate in the Virtual Lock table.
	 */
	public boolean lockAttributes(int flag, Hashtable attributes)
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(attributes != null, 
				"cannot call lockProperties with null attribute list");
		}

		if ((flag & VirtualLockTable.TABLE_AND_ROWLOCK) == 0)
			return false;

		attributes.put(VirtualLockTable.CONTAINERID, 
// GemStone changes BEGIN
					   // changed to use valueOf
					   Long.valueOf(getContainerId()));
					   /* (original code)
					   new Long(getContainerId()));
					   */
// GemStone changes END
		attributes.put(VirtualLockTable.LOCKNAME, "Tablelock");
		attributes.put(VirtualLockTable.LOCKTYPE, "TABLE");

		// attributes.put(VirtualLockTable.SEGMENTID, new Long(identity.getSegmentId()));

		return true;
	}
// GemStone changes BEGIN

  /** The size of {@link #zeroSegmentCache}. */
  private static final int KEYCACHE_SIZE = 300;

  /**
   * A cache for {@link ContainerKey}s for the special case when segment ID is
   * zero which is the case most of the time currently.
   */
  private static final ContainerKey[] zeroSegmentCache =
    new ContainerKey[KEYCACHE_SIZE];

  static {
    for (int index = 0; index < KEYCACHE_SIZE; ++index) {
      zeroSegmentCache[index] = new ContainerKey(0, (long)index);
    }
  }

  public static ContainerKey valueOf(long segmentId, long containerId) {
    if (segmentId == 0 && containerId >= 0 && containerId < KEYCACHE_SIZE) {
      return zeroSegmentCache[(int)containerId];
    }
    return new ContainerKey(segmentId, containerId);
  }

// GemStone changes END
}
