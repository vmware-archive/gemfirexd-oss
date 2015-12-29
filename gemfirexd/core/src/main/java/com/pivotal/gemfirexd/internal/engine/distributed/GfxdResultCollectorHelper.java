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

package com.pivotal.gemfirexd.internal.engine.distributed;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.access.GemFireTransaction;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.engine.locks.GfxdLockSet;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.store.raw.ContainerHandle;

/**
 * Some helper methods for {@link GfxdResultCollector} implementations.
 * 
 * @author swale
 */
public final class GfxdResultCollectorHelper extends ReentrantLock {

  private static final long serialVersionUID = 5295482892857557452L;

  private Set<DistributedMember> members;

  private Collection<GemFireContainer> containersToClose;

  private GemFireTransaction tran;

  private int numRefs;

  /**
   * @see GfxdResultCollector#setResultMembers(Set)
   */
  public final void setResultMembers(Set<DistributedMember> members) {
    this.members = members;
  }

  /**
   * @see GfxdResultCollector#getResultMembers()
   */
  public final Set<DistributedMember> getResultMembers() {
    return this.members;
  }

  public final void addResultMember(final DistributedMember member) {
    if (member != null) {
      final Set<DistributedMember> members = this.members;
      if (members != null) {
        // TODO: SW: instead of sync on members, can use the helper to lock
        synchronized (members) {
          members.add(member);
        }
      }
    }
  }

  /**
   * @see GfxdResultCollector#setupContainersToClose(Collection,
   *      GemFireTransaction)
   */
  public final boolean setupContainersToClose(final GfxdResultCollector<?> rc,
      final Collection<GemFireContainer> containers,
      final GemFireTransaction tran) throws StandardException {
    if (containers != null && tran != null && containers.size() > 0) {
      if (GemFireXDUtils.TraceRSIter) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_RSITER,
            "GfxdResultCollectorHelper#setupContainersToClose: "
                + "for ResultCollector " + rc + " setting containers: "
                + containers);
      }
      this.lock();
      for (GemFireContainer container : containers) {
        container.open(tran, ContainerHandle.MODE_READONLY);
      }
      this.containersToClose = containers;
      this.tran = tran;
      this.numRefs = 0;
      this.unlock();
      tran.getLockSpace().rcSet(rc);
      return true;
    }
    return false;
  }

  /**
   * Close the containers setup with
   * {@link #setupContainersToClose(Collection, GemFireTransaction)} releasing
   * any read/write locks obtained on them in this Transaction.
   */
  public final void closeContainers(final GfxdResultCollector<?> rc,
      boolean rcEnd) {
    this.lock();
    try {
      final Collection<GemFireContainer> closeContainers =
          this.containersToClose;
      if (closeContainers != null) {
        // check that all references have invoked closeContainers
        if (removeReference()) {
          if (GemFireXDUtils.TraceRSIter) {
            SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_RSITER,
                "GfxdResultCollectorHelper#closeContainers: "
                    + "clearing ResultCollector " + rc
                    + " and closing containers: " + closeContainers);
          }
          // release the DML locks
          closeContainers(closeContainers, rc, this.tran);
        }
        else if (rcEnd) {
          if (GemFireXDUtils.TraceRSIter) {
            SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_RSITER,
                "GfxdResultCollectorHelper#closeContainers: force "
                    + "end ResultCollector " + rc);
          }
          final GemFireTransaction tran = this.tran;
          final GfxdLockSet lockSet;
          if (tran != null && (lockSet = tran.getLockSpace()) != null) {
            lockSet.rcEnd(rc);
          }
        }
      }
    } finally {
      this.unlock();
    }
  }

  public final void clear(final GfxdResultCollector<?> rc) {
    if (GemFireXDUtils.TraceRSIter) {
      SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_RSITER,
          "GfxdResultCollectorHelper#clear: end ResultCollector " + rc);
    }
    this.lock();
    try {
      assert this.numRefs >= 0: "clear: unexpected numRefs=" + this.numRefs;

      // this.members = null;
      if (this.numRefs > 0) {
        final Collection<GemFireContainer> closeContainers =
            this.containersToClose;
        if (closeContainers != null) {
          closeContainers(closeContainers, rc, this.tran);
        }
        this.numRefs = 0;
      }
    } finally {
      this.unlock();
    }
  }

  private void closeContainers(
      final Collection<GemFireContainer> closeContainers,
      final GfxdResultCollector<?> rc, final GemFireTransaction tran) {
    assert this.isHeldByCurrentThread(): "closeContainers: lock not held";

    GemFireContainer container = null;
    try {
      Iterator<GemFireContainer> iter = closeContainers.iterator();
      while (iter.hasNext()) {
        container = iter.next();
        container.closeForEndTransaction(tran, false);
      }
      this.containersToClose = null;
      this.tran = null;
    } catch (RuntimeException ex) {
      final LogWriter logger = Misc.getCacheLogWriter();
      logger.error("GfxdResultCollectorHelper#closeContainers: "
          + "for ResultCollector " + rc + " unexpected exception "
          + "in closing container " + container, ex);
      throw ex;
    } finally {
      if (GemFireXDUtils.TraceRSIter) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_RSITER,
            "GfxdResultCollectorHelper#closeContainers: "
                + "end ResultCollector " + rc);
      }
      final GfxdLockSet lockSet;
      if (tran != null && (lockSet = tran.getLockSpace()) != null) {
        lockSet.rcEnd(rc);
      }
    }
  }

  /**
   * Add a reference to this helper so that the helper will wait for an
   * additional {@link #closeContainers(GfxdResultCollector)} call for this
   * reference before actually closing the containers.
   */
  public final void addReference() {
    this.lock();
    this.numRefs++;
    this.unlock();
  }

  /**
   * Remove a reference added by {@link #addReference()} and return true if the
   * current number of references (before removal) is zero.
   */
  private final boolean removeReference() {
    assert this.isHeldByCurrentThread(): "removeReference: lock not held";

    if (this.numRefs == 0) {
      return true;
    }
    this.numRefs--;
    return false;
  }
}
