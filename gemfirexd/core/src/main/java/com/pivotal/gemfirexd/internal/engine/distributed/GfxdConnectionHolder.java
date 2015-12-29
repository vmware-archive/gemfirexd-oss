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

import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import com.gemstone.gemfire.distributed.internal.MembershipListener;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.concurrent.CustomEntryConcurrentHashMap;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedConnection;

/**
 * 
 * @author Asif
 */
public final class GfxdConnectionHolder implements MembershipListener {

  private final CustomEntryConcurrentHashMap<Long, GfxdConnectionWrapper>
      idToConnMap;

  private static final GfxdConnectionHolder singleton;

  static {
    singleton = new GfxdConnectionHolder();
  }

  private GfxdConnectionHolder() {
    this.idToConnMap =
        new CustomEntryConcurrentHashMap<Long, GfxdConnectionWrapper>();
  }

  public static GfxdConnectionHolder getHolder() {
    return singleton;
  }

  /**
   * This method creates a new GfxdConnectionWrapper if not already present &
   * stores it against the conenction ID passed. Will also ensure that the
   * {@link EmbedConnection} wrapped has a hard reference rather than a soft
   * reference.
   * 
   * @param connId
   *          Long Connection ID against which GfxdConnectionWrapper is stored
   * @param remoteDDL
   *          True if this connection is required for a DDL execution
   * 
   * @return GfxdConnectionWrapper
   * 
   * @throws SQLException
   */
  public GfxdConnectionWrapper createWrapper(String defaultSchema, long connId,
      boolean remoteDDL, Properties props) throws SQLException {
    GfxdConnectionWrapper wrapper = null;
    assert connId != EmbedConnection.UNINITIALIZED:
      "unexpected uninitialized connection";
    boolean isCached = (connId != EmbedConnection.CHILD_NOT_CACHEABLE);
    Long id = null;
    if (isCached) {
      id = Long.valueOf(connId);
      wrapper = getWrapper(id, false);
    }
    if (wrapper == null) {
      // for DDLs make a hard-reference since DDL commit/rollback needs to find
      // the connection
      wrapper = new GfxdConnectionWrapper(defaultSchema, connId, isCached, true,
          remoteDDL, props);
      if (isCached) {
        assert id != null;
        GfxdConnectionWrapper old = this.idToConnMap.putIfAbsent(id, wrapper);
        if (old != null) {
          wrapper.close();
          wrapper = old;
        }
      }
    }
    else {
      // default schema may have changed
      wrapper.setDefaultSchema(defaultSchema);
    }

    return wrapper;
  }

  /**
   * This method does not create a GfxdConnectionWrapper if not created yet.
   * 
   * @param connId
   *          Long Connection ID against which GfxdConnectionWrapper is stored
   * 
   * @return instance of GfxdConnectionWrapper or null
   */
  public GfxdConnectionWrapper getExistingWrapper(Long connId) {
    return getWrapper(connId, true);
  }

  /**
   * This method does not create a GfxdConnectionWrapper if not created yet.
   * 
   * @param connId
   *          Long Connection ID against which GfxdConnectionWrapper is stored
   * @param removeClosed
   *          if true then it causes a closed connection to be removed from
   *          GfxdConnectionWrapper; callers should normally invoked
   *          {@link GfxdConnectionWrapper#getConnectionForSynchronization()} to
   *          allow the connection to be recreated
   * 
   * @return instance of GfxdConnectionWrapper or null
   */
  public GfxdConnectionWrapper getWrapper(Long connId, boolean removeClosed) {
    assert connId.longValue() != EmbedConnection.UNINITIALIZED:
      "unexpected uninitialized connection requested";
    if (connId.longValue() == EmbedConnection.CHILD_NOT_CACHEABLE) {
      return null;
    }
    GfxdConnectionWrapper wrapper = this.idToConnMap.get(connId);
    /* not doing this any longer; causes SQL map to be lost (#43203)
     * wrapper.getConnectionForSynchronization() will recreate if closed
    if (wrapper != null) {
      // wrapper.isClosed() call need not be synchronized since removeClosed
      // is only true for DDL statements which will sync out at source node
      if (removeClosed && wrapper.isClosed()) {
        // In case the connection got closed due to some assert or other fatal
        // error previously, remove this connection from map (see #40566).
        // Close the wrapper only if we successfully remove it from the map
        // otherwise someone else beat us to it.
        if (this.idToConnMap.remove(connId, wrapper)) {
          wrapper.close();
        }
        wrapper = null;
      }
    }
    */
    return wrapper;
  }

  public static GfxdConnectionWrapper getOrCreateWrapper(String defaultSchema,
      long connId, boolean remoteDDL, Properties props) throws SQLException {
    return getHolder().createWrapper(defaultSchema, connId, remoteDDL, props);
  }

  /** Clear the connection map. */
  public void clear() {
    GfxdConnectionWrapper wrapper;
    for (Long connId : this.idToConnMap.keySet()) {
      if ((wrapper = removeWrapper(connId)) != null) {
        wrapper.close();
      }
    }
    // clear the map just to be sure
    this.idToConnMap.clear();
  }

  public final GfxdConnectionWrapper removeWrapper(Long connId) {
    return this.idToConnMap.remove(connId);
  }

  @Override
  public void memberJoined(InternalDistributedMember id) {
  }

  @Override
  public void memberDeparted(InternalDistributedMember id, boolean crashed) {
    GfxdConnectionWrapper wrapper;
    for (Long connId : this.idToConnMap.keySet()) {
      if (GemFireXDUtils.getDistributedMemberFromUUID(connId).equals(id)) {
        if ((wrapper = removeWrapper(connId)) != null) {
          wrapper.close();
        }
      }
    }
  }

  @Override
  public void memberSuspect(InternalDistributedMember id,
      InternalDistributedMember whoSuspected) {
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void quorumLost(Set<InternalDistributedMember> failures,
      List<InternalDistributedMember> remaining) {
  }

  // added for testing cleanup of statements inside wrapper
  public ConcurrentMap<Long, GfxdConnectionWrapper> getWrapperMap() {
    return this.idToConnMap;
  }
}
