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

import java.util.Set;

import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.membership.
    InternalDistributedMember;
import com.gemstone.gnu.trove.TObjectIntHashMap;

/**
 * Base class containing some helper common methods for ReplyProcessors that can
 * process more than one replies from a node.
 * 
 * @author swale
 * @since 7.0
 */
public abstract class GfxdWaitingReplyProcessorBase extends
    GfxdReplyMessageProcessor {

  protected TObjectIntHashMap waiters;

  public GfxdWaitingReplyProcessorBase(DM dm, Set<DistributedMember> members,
      boolean register) {
    super(dm, members, register);
  }

  public GfxdWaitingReplyProcessorBase(final DM dm,
      final InternalDistributedMember member, boolean register) {
    super(dm, member, register);
  }

  public final boolean hasWaiters() {
    return stillWaiting();
  }

  /**
   * Returns the current sequence ID for a waiting member stored in the map, or
   * 0 if not present.
   */
  protected final int getCurrSequenceId(final DistributedMember member) {
    assert Thread.holdsLock(this);

    if (this.waiters != null) {
      return this.waiters.get(member);
    }
    return 0;
  }

  protected final TObjectIntHashMap newWaitersMap() {
    assert Thread.holdsLock(this);

    if (this.waiters != null) {
      return this.waiters;
    }
    return (this.waiters = new TObjectIntHashMap());
  }

  protected abstract void addGrantedMember(final DistributedMember member);

  /**
   * Adds a GRANT reply from a member and returns true if there are more replies
   * expected from this member and false if it is done.
   */
  public final synchronized boolean addGrantedMember(
      final DistributedMember member, final int sequenceId) {
    assert sequenceId > 0;
    int newSequenceId = 0;
    if (sequenceId == 1
        || (newSequenceId = (sequenceId - getCurrSequenceId(member))) <= 1) {
      addGrantedMember(member);
      // no need to remove from waitingMembers since that will be done
      // by removeMember() via ReplyProcessor21#process()
      return false;
    }
    else {
      newSequenceId--;
      newWaitersMap().put(member, -newSequenceId);
      return true;
    }
  }

  /**
   * Adds a WAITING reply from a member and returns true if there are more
   * replies expected from this member and false if it is done.
   */
  public final synchronized boolean addWaitingMember(
      final DistributedMember member, final int sequenceId) {
    assert sequenceId >= 0;

    final TObjectIntHashMap waiters = newWaitersMap();
    if (sequenceId == 0) {
      waiters.put(member, 0);
    }
    else {
      int currSequenceId = getCurrSequenceId(member);
      if (currSequenceId != -1) {
        waiters.put(member, ++currSequenceId);
      }
      else {
        addGrantedMember(member);
        // no need to remove from waitingMembers since that will be done
        // by removeMember() via ReplyProcessor21#process()
        return false;
      }
    }
    return true;
  }

  /**
   * Process the given message and return true if there are still replies left
   * from the node (can happen with either {@link GfxdResponseCode#WAITING(int)}
   * or {@link GfxdResponseCode#GRANT(int)} since replies may come in any
   * order).
   */
  protected final boolean processResponseCode(GfxdReplyMessage reply,
      GfxdResponseCode responseCode) {
    if (responseCode.isWaiting()) {
      return addWaitingMember(reply.getSender(),
          responseCode.waitingSequenceId());
    }
    else if (responseCode.isGrant()) {
      return addGrantedMember(reply.getSender(),
          responseCode.grantedSequenceId());
    }
    return false;
  }

  @Override
  protected final boolean removeMember(InternalDistributedMember member,
      boolean departed) {
    final boolean result = super.removeMember(member, departed);
    removeMemberFromLists(member, departed);
    return result;
  }

  protected synchronized void removeMemberFromLists(
      final InternalDistributedMember member, boolean departed) {
    if (this.waiters != null) {
      this.waiters.remove(member);
    }
  }
}
