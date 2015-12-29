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
/**
 * 
 */
package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.cache.locks.LockingPolicy;

/**
 * Messages that carry transaction information will implement this interface
 * 
 * @author sbawaska
 */
public interface TransactionMessage {

  /**
   * Returns the transaction id on the sender this message belongs to
   * 
   * @return the globally unique transaction id
   */
  public TXId getTXId();

  /**
   * Get the {@link TXStateInterface} to be used while executing this message
   * for the given region. This is normally setup in the
   * {@link TXManagerImpl#masqueradeAs} method by a call to
   * {@link #setTXState(TXStateInterface)} method.
   */
  public TXStateInterface getTXState(LocalRegion region);

  /**
   * Get the {@link TXStateInterface} set for this message using
   * {@link #setTXState(TXStateInterface)}.
   */
  public TXStateInterface getTXState();

  /**
   * Set the {@link TXStateInterface} for this message processing. Normally
   * setup in the {@link TXManagerImpl#masqueradeAs} method.
   */
  public void setTXState(TXStateInterface tx);

  /**
   * We do not want all the messages to start a remote transaction. e.g.
   * SizeMessage. If this method returns true, a transaction will be created if
   * none exists
   * 
   * @return true if this message can start a remote transaction, false
   *         otherwise
   */
  public boolean canStartRemoteTransaction();

  /**
   * @see DistributionMessage#getSender()
   * @return the sender of this message
   */
  public InternalDistributedMember getSender();

  /**
   * Get the {@link LockingPolicy} to be used for execution of this message.
   */
  public LockingPolicy getLockingPolicy();

  /**
   * True when to use {@link TXStateProxy} for operations rather than the
   * underlying {@link TXState}. Should return true for function messages that
   * will distribute operations to more than one level.
   */
  public boolean useTransactionProxy();
}
