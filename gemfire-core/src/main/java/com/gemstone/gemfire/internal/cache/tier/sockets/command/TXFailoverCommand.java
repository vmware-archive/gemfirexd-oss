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
package com.gemstone.gemfire.internal.cache.tier.sockets.command;

import java.io.IOException;

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.cache.tier.Command;
import com.gemstone.gemfire.internal.cache.tier.sockets.BaseCommand;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection;

/**
 * Used for bootstrapping txState/PeerTXStateStub on the server.
 * This command is send when in client in a transaction is about
 * to failover to this server
 * @author sbawaska
 */
public class TXFailoverCommand extends BaseCommand {

  private static final Command singleton = new TXFailoverCommand();
  
  public static Command getCommand() {
    return singleton;
  }

  private TXFailoverCommand() {
  }
  
  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long start)
      throws IOException, ClassNotFoundException, InterruptedException {
    servConn.setAsTrue(REQUIRES_RESPONSE);
    // Build the TXId for the transaction
    InternalDistributedMember client = (InternalDistributedMember) servConn.getProxyID().getDistributedMember();
    int uniqId = msg.getTransactionId();
    if (logger.fineEnabled()) {
      logger.fine("TX: Transaction " + uniqId + " from " + client + " is failing over to this server");
    }
    // TODO: merge: from trunk; see if below can be useful for new TX model
    /*
    TXId txId = new TXId(client, uniqId);
    TXManagerImpl mgr = (TXManagerImpl) servConn.getCache().getCacheTransactionManager();
    mgr.waitForCompletingTransaction(txId); // in case it's already completing here in another thread
    if (mgr.isHostedTxRecentlyCompleted(txId)) {
      writeReply(msg, servConn);
      servConn.setAsTrue(RESPONDED);
      mgr.removeHostedTXState(txId);
      return;
    }
    boolean wasInProgress = mgr.setInProgress(true); // fixes bug 43350
    TXStateProxy tx = mgr.getTXState();
    Assert.assertTrue(tx != null);
    if (!tx.isRealDealLocal()) {
      // send message to all peers to find out who hosts the transaction
      FindRemoteTXMessageReplyProcessor processor = FindRemoteTXMessage.send(servConn.getCache(), txId);
      try {
        processor.waitForRepliesUninterruptibly();
      } catch (ReplyException e) {
        e.handleAsUnexpected();
      }
      InternalDistributedMember hostingMember = processor.getHostingMember();
      if (hostingMember == null && processor.getTxCommitMessage() == null) {
        // bug #42228 and bug #43504 - this cannot return until the current view
        // has been installed by all members, so that dlocks are released and
        // the same keys can be used in a new transaction by the same client thread
        GemFireCacheImpl cache = (GemFireCacheImpl)servConn.getCache();
        try {
          WaitForViewInstallation.send((DistributionManager)cache.getDistributionManager());
        } catch (InterruptedException e) {
          cache.getDistributionManager().getCancelCriterion().checkCancelInProgress(e);
          Thread.currentThread().interrupt();
        }
        writeException(msg, new TransactionDataNodeHasDepartedException("Could not find transaction host for "+txId), false, servConn);
        servConn.setAsTrue(RESPONDED);
        mgr.removeHostedTXState(txId);
        return;
      }
      if (processor.getTxCommitMessage() != null) {
        if (logger.fineEnabled()) {
          logger.fine("TX: for txId:"+txId+" found a recently completed tx on:"+hostingMember);
        }
        mgr.saveTXCommitMessageForClientFailover(txId, processor.getTxCommitMessage());
      } else {
        if (logger.fineEnabled()) {
          logger.fine("TX: txState is not local, bootstrapping PeerTXState stub for targetNode:"+hostingMember);
        }
        // inject the real deal
        tx.setLocalTXState(new PeerTXStateStub(tx, hostingMember, client));
      }
    }
    if (!wasInProgress) {
      mgr.setInProgress(false);
    }
    */
    writeReply(msg, servConn);
    servConn.setAsTrue(RESPONDED);
  }
}
