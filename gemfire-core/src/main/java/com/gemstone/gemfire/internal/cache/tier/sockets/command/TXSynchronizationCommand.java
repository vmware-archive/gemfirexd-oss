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

import com.gemstone.gemfire.cache.client.internal.TXSynchronizationOp.CompletionType;
import com.gemstone.gemfire.internal.cache.tier.Command;
import com.gemstone.gemfire.internal.cache.tier.sockets.BaseCommand;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection;

import java.io.IOException;

public class TXSynchronizationCommand extends BaseCommand {

  private final static TXSynchronizationCommand singleton = new TXSynchronizationCommand();

  public static Command getCommand() {
    return singleton;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.tier.sockets.BaseCommand#shouldMasqueradeForTx(com.gemstone.gemfire.internal.cache.tier.sockets.Message, com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection)
   */
  @Override
  protected boolean shouldMasqueradeForTx(Message msg, ServerConnection servConn) {
    // masquerading is done in the waiting thread pool
    return false;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.tier.sockets.BaseCommand#cmdExecute(com.gemstone.gemfire.internal.cache.tier.sockets.Message, com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection, long)
   */
  @Override
  public void cmdExecute(final Message msg, final ServerConnection servConn, long start)
      throws IOException, ClassNotFoundException, InterruptedException {
    
    servConn.setAsTrue(REQUIRES_RESPONSE);

    CompletionType type = CompletionType.values()[msg.getPart(0).getInt()];
    /*int txIdInt =*/ msg.getPart(1).getInt();  // [bruce] not sure if we need to transmit this
    if (type == CompletionType.AFTER_COMPLETION) {
      msg.getPart(2);
    } else {
    }
    // TODO: merge: from trunk; see if below can be useful in new TX impl
    /*
    final TXManagerImpl txMgr = (TXManagerImpl)servConn.getCache().getCacheTransactionManager();
    final InternalDistributedMember member = (InternalDistributedMember)servConn.getProxyID().getDistributedMember();

    // get the tx state without associating it with this thread.  That's done later
    final TXStateProxy txProxy = txMgr.masqueradeAs(msg, member, true);
    
    // we have to run beforeCompletion and afterCompletion in the same thread
    // because beforeCompletion obtains locks for the thread and afterCompletion
    // releases them
    if (txProxy != null) {
      try {
        if (type == CompletionType.BEFORE_COMPLETION) {
          Runnable beforeCompletion = new Runnable() {
                @SuppressWarnings("synthetic-access")
                public void run() {
                  TXStateProxy txState = null;
                  Throwable failureException = null;
                  try {
                    txState = txMgr.masqueradeAs(msg, member, false);
                    if (logger.fineEnabled()) {
                      logger.fine("Executing beforeCompletion() notification for transaction " + msg.getTransactionId());
                    }
                    txState.setIsJTA(true);
                    txState.beforeCompletion();
                    try {
                      writeReply(msg, servConn);
                    } catch (IOException e) {
                      if (logger.fineEnabled()) {
                        logger.fine("Problem writing reply to client", e);
                      }
                    }
                    servConn.setAsTrue(RESPONDED);
                  } catch (ReplyException e) {
                    failureException = e.getCause();
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                  } catch (Exception e) {
                    failureException = e;
                  } finally {
                    txMgr.unmasquerade(txState);
                  }
                  if (failureException != null) {
                    try {
                      writeException(msg, failureException, false, servConn);
                    } catch (IOException ioe) {
                      if (logger.fineEnabled()) {
                        logger.fine("Problem writing reply to client", ioe);
                      }
                    }
                    servConn.setAsTrue(RESPONDED);
                  }
                }
              };
          TXSynchronizationRunnable sync = new TXSynchronizationRunnable(beforeCompletion);
          txProxy.setSynchronizationRunnable(sync);
          Executor exec = InternalDistributedSystem.getConnectedInstance().getDistributionManager().getWaitingThreadPool();
          exec.execute(sync);
          sync.waitForFirstExecution();
        } else {
          Runnable afterCompletion = new Runnable() {
                @SuppressWarnings("synthetic-access")
                public void run() {
                  TXStateProxy txState = null;
                  try {
                    txState = txMgr.masqueradeAs(msg, member, false);
                    int status = statusPart.getInt();
                    if (logger.fineEnabled()) {
                      logger.fine("Executing afterCompletion("+status+") notification for transaction " + msg.getTransactionId());
                    }
                    txState.setIsJTA(true);
                    txState.afterCompletion(status);
                    // GemFire commits during afterCompletion - send the commit info back to the client
                    // where it can be applied to the local cache
                    TXCommitMessage cmsg = txState.getCommitMessage();
                    try {
                      CommitCommand.writeCommitResponse(cmsg, msg, servConn);
                      txMgr.removeHostedTXState(txState.getTxId());
                    } catch (IOException e) {
                      // not much can be done here
                      if (logger.fineEnabled()) {
                        logger.fine("Problem writing reply to client", e);
                      }
                    }
                    servConn.setAsTrue(RESPONDED);
                  } catch (RuntimeException e) {
                    try {
                      writeException(msg, e, false, servConn);
                    } catch (IOException ioe) {
                      if (logger.fineEnabled()) {
                        logger.fine("Problem writing reply to client", ioe);
                      }
                    }
                    servConn.setAsTrue(RESPONDED);
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                  } finally {
                    txMgr.unmasquerade(txState);
                  }
                }
              };
          // if there was a beforeCompletion call then there will be a thread
          // sitting in the waiting pool to execute afterCompletion.  Otherwise
          // we have failed-over and may need to do beforeCompletion & hope that it works
          TXSynchronizationRunnable sync = txProxy.getSynchronizationRunnable();
          if (sync != null) {
            sync.runSecondRunnable(afterCompletion);
          } else {
            if (statusPart.getInt() == Status.STATUS_COMMITTED) {
              TXStateProxy txState = txMgr.masqueradeAs(msg, member, false);
              try {
                if (logger.fineEnabled()) {
                  logger.fine("Executing beforeCompletion() notification for transaction " + msg.getTransactionId() + " after failover");
                }
                txState.setIsJTA(true);
                txState.beforeCompletion();
              } finally {
                txMgr.unmasquerade(txState);
              }
            }
            afterCompletion.run();
          }
        }
      } catch (Exception e) {
        writeException(msg, MessageType.EXCEPTION, e, false, servConn);
        servConn.setAsTrue(RESPONDED);
      }
      if (logger.fineEnabled()) {
        logger.fine("Sent tx synchronization response");
      }
    }
    */
  }

}
