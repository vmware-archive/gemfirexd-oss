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
package util; 

import hydra.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.internal.cache.*;
import java.util.concurrent.TimeUnit;

import tx.*;

public class TxHelper {

  static CacheTransactionManager tm = CacheHelper.getCache()
      .getCacheTransactionManager();

   // HA tests may re-use an existing VM, allow application to reset tm
   public static void setTransactionManager() {
     tm = CacheHelper.getCache().getCacheTransactionManager();
   }

   /* TODO: TX: needs to be reworked for new TX impl
   private static ThreadLocal<List<TransactionalOperation>> recordedOperations =
     new ThreadLocal<List<TransactionalOperation>>() {
      @Override
      protected List<TransactionalOperation> initialValue() {
        return Collections.emptyList();
      }
   };
   
   public static void recordClientTXOperations() {
     ClientTXStateStub.setTransactionalOperationContainer(recordedOperations);
   }
   
   /**
    * This method may be used in a client to obtain the list of operations
    * performed in the current transaction.  The list is retained until
    * the next transaction begins.
    *
   public static List<TransactionalOperation> getClientTXOperations() {
     return recordedOperations.get();
   }
   */

   public static void begin() {
     Log.getLogWriter().info("BEGINNING TX");
     tm.begin();
     TxBB.getBB().getSharedCounters().increment(TxBB.TX_IN_PROGRESS);
     Log.getLogWriter().info("Began transaction with id " + tm.getTransactionId());
   }

   public static void commit() throws TransactionException {
     Log.getLogWriter().info("COMMITTING TX");
     tx.TxBB.getBB().getSharedCounters().decrement(TxBB.TX_IN_PROGRESS);

     TransactionId txid = tm.getTransactionId();

     Log.getLogWriter().info("Committing transaction with id " + txid.toString());
     try {
       tm.commit();
       Log.getLogWriter().info("Committed transaction with id " + txid.toString());
     } catch (ConflictException e) {
       checkCommitConflictException(e);
       throw e;
     }
   }
   
   /** Do a commit; throw an error if it fails */
   public static void commitExpectSuccess() {
      Log.getLogWriter().info("In commitExpectSuccess");
      tx.TxBB.getBB().getSharedCounters().decrement(TxBB.TX_IN_PROGRESS);
      TransactionId txid = tm.getTransactionId();
      try {
         tm.commit();
         Log.getLogWriter().info("Successful commit for transaction with id " + txid.toString());
      } catch (ConflictException e) {
         checkCommitConflictException(e);
         throw new TestException("Expected successful commit for transaction with id " +
                   txid.toString() + ", but got " + TestHelper.getStackTrace(e));
      }
   }
   
   /** Do a commit; throw an error if it succeeds */
   public static void commitExpectFailure() {
      Log.getLogWriter().info("In commitExpectFailure");
      tx.TxBB.getBB().getSharedCounters().decrement(TxBB.TX_IN_PROGRESS);
      TransactionId txid = tm.getTransactionId();
      try {
         tm.commit();
         throw new TestException("Expected failed commit, but got success for transaction with id " + 
                   tm.getTransactionId());
      } catch (ConflictException e) {
         checkCommitConflictException(e);
         // expect failure
         Log.getLogWriter().info("Transaction with id " + txid.toString() + " failed as expected");
      }
   }
   
   public static boolean exists() {
      return tm.exists();
   }

   public static boolean exists(TransactionId txId) {
      return tm.exists(txId);
   }

   public static TransactionId getTransactionId() {
      return tm.getTransactionId();
   }

   public static void rollback() {
     Log.getLogWriter().info("ROLLBACK TX");
     tx.TxBB.getBB().getSharedCounters().decrement(TxBB.TX_IN_PROGRESS);
     TransactionId txid = tm.getTransactionId();

     Log.getLogWriter().info("Rolling back transaction with id " + txid.toString());
     tm.rollback();
     Log.getLogWriter().info("Rolled back transaction with id " + txid.toString());
   }

   public static TransactionId suspend() {
     TransactionId txId = ((TXManagerImpl)tm).suspend();
     Log.getLogWriter().info("SUSPENDED TX: " + txId);
     return txId;
   }

   public static TXStateInterface internalSuspend() {
     TXStateInterface txContext = ((TXManagerImpl)tm).internalSuspend();
     if (txContext != null) {
        Log.getLogWriter().fine("SUSPENDED TX: " + txContext.toString());
     }
     return txContext;
   }

   public static boolean isSuspended(TransactionId txId) {
      return ((TXManagerImpl)tm).isSuspended(txId);
   }

   public static void resume(TransactionId txId) {
     Log.getLogWriter().info("RESUMING TX: " + txId);
     ((TXManagerImpl)tm).resume(txId);
   }

  public static boolean tryResume(TransactionId txId, long wait, TimeUnit timeUnit) {
     Log.getLogWriter().fine("Trying RESUME with wait (" + wait + "): " + txId);
     return ((TXManagerImpl)tm).tryResume(txId, wait, timeUnit);
  }

   public static void internalResume(TXStateInterface txContext) {
     if (txContext != null) {
        Log.getLogWriter().fine("RESUMING TX: " + txContext.toString());
     }
     ((TXManagerImpl)tm).resume(txContext);
   }

   public static TXStateInterface getTxState() {
     return ((TXManagerImpl)tm).getTXState();
   }

   /** Verify that the commit conflict string contains customer-friendly
    *  information. Throw an error if a problem is detected.
    *
    *  @param ex The commit conflict exception.
    */
   public static void checkCommitConflictException(ConflictException ex) {
      String errStr = ex.toString();
      if ((errStr.indexOf("REMOVED") >= 0) ||
          (errStr.indexOf("INVALID") >= 0) ||
          (errStr.indexOf("Deserializ") >= 0)) {
         throw new TestException("Test got " + errStr + ", but this error text " +
               "might contain GemFire internal values\n" + TestHelper.getStackTrace(ex));
      }
//      if (errStr.indexOf("from <local invalidated> to <invalidated>") >= 0) {
//         throw new TestException("Should not get a conflict on a local invalidate becoming an invalidate " +
//                   TestHelper.getStackTrace(ex));
//      }
   }

}
