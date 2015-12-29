
/*

 Derived from source files from the Derby project.

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
 * Changes for GemFireXD distributed data platform.
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

package com.pivotal.gemfirexd.internal.engine.access;

import com.pivotal.gemfirexd.internal.iapi.error.ExceptionSeverity;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.context.ContextImpl;
import com.pivotal.gemfirexd.internal.iapi.services.context.ContextManager;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;


/**
 * DOCUMENT ME!
 *
 * @author Eric Zoerner
 */

final public class GemFireTransactionContext extends ContextImpl {

  /** The transaction this context is managing. */
  private final GemFireTransaction transaction;

  /** true if any exception causes this transaction to be destroyed */
  private boolean abortAll;

  /*
   ** Context methods (most are implemented by super-class).
   */

  /**
   * this constructor is called with the transaction controller to be saved when
   * the context is created (when the first statement comes in, likely).
   *
   * @param  cm  DOCUMENT ME!
   * @param  context_id  DOCUMENT ME!
   * @param  theTransaction  DOCUMENT ME!
   * @param  abortAll  DOCUMENT ME!
   *
   * @throws  StandardException  DOCUMENT ME!
   */
  GemFireTransactionContext(ContextManager cm,
                            String context_id,
                            GemFireTransaction theTransaction,
                            boolean abortAll) throws StandardException {
    super(cm, context_id);
    assert theTransaction != null;
    this.abortAll = abortAll;
    this.transaction = theTransaction;
  }

  /**
   * Handle cleanup processing for this context. The resources associated with a
   * transaction are the open controllers. Cleanup involves closing them at the
   * appropriate time. Rollback of the underlying transaction is handled by the
   * raw store.
   */
  public void cleanupOnError(Throwable error) throws StandardException {

    if (SanityManager.DEBUG) {
      SanityManager.ASSERT(getContextManager() != null);
    }

    boolean destroy = false;

    if (error instanceof StandardException) {
      StandardException se = (StandardException) error;

      // If the severity is lower than a transaction error then do nothing.
      if (se.getSeverity() < ExceptionSeverity.TRANSACTION_SEVERITY) {
        return;
      }

      // If the session is going to disappear then we want to destroy this
      // transaction, not just abort it.
      if (se.getSeverity() >= ExceptionSeverity.SESSION_SEVERITY) {
        destroy = true;
      }
    }
    else {

      // abortAll is true or some java* error, throw away the
      // transaction.
      destroy = true;
    }

    // abort should already have been done by LCC cleanup, and below will
    // just be a no-op for that case since tran will be idle
    // for the case of DDLs don't abort here since it will be taken care of
    // at higher layer and below will also abort the parent transaction
    final LanguageConnectionContext lcc = this.transaction
        .getLanguageConnectionContext();
    if (lcc == null || lcc.getParentOfNestedTransactionExecute() == null) {
      this.transaction.abort();
    }
    if (destroy) {
      this.transaction.clean();
      popMe();
    }
  }

  /*
   ** Methods of GemFireTransactionContext
   */

  /**
   * package
   *
   * @return  DOCUMENT ME!
   */
  public GemFireTransaction getTransaction() {
    return transaction;
  }

  void reset(String contextId, boolean abortAll) {
    this.myIdName = contextId;
    this.abortAll = abortAll;
  }
}
