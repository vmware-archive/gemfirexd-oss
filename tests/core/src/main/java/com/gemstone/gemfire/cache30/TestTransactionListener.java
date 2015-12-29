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
package com.gemstone.gemfire.cache30;

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.*;

/**
 * A <code>TransactionListener</code> used in testing.  Its callback methods
 * are implemented to throw {@link UnsupportedOperationException}
 * unless the user overrides the "2" methods.
 *
 * @see #wasInvoked
 *
 * @author Mitch Thomas
 * @since 4.0
 */
public abstract class TestTransactionListener extends TestCacheCallback
  implements TransactionListener {

  public final void afterCommit(TransactionEvent event) {
    this.invoked = true;
    try {
      afterCommit2(event);
    }
    catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    }
    catch (Throwable t) {
      this.callbackError = t;
    }
  }

  public void afterCommit2(TransactionEvent event) {
    String s = "Unexpected callback invocation";
    throw new UnsupportedOperationException(s);
  }

  public final void afterFailedCommit(TransactionEvent event) {
    this.invoked = true;
    try {
      afterFailedCommit2(event);
    }
    catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    }
    catch (Throwable t) {
      this.callbackError = t;
    }
  }

  public void afterFailedCommit2(TransactionEvent event) {
    String s = "Unexpected callback invocation";
    throw new UnsupportedOperationException(s);
  }


  public final void afterRollback(TransactionEvent event) {
    this.invoked = true;
    try {
      afterRollback2(event);
    }
    catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    }
    catch (Throwable t) {
      this.callbackError = t;
    }
  }

  public void afterRollback2(TransactionEvent event) {
    String s = "Unexpected callback invocation";
    throw new UnsupportedOperationException(s);
  }

}
