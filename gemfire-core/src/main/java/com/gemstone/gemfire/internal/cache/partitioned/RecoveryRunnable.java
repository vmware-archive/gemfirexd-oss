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
package com.gemstone.gemfire.internal.cache.partitioned;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.cache.PRHARedundancyProvider;

public abstract class RecoveryRunnable implements Runnable {
  
  protected final PRHARedundancyProvider redundancyProvider;

  /**
   * @param prhaRedundancyProvider
   */
  public RecoveryRunnable(PRHARedundancyProvider prhaRedundancyProvider) {
    redundancyProvider = prhaRedundancyProvider;
  }

  private volatile Throwable failure;
  
  public abstract void run2();

  public void checkFailure() {
    if(failure != null) {
      if( failure instanceof RuntimeException) {
        throw (RuntimeException) failure;
      } else {
        throw new InternalGemFireError("Failure during bucket recovery ", failure);
      }
    }
  }

  public void run()
  {
    CancelCriterion stopper = redundancyProvider.prRegion
        .getGemFireCache().getDistributedSystem().getCancelCriterion();
    DistributedSystem.setThreadsSocketPolicy(true /* conserve sockets */);
    SystemFailure.checkFailure();
    if (stopper.cancelInProgress() != null) {
      return;
    }
    try {
      run2();
    }
    catch (Throwable t) {
      Error err;
      if (t instanceof Error && SystemFailure.isJVMFailureError(
          err = (Error)t)) {
        SystemFailure.initiateFailure(err);
        // If this ever returns, rethrow the error. We're poisoned
        // now, so don't let this thread continue.
        throw err;
      }
      // Whenever you catch Error or Throwable, you must also
      // check for fatal JVM error (see above).  However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      redundancyProvider.getLogger().fine(
          "Unexpected exception in PR redundancy recovery", t);
      failure = t;
    }
  }
}