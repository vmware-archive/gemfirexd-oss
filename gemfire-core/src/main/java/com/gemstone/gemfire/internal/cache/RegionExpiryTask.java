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

package com.gemstone.gemfire.internal.cache;

/**
 * RegionExpiryTask represents a timeout event for region expiration
 */

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.ExpirationAttributes;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.TimeoutException;

abstract class RegionExpiryTask extends ExpiryTask
  {
  private boolean isCanceled;

  protected RegionExpiryTask(LocalRegion reg) {
    super(reg);
    this.isCanceled = false;
  }

  @Override
  public Object getKey() {
    return null;
  }
  
  @Override
  protected ExpirationAttributes getIdleAttributes() {
    return getLocalRegion().getRegionIdleTimeout();
  }
  @Override
  protected ExpirationAttributes getTTLAttributes() {
    return getLocalRegion().getRegionTimeToLive();
  }

  @Override
  protected final long getLastAccessedTime()
  {
    return getLocalRegion().getLastAccessedTime();
  }

  @Override
  protected final long getLastModifiedTime()
  {
    return getLocalRegion().getLastModifiedTime();
  }

  @Override
  protected final boolean destroy(boolean isPending) throws CacheException
  {
      LocalRegion lr = getLocalRegion();
      RegionEventImpl event = new RegionEventImpl(lr,
          Operation.REGION_EXPIRE_DESTROY, null, false, lr.getMyId(),
          lr.generateEventID() /* generate EventID */);
      lr.basicDestroyRegion(event, true);
      return true;
  }

  @Override
  protected final boolean invalidate() throws TimeoutException
  {
    LocalRegion lr = getLocalRegion();
    if (lr.regionInvalid) {
      return false;
    }
    RegionEventImpl event = new RegionEventImpl(lr,
        Operation.REGION_EXPIRE_INVALIDATE, null, false, lr.getMyId());
    lr.basicInvalidateRegion(event);
    return true;
  }

  @Override
  protected final boolean localDestroy() throws CacheException
  {
      LocalRegion lr = getLocalRegion();
      RegionEventImpl event = new RegionEventImpl(lr,
          Operation.REGION_EXPIRE_LOCAL_DESTROY, null, false, lr.getMyId(),
          lr.generateEventID() /* generate EventID */);
      lr.basicDestroyRegion(event, false);
      return true;
  }

  @Override
  protected final boolean localInvalidate()
  {
    LocalRegion lr = getLocalRegion();
    if (lr.regionInvalid)
      return false;
    RegionEventImpl event = new RegionEventImpl(lr,
        Operation.REGION_EXPIRE_LOCAL_INVALIDATE, null, false, lr.getMyId());
    lr.basicInvalidateRegion(event);
    return true;
  }

  @Override
  public boolean cancel()
  {
    isCanceled = true;
    return super.cancel();
  }

  @Override
  protected final void performTimeout() throws CacheException
  {
    if (isCanceled) {
      return;
    }
    super.performTimeout();
  }

  @Override
  protected final void basicPerformTimeout(boolean isPending) throws CacheException
  {
    if (isCanceled) {
      return;
    }
    super.basicPerformTimeout(isPending);
  }

  @Override
  final protected void reschedule() throws CacheException
  {
    if (isCacheClosing() || getLocalRegion().isClosed() || getLocalRegion().isDestroyed()
        || !isExpirationAllowed()) {
      return;
    }

    addExpiryTask();
  }

  @Override
  public String toString()
  {
    String expireTime = "<unavailable>";
    try {
      expireTime = String.valueOf(getExpirationTime());
    }
    catch (Throwable e) {
      Error err;
      if (e instanceof Error && SystemFailure.isJVMFailureError(
          err = (Error)e)) {
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
    }
    return super.toString() + " for " + getLocalRegion().getFullPath()
        + ", expiration time: " + expireTime + " [now: "
 + getNow() + "]";
  }
}
