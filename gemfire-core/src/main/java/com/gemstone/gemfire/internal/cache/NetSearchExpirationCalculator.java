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

import com.gemstone.gemfire.cache.*;

/**
 * EntryExpiryTask already implements the algorithm for figuring out
 * expiration.  This class has it use the remote expiration attributes 
 * provided by the netsearch, rather than this region's attributes.
 */
public class NetSearchExpirationCalculator extends EntryExpiryTask {
  
  private final ExpirationAttributes idleAttr;
  private final ExpirationAttributes ttlAttr;
  
  /** Creates a new instance of NetSearchExpirationCalculator */
  public NetSearchExpirationCalculator(LocalRegion region, Object key, int ttl, int idleTime) {
    super(region, null);
    idleAttr = new ExpirationAttributes(idleTime, ExpirationAction.INVALIDATE);
    ttlAttr = new ExpirationAttributes(ttl, ExpirationAction.INVALIDATE); 
  }

  @Override
  protected ExpirationAttributes getTTLAttributes() {
    return this.ttlAttr;
  }

  @Override
  protected ExpirationAttributes getIdleAttributes() {
    return this.idleAttr;
  }
  
}
