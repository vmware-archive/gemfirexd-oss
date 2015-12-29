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

import com.gemstone.gemfire.cache.ExpirationAttributes;

/**
 * Remembers the expiration attributes returned from
 * the customer's CustomExpiry callback, if any.
 * 
 * @author dschneider
 * @since 7.5
 *
 */
public class CustomEntryExpiryTask extends EntryExpiryTask {  
  private final ExpirationAttributes ttlAttr;
  private final ExpirationAttributes idleAttr;

  public CustomEntryExpiryTask(LocalRegion region, RegionEntry re, ExpirationAttributes ttlAtts, ExpirationAttributes idleAtts) {
    super(region, re);
    this.ttlAttr = ttlAtts;
    this.idleAttr = idleAtts;
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
