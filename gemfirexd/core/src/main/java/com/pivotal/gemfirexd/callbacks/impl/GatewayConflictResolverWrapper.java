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
package com.pivotal.gemfirexd.callbacks.impl;

import com.gemstone.gemfire.cache.util.GatewayConflictHelper;
import com.gemstone.gemfire.cache.util.GatewayConflictResolver;
import com.gemstone.gemfire.cache.util.TimestampedEntryEvent;
import com.gemstone.gemfire.internal.cache.TimestampedEntryEventImpl;
import com.pivotal.gemfirexd.internal.engine.jdbc.GemFireXDRuntimeException;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;

/**
 * A wrapper over GemFireXD's GatewayConflictResolver. An instance of this class
 * holds reference to user-supplied implementation of
 * com.pivotal.gemfirexd.callbacks.GatewayConflictResolver.
 * 
 * AbstractRegionEntry's processGatewayTag() passes on the TimestampedEntryEvent
 * to onEvent() of the wrapper, which internally invokes the user-supplied
 * conflict resolver. Since the wrapper implements GemFire's
 * GatewayConflictResolver interface, the same conflict resolver machinery of
 * GemFire is used.
 * 
 * A reference to this instance is required to be set in GemFireCacheImpl when
 * the user installs a custom resolver.
 * 
 * @author sjigyasu
 * 
 */
public class GatewayConflictResolverWrapper implements GatewayConflictResolver{

  // GemFireXD's WAN conflict resolver
  private com.pivotal.gemfirexd.callbacks.impl.GatewayConflictResolver gfxdResolver;
  
  // Helper
  private com.pivotal.gemfirexd.callbacks.impl.GatewayConflictHelperImpl gfxdHelper;
  
  /**
   * Wraps the custom resolver
   * @param resolver Custom resolver
   */
  public GatewayConflictResolverWrapper(com.pivotal.gemfirexd.callbacks.impl.GatewayConflictResolver resolver) {
    this.gfxdResolver = resolver;
    this.gfxdHelper = new com.pivotal.gemfirexd.callbacks.impl.GatewayConflictHelperImpl();
  }
  
  @Override
  public void onEvent(TimestampedEntryEvent event, GatewayConflictHelper helper) {
    // Wrap the event into a GatewayEvent
    GatewayEventImpl gatewayEvent = new GatewayEventImpl((TimestampedEntryEventImpl)event);
    
    // Wrap the GFE helper into GemFireXD's helper
    gfxdHelper.setGFEConflictHelper(helper);
    gfxdHelper.setGatewayEvent(gatewayEvent);
    
    gfxdResolver.onEvent(gatewayEvent, gfxdHelper);
    
    try {
      gfxdHelper.applyChanges(); 
    } catch(StandardException e){
      throw new GemFireXDRuntimeException(e);
    }
  }
  public static void dummy() {
  }

  public String toString() {
    return ("GatewayConflictResolver class:" + gfxdResolver == null ? "null" : gfxdResolver.getClass().getName());
  }
}
