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

import com.gemstone.gemfire.cache.CacheEvent;
import com.gemstone.gemfire.internal.cache.FilterRoutingInfo.FilterInfo;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;

/**
 * A CacheEvent, but the isGenerateCallbacks() is hidden from public consumption
 * @author jpenney
 *
 */
public interface InternalCacheEvent extends CacheEvent<Object, Object> {

  /**
   * Answers true if this event should generate user callbacks.
   * @return true if this event will generate user callbacks
   */
  public boolean isGenerateCallbacks();
  
  /**
   * Answers true if this event is from a client
   * @deprecated as of 5.7 use {@link #hasClientOrigin} instead.
   */
  @Deprecated
  public boolean isBridgeEvent();
  /**
   * Answers true if this event is from a client
   * @since 5.7
   */
  public boolean hasClientOrigin();
  
  /**
   * returns the ID associated with this event
   */
  public EventID getEventId();
  
  /**
   * Returns the Operation type.
   * @return eventType
   */
  public EnumListenerEvent getEventType();

  /**
   * sets the event type
   * @param operation the operation performed by this event
   */
  public void setEventType(EnumListenerEvent operation);
  
  /**
   * returns the bridge context for the event, if any
   */
  public ClientProxyMembershipID getContext();
  
  /**
   * set the client routing information for this event
   * @param info TODO
   */
  public void setLocalFilterInfo(FilterInfo info);
  
  /**
   * get the local routing information for this event
   */
  public FilterInfo getLocalFilterInfo();
  
  /**
   * get the version tag for the event
   */
  public VersionTag getVersionTag();

}
