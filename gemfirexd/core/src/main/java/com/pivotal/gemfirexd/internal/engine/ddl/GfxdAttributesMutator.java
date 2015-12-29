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
package com.pivotal.gemfirexd.internal.engine.ddl;

import java.util.HashSet;
import java.util.Set;

import com.gemstone.gemfire.cache.AttributesMutator;
import com.gemstone.gemfire.cache.EvictionAttributesMutator;
import com.gemstone.gemfire.cache.ExpirationAttributes;

/**
 * This class holds the altered values for a region used by the region's
 * {@link AttributesMutator} in an ALTER TABLE statement.
 * 
 * @author Sumedh Wale
 * @author Yogesh Mahajan
 * @since 6.0
 */
public class GfxdAttributesMutator {

  /** the new maximum limit to be set in {@link EvictionAttributesMutator} */
  private int evictionMaximum;

  // Constants for expiration attributes
  public static final int EXPIRE_NONE = 0;

  public static final int EXPIRE_REGION_TIMETOLIVE = 1;

  public static final int EXPIRE_REGION_IDLETIME = 2;

  public static final int EXPIRE_ENTRY_TIMETOLIVE = 3;

  public static final int EXPIRE_ENTRY_IDLETIME = 4;

  /** the kind of expiration to be set; one of the above four constants */
  private int expirationKind;

  /** the new expiration attributes to be set in {@link AttributesMutator} */
  private ExpirationAttributes expirationAttrs;

  private boolean isAlterGatewaySender;

  private boolean isAlterAsyncEventListener;

  private final Set<String> gatewaySenderIds;
  private final Set<String> asyncEventQueueIds;

  private String hdfsStoreName;

  private boolean isCustomEvictionChange;
  private long evictionStart;
  private long evictionInterval;

  public GfxdAttributesMutator() {
    this.evictionMaximum = -1;
    this.expirationKind = EXPIRE_NONE;
    this.expirationAttrs = null;
    this.gatewaySenderIds = new HashSet<String>();
    this.asyncEventQueueIds = new HashSet<String>();
  }

  public Set<String> getGatewaySenderIds() {
    return this.gatewaySenderIds;
  }

  public void addGatewaySenderId(String id) {
    this.gatewaySenderIds.add(id);
  }

  public Set<String> getAsyncEventQueueIds() {
    return this.asyncEventQueueIds;
  }

  public void addAsyncEventQueueId(String id) {
    this.asyncEventQueueIds.add(id);
  }

  public int getExpirationKind() {
    return this.expirationKind;
  }

  public void setExpirationKind(int expirationKind) {
    this.expirationKind = expirationKind;
  }

  public ExpirationAttributes getExpirationAttributes() {
    return this.expirationAttrs;
  }

  public void setExpirationAttributes(ExpirationAttributes exprAttrs) {
    this.expirationAttrs = exprAttrs;
  }

  public int getEvictionMaximum() {
    return this.evictionMaximum;
  }

  public void setEvictionMaximum(int maximum) {
    this.evictionMaximum = maximum;
  }

  public String getHDFSStoreName() {
    return this.hdfsStoreName;
  }

  public void setHDFSStoreName(String hdfsStoreName) {
    this.hdfsStoreName = hdfsStoreName;
  }

  public long getCustomEvictionStart() {
    return this.evictionStart;
  }

  public long getCustomEvictionInterval() {
    return this.evictionInterval;
  }

  public boolean isAlterCustomEviction() {
    return this.isCustomEvictionChange;
  }

  public void setCustomEvictionAttributes(long newStart, long newInterval) {
    this.isCustomEvictionChange = true;
    this.evictionStart = newStart;
    this.evictionInterval = newInterval;
  }

  @Override
  public String toString() {
    return "evictionMaximum: " + this.evictionMaximum + ";expirationKind: "
        + this.expirationKind + ";expirationAttributes: "
        + this.expirationAttrs + ";senderIds: " + this.gatewaySenderIds
        + ";asyncEventIds: " + this.asyncEventQueueIds
        + (hdfsStoreName != null ? ";HDFSStoreName=" + this.hdfsStoreName : "")
        + (this.isCustomEvictionChange ? ";evictionStart="
            + new java.sql.Time(this.evictionStart) + ";evictionInterval="
            + this.evictionInterval : "");
  }

  public void setIsAlterGatewaySender(boolean isAlterGatewaySender) {
    this.isAlterGatewaySender = isAlterGatewaySender;
  }

  public boolean isAlterGatewaySender() {
    return isAlterGatewaySender;
  }
  
  public void setIsAlterAsyncEventListener(boolean isAlterAsyncEventListener) {
    this.isAlterAsyncEventListener = isAlterAsyncEventListener;
  }

  public boolean isAlterAsyncEventListener() {
    return isAlterAsyncEventListener;
  }
}
