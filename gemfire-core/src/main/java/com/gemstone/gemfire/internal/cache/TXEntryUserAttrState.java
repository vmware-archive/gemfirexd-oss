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
 * TXEntryUserAttrState is the entity that tracks transactional changes
 * to an entry user attribute.
 *
 * [sumedh] No longer supported in the new transaction model.
 * <p>
 * UNMAINTAINED -- DO NOT USE THIS CLASS.
 * 
 * @author Darrel Schneider
 * @since 4.0
 * 
 * @deprecated as of 7.0 support removed in the new TX model
 */
@Deprecated
public class TXEntryUserAttrState {
  private final Object originalValue;
  private final Object pendingValue;

  @Deprecated
  public TXEntryUserAttrState(Object originalValue) 
  {
    this.originalValue = originalValue;
    this.pendingValue = originalValue;
  }
  /*
  public Object getOriginalValue() {
    return this.originalValue;
  }
  public Object getPendingValue() {
    return this.pendingValue;
  }
  public Object setPendingValue(Object pv) {
    Object result = this.pendingValue;
    this.pendingValue = pv;
    return result;
  }
  void checkForConflict(LocalRegion r, Object key) throws CommitConflictException {
    Object curCmtValue = r.basicGetEntryUserAttribute(key);
    if (this.originalValue != curCmtValue) {
      throw new CommitConflictException(LocalizedStrings.TXEntryUserAttrState_ENTRY_USER_ATTRIBUTE_FOR_KEY_0_ON_REGION_1_HAD_ALREADY_BEEN_CHANGED_TO_2.toLocalizedString(new Object[] {key, r.getFullPath(), curCmtValue}));
    }
  }
  void applyChanges(LocalRegion r, Object key) {
    try {
      Region.Entry re = r.getEntry(key);
      re.setUserAttribute(this.pendingValue);
    } catch (CacheRuntimeException ignore) {
      // ignore any exceptions since we have already locked and
      // found no conflicts.
    }
  }
  */
}
