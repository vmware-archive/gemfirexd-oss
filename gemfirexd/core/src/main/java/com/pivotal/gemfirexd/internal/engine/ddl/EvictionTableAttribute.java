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

import java.lang.reflect.Method;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.util.ObjectSizer;
import com.gemstone.gemfire.internal.ClassLoadUtil;
import com.gemstone.gemfire.internal.cache.lru.HeapLRUCapacityController;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.store.GemFireStore;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;

/**
 * DOCUMENT ME
 * 
 * 
 * @author Kumar Neeraj
 * @since 6.0
 */
public class EvictionTableAttribute {

  public static final byte ID_MEMSIZE = 1;

  public static final byte ID_HEAPPERCENT = 2;

  public static final byte ID_COUNT = 3;

  private EvictionAction evictAction;

  private byte lruType;

  private int count;

  public EvictionTableAttribute(byte lruType) {
    this.lruType = lruType;
    this.evictAction = null;
  }

  public EvictionTableAttribute() {
    this.evictAction = null;
  }

  public byte getLRUType() {
    return this.lruType;
  }

  public void setEvictionAction(EvictionAction eaction) {
    this.evictAction = eaction;
  }

  public EvictionAction getEvictionAction() {
    return this.evictAction;
  }

  public void setValue(int i) {
    this.count = i;
  }

  public void setType(byte evctType) {
    this.lruType = evctType;
  }

  public void setEvictionAttributes(AttributesFactory afact)
      throws StandardException {
    EvictionAttributes evictAttr;
    ObjectSizer osizer = Misc.getMemStore().getObjectSizer();
    if (this.evictAction == null) {
      this.evictAction = EvictionAction.DEFAULT_EVICTION_ACTION;
    }
    switch (this.lruType) {
      case EvictionTableAttribute.ID_MEMSIZE:
        evictAttr = EvictionAttributes.createLRUMemoryAttributes(this.count,
            osizer, this.evictAction);
        break;

      case EvictionTableAttribute.ID_HEAPPERCENT:
        evictAttr = EvictionAttributes.createLRUHeapAttributes(osizer,
            this.evictAction);
        break;

      case EvictionTableAttribute.ID_COUNT:
        evictAttr = EvictionAttributes.createLRUEntryAttributes(this.count,
            this.evictAction);
        break;

      default:
        throw StandardException.newException(SQLState.PROPERTY_SYNTAX_INVALID
            + ": unknown eviction type " + this.lruType);
    }
    afact.setEvictionAttributes(evictAttr);
  }
}
