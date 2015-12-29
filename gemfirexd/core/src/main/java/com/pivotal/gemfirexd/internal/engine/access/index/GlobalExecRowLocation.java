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

package com.pivotal.gemfirexd.internal.engine.access.index;


import java.io.Externalizable;
import java.io.Serializable;

import com.gemstone.gemfire.internal.cache.RegionEntry;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.types.CloneableObject;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.RowLocation;
import com.pivotal.gemfirexd.internal.shared.common.StoredFormatIds;
import com.pivotal.gemfirexd.internal.shared.common.sanity.SanityManager;

public final class GlobalExecRowLocation extends AbstractRowLocation implements
    CloneableObject, Externalizable {

  private AbstractRowLocation rowLocation;

  private RegionEntry regionEntry;

  public GlobalExecRowLocation() {
  }

  private GlobalExecRowLocation(GlobalExecRowLocation gerl) {
    this.rowLocation = gerl.rowLocation;
    this.regionEntry = gerl.regionEntry;
  }

  @Override
  public Object getValue(GemFireContainer baseContainer)
      throws StandardException {
    // process if no location is set but with only the region entry
    if (this.rowLocation == null) {
      this.rowLocation = GlobalRowLocation.getRowLocation(
          (RowLocation)this.regionEntry, null, baseContainer, null, false);
    }
    return this.rowLocation.getValue(baseContainer);
  }

  @Override
  public Object getValueWithoutFaultIn(GemFireContainer baseContainer)
      throws StandardException {
    // process if no location is set but with only the region entry
    if (this.rowLocation == null) {
      this.rowLocation = GlobalRowLocation.getRowLocation(
          (RowLocation)this.regionEntry, null, baseContainer, null, false);
    }
    return this.rowLocation.getValueWithoutFaultIn(baseContainer);
  }
  
  
  

  @Override
  public ExecRow getRow(GemFireContainer baseContainer)
      throws StandardException {
    // process if no location is set but with only the region entry
    if (this.rowLocation == null) {
      this.rowLocation = GlobalRowLocation.getRowLocation(
          (RowLocation)this.regionEntry, null, baseContainer, null, false);
    }
    return this.rowLocation.getRow(baseContainer);
  }

  @Override
  public ExecRow getRowWithoutFaultIn(GemFireContainer baseContainer)
      throws StandardException {
    // process if no location is set but with only the region entry
    if (this.rowLocation == null) {
      this.rowLocation = GlobalRowLocation.getRowLocation(
          (RowLocation)this.regionEntry, null, baseContainer, null, false);
    }
    return this.rowLocation.getRowWithoutFaultIn(baseContainer);
  }

  @Override
  public RegionEntry getRegionEntry() {
    if (this.regionEntry != null) {
      return this.regionEntry;
    }
    assert this.rowLocation != null;
    return this.rowLocation.getRegionEntry();
  }

  @Override
  public boolean isDestroyedOrRemoved() {
    if (this.regionEntry != null) {
      return this.regionEntry.isDestroyedOrRemoved();
    }
    assert this.rowLocation != null;
    return this.rowLocation.isDestroyedOrRemoved();
  }

  @Override
  public boolean isUpdateInProgress() {
    if (this.regionEntry != null) {
      return this.regionEntry.isUpdateInProgress();
    }
    assert this.rowLocation != null;
    return this.rowLocation.isUpdateInProgress();
  }

  @Override
  public Object getKey() {
    if (this.regionEntry != null) {
      return this.regionEntry.getKey();
    }
    assert this.rowLocation != null;
    return this.rowLocation.getKey();
  }

  @Override
  public Serializable getRoutingObject() {
    assert this.rowLocation != null;
    return this.rowLocation.getRoutingObject();
  }

  public AbstractRowLocation getRowLocation(GemFireContainer baseContainer)
      throws StandardException {
    // process if no location is set but with only the region entry
    if (this.rowLocation == null) {
      this.rowLocation = GlobalRowLocation.getRowLocation(
          (RowLocation)this.regionEntry, null, baseContainer, null, false);
    }
    return this.rowLocation;
  }

  public void setRegionEntry(RegionEntry entry) {
    this.regionEntry = entry;
  }

  @Override
  public void setValue(DataValueDescriptor theValue) throws StandardException {
    assert theValue instanceof AbstractRowLocation:
      "theValue is not an AbstractRowLocation";

    if (theValue instanceof GlobalExecRowLocation) {
      throw new AssertionError(
          "GlobalExecRowLocation#setValue: unexpected GlobalExecRowLocation");
    }
    this.rowLocation = (AbstractRowLocation)theValue;
  }

  @Override
  public GlobalExecRowLocation getClone() {
    return new GlobalExecRowLocation(this);
  }

  public DataValueDescriptor getNewNull() {
    return new GlobalExecRowLocation();
  }

  @Override
  public DataValueDescriptor recycle() {
    this.regionEntry = null;
    this.rowLocation = null;
    return this;
  }

  @Override
  public String toString() {
    return "GlobalExecRowLocation: RegionEntry=" + this.regionEntry + " "
        + this.rowLocation;
  }

  @Override
  public GlobalExecRowLocation cloneObject() {
    GlobalExecRowLocation retVal = new GlobalExecRowLocation(this);
    if (this.rowLocation != null) {
      retVal.rowLocation = this.rowLocation.cloneObject();
    }
    else {
      retVal.rowLocation = null;
    }
    return retVal;
  }

  public final void setFrom(RegionEntry slot) {
    setRegionEntry(slot);
    if (GemFireXDUtils.TraceRSIter) {
      SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_RSITER,
          "ExecRowLocation::setFrom: RegionEntry {" + slot
              + "}. This bucket ID will not be stored as it is not needed");
    }
  }

  @Override
  public int getTypeFormatId() {
    return StoredFormatIds.ACCESS_MEM_HEAP_ROW_LOCATION_ID;
  }

 
 
}
