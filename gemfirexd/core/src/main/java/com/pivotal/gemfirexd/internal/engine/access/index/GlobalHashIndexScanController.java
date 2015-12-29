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

import java.io.Serializable;
import java.util.HashSet;

import com.gemstone.gemfire.GemFireException;
import com.gemstone.gemfire.internal.cache.CacheMap;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegionHelper;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserver;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverHolder;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.access.MemConglomerate;
import com.pivotal.gemfirexd.internal.engine.access.operations.GlobalHashIndexDeleteOperation;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.engine.store.RegionKey;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.store.access.RowUtil;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.RowLocation;

public final class GlobalHashIndexScanController extends
    Hash1IndexScanController {

  private RegionKey gfKey;

  @Override
  public int getType() {
    return MemConglomerate.GLOBALHASHINDEX;
  }

  @Override
  protected void initEnumerator() throws StandardException {
    this.observer = GemFireXDQueryObserverHolder.getInstance();
    // get the global index partitioned region
    if (this.baseRegion == null) {
      this.baseRegion = this.openConglom.getGemFireContainer().getRegion();
    }
    assert baseRegion != null && baseRegion instanceof PartitionedRegion;

    if (GemFireXDUtils.TraceIndex | GemFireXDUtils.TraceQuery) {
      GfxdIndexManager.traceIndex("GlobalHashIndexScanController: startKey=%s "
          + "stopKey=%s qualifier=%s for index container %s",
          this.init_startKeyValue, this.init_stopKeyValue, this.init_qualifier,
          this.openConglom.getGemFireContainer());
    }

    // full index scan for bulk FK checking by RIBulkChecker
    if ((this.openMode & GfxdConstants.SCAN_OPENMODE_FOR_REFERENCED_PK) != 0) {
      this.hasNext = true;
      if (this.init_startKeyValue == null && this.init_stopKeyValue == null) {
        this.init_scanColumnList = null;
      }
      else {
        // start and stop key must be equal
        if (this.init_startKeyValue != this.init_stopKeyValue && RowUtil
            .compare(this.init_startKeyValue, this.init_stopKeyValue) != 0) {
          failScan();
        }
        // for partitioned table we accumulate keys to do bulk contains checks
        // in next()
        // for replicated table just do containsKey check in next()
        if (this.baseRegion.getDataPolicy().withPartitioning()) {
          if (regionKeysSet == null) {
            regionKeysSet = new HashSet<RegionKey>();
          }
          // need to clone init_startKeyValue to add to the set as RIBulkChecker
          // sends keys using the same DVD[] object repeatedly and therefore
          // the earlier formed keys in getRegionKey() will be changed as it could
          // just use the passed DVD for key
          DataValueDescriptor[] startKeyValueCopy = 
              new DataValueDescriptor[this.init_startKeyValue.length];
          for (int i = 0; i < this.init_startKeyValue.length; i++) {
            startKeyValueCopy[i] = this.init_startKeyValue[i].getClone();
          }
          regionKeysSet.add(getRegionKey(startKeyValueCopy));
        }
      }
    }
    else if (this.init_startKeyValue != null
        && this.init_stopKeyValue != null
        && (this.init_startKeyValue.length == (this.openConglom
            .getConglomerate().keyColumns))) {

      if (this.init_startKeyValue != this.init_stopKeyValue && RowUtil
          .compare(this.init_startKeyValue, this.init_stopKeyValue) != 0) {
        failScan();
      }

      // generate the key of the region
      this.currentKey = this.init_startKeyValue;
      this.gfKey = this.openConglom
          .newGlobalKeyObject(this.init_startKeyValue);
      getRowLocation(this.gfKey);
    }
    else if (this.init_startKeyValue == null && this.init_stopKeyValue == null) {
      // this is the case of full scan on global index for ConsistencyChecker
      this.gfKeysIterator = this.baseRegion.keySet().iterator();
      this.init_scanColumnList = null;
      this.hasNext = true;
    }
    else {
      failScan();
    }
  }

  private void failScan() {
    GemFireXDUtils.throwAssert("The global hash index does not support this "
        + "search operation with startKey {"
        + RowUtil.toString(this.init_startKeyValue) + "} stopKey {"
        + RowUtil.toString(this.init_stopKeyValue) + '}');
  }

  @Override
  public boolean delete() throws StandardException {
    GlobalHashIndexDeleteOperation.doMe(this.openConglom.getTransaction(),
        this.txState, this.openConglom.getGemFireContainer(), this.gfKey, false);
    return true;
  }

  @Override
  protected void postProcessSearchCondition() throws StandardException {
    if ((this.openMode & GfxdConstants.SCAN_OPENMODE_FOR_REFERENCED_PK) == 0) {
      if (this.init_startKeyValue != null
          && this.containRowLocation(this.init_startKeyValue)) {
        this.init_startKeyValue = this
            .removeRowLocation(this.init_startKeyValue);
      }
      if (this.init_stopKeyValue != null
          && this.containRowLocation(this.init_stopKeyValue)) {
        this.init_stopKeyValue = this.removeRowLocation(this.init_stopKeyValue);
      }
    }
  }

  @Override
  protected boolean getRowLocation(Object key) throws StandardException {
    if (GemFireXDUtils.TraceIndex) {
      GfxdIndexManager.traceIndex("%s: getting row location for key=%s",
          toString(), key);
    }
    try {
      CacheMap cache = baseContainer.getGlobalIndexCache();
      Object routingObject = null;
      if (cache != null) {
        routingObject = cache.get(key);
      }
      if (routingObject == null) {
        final Object value = this.baseRegion.get(key);
        if (GemFireXDUtils.TraceIndex) {
          GfxdIndexManager.traceIndex("%s: got row location for key=%s: %s",
              toString(), key, value);
        }
        if (this.statNumRowsVisited != null) {
          this.statNumRowsVisited[0]++;
        }
        if (value == null) {
          this.currentRowLocation = null;
          // check for JVM going down (#44944)
          checkCancelInProgress();
          this.hasNext = false;
          this.statNumDeletedRowsVisited++;
        }
        else {
          assert value instanceof GlobalRowLocation;
          this.currentRowLocation = (GlobalRowLocation)value;
          this.hasNext = true;
          this.statNumRowsQualified++;
          if (cache != null) {
            cache.put(key, value);
            if (observer != null) {
              observer.afterPuttingInCached((Serializable)key, value);
            }
          }
        }
      }
      else {
        if (GemFireXDUtils.TraceIndex) {
          GfxdIndexManager.traceIndex(
              "%s: got routingObject from cache for key=%s: %s", toString(),
              key, routingObject);
        }
        GlobalRowLocation grl = new GlobalRowLocation(-1,
            ((Integer)routingObject));
        this.currentRowLocation = grl;
        this.hasNext = true;
        final GemFireXDQueryObserver observer = GemFireXDQueryObserverHolder.getInstance();
        if (observer != null) {
          observer.beforeReturningCachedVal((Serializable)key, routingObject);
        }
        // TODO what stats need to be incremented
      }
      return this.hasNext;
    } catch (GemFireException gfeex) {
      throw Misc.processGemFireException(gfeex, gfeex,
          "lookup of global index for key " + key, true);
    }
  }

  @Override
  public void fetch(DataValueDescriptor[] destRow) throws StandardException {
    assert this.hasNext == true;

    if (this.currentKey == null) {
      setCurrentKey();
    }
    if (this.init_scanColumnList == null) {
      // access the whole row
      int i;
      for (i = 0; i < this.currentKey.length; ++i) {
        if (destRow[i] == null) {
          destRow[i] = this.currentKey[i].getClone();
        }
        else {
          destRow[i].setValue(this.currentKey[i]);
        }
      }
      destRow[i].setValue(this.currentRowLocation);
    }
    else {
      for (int i = this.init_scanColumnList.anySetBit(); i > -1;
          i = this.init_scanColumnList.anySetBit(i)) {
        if (i == this.currentKey.length) {
          destRow[i].setValue(this.currentRowLocation);
        }
        else if (destRow[i] == null) {
          destRow[i] = this.currentKey[i].getClone();
        }
        else {
          destRow[i].setValue(this.currentKey[i]);
        }
      }
    }
    this.hasNext = false;
  }
  
  @Override
  public Object[] getRoutingObjectsForKeys(Object[] regionKeysArray) {
    Object[] routingObjects = new Object[regionKeysArray.length];
    for (int i = 0; i < regionKeysArray.length; i++) {
      routingObjects[i] = Integer.valueOf(PartitionedRegionHelper.getHashKey(
          (PartitionedRegion) this.baseRegion, regionKeysArray[i]));
    }
    return routingObjects;
  }

  @Override
  protected RegionKey getRegionKey(final DataValueDescriptor[] keyArray)
      throws StandardException {
    final int size_1 = keyArray.length - 1;
    if (size_1 == 0) {
      return keyArray[0];
    }
    if (keyArray[size_1] instanceof RowLocation) {
      if (size_1 == 1) {
        return keyArray[0];
      }
      final DataValueDescriptor[] newKeyValue = new DataValueDescriptor[size_1];
      for (int index = 0; index < size_1; ++index) {
        newKeyValue[index] = keyArray[index];
      }
      return this.openConglom.newGlobalKeyObject(newKeyValue);
    }
    return this.openConglom.newGlobalKeyObject(keyArray);
  }

  @Override
  protected void closeScan() {
    super.closeScan();
    this.gfKey = null;
  }

  @Override
  public String toString() {
    return "GlobalHashIndexScanController on "
        + this.openConglom.getGemFireContainer().toString()
        + ", forReferencedPK=" + ((this.openMode & GfxdConstants
            .SCAN_OPENMODE_FOR_REFERENCED_PK) != 0);
  }
}
