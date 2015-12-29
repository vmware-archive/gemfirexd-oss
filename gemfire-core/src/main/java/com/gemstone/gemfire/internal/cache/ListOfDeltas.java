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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.InternalGemFireError;

import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.HeapDataOutputStream;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.cache.delta.Delta;
import com.gemstone.gemfire.internal.cache.versions.VersionSource;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;
import com.gemstone.gemfire.internal.offheap.annotations.Unretained;
import com.gemstone.gemfire.pdx.internal.ConvertableToBytes;

/**
 * 
 * @author Neeraj
 */
public final class ListOfDeltas implements Delta {

  private final ArrayList<Delta> listOfDeltas;

  private transient Delta lastDelta;
  
  public ListOfDeltas(final int size) {
    this.listOfDeltas = new ArrayList<Delta>(size);
  }

  public ListOfDeltas(Delta deltaObj) {
    this.listOfDeltas = new ArrayList<Delta>();
    this.listOfDeltas.add(deltaObj);
    this.lastDelta = deltaObj;
  }

  public Object apply(final EntryEvent<?, ?> ev) {
    if (ev != null && ev instanceof EntryEventImpl) {
      final EntryEventImpl putEvent = (EntryEventImpl)ev;
      final Object newValue = apply(putEvent.getRegion(), putEvent.getKey(),
          putEvent.getOldValueAsOffHeapDeserializedOrRaw(), false);
      putEvent.setNewValue(newValue);
      return newValue;
    }
    else {
      throw new InternalGemFireError(
          "ListOfDeltas.apply: putEvent is either null "
              + "or is not of type EntryEventImpl: " + ev);
    }
  }

  public Object apply(final Region<?, ?> region, final Object key, @Unretained Object v, boolean prepareForOffHeap) {
    for (Delta delta : this.listOfDeltas) {
      v = delta.apply(region, key, v, prepareForOffHeap);
    }
    return v;
  }

  public Delta merge(Region<?, ?> region, Delta toMerge) {
    this.listOfDeltas.add(toMerge);
    this.lastDelta = toMerge;
    return this;
  }

  public Delta cloneDelta() {
    final int numDeltas = this.listOfDeltas.size();
    final ListOfDeltas clone = new ListOfDeltas(numDeltas);
    for (int index = 0; index < numDeltas; ++index) {
      clone.listOfDeltas.add(this.listOfDeltas.get(index).cloneDelta());
      clone.lastDelta = this.lastDelta;
    }
    return clone;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean allowCreate() {
    return false;
  }

 
  public List<Delta> getDeltas()  {
    return Collections.unmodifiableList(this.listOfDeltas);
  }

  public VersionTag getLastDeltaVersionTag() {
    if (this.lastDelta != null) {
      return this.lastDelta.getVersionTag();
    }
    return null;
  }
  
  public void sortAccordingToVersionNum(boolean concurrencyEnabled, final Object key) {
    if (this.listOfDeltas != null && concurrencyEnabled) {
      checkVersionTagsValidity(key);
      Collections.sort(this.listOfDeltas, new Comparator<Delta>() {
        @Override
        public int compare(Delta o1, Delta o2) {
          VersionTag vtag1 = o1.getVersionTag();
          VersionTag vtag2 = o2.getVersionTag();
          int ver1 = vtag1.getEntryVersion();
          int ver2 = vtag2.getEntryVersion();
          long difference = ver2 - ver1;
          if (0x10000 < difference || difference < -0x10000) {
            if (difference < 0) {
              ver2 += 0x1000000L;
            } else {
              ver1 += 0x1000000L;
            }
          }
          long cmp = (ver1 - ver2);
          if (cmp != 0) {
            return Long.signum(cmp);
          }
          VersionSource vsrc1 = vtag1.getMemberID();
          VersionSource vsrc2 = vtag2.getMemberID();
          if (vsrc1 != null && vsrc2 != null) {
            return vsrc1.compareTo(vsrc2);
          }
          return 0;
        }
      });
      int sz = this.listOfDeltas.size();
      this.lastDelta = this.listOfDeltas.get(sz-1);
    }
  }

  private void checkVersionTagsValidity(Object key) {
    for(Delta d : this.listOfDeltas) {
      if (d.getVersionTag() == null) {
        throw new IllegalStateException("version tag should be non null for delta=" + d + ", key=" + key);
      }
    }
  }
  /**
   * {@inheritDoc}
   */
  @Override
  public void setVersionTag(VersionTag versionTag) {
    // TODO Auto-generated method stub
    
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public VersionTag getVersionTag() {
    // TODO Auto-generated method stub
    return null;
  }
}
