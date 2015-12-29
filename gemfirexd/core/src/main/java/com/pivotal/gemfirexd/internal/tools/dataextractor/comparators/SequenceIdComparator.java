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
package com.pivotal.gemfirexd.internal.tools.dataextractor.comparators;

import java.util.Comparator;

import com.pivotal.gemfirexd.internal.tools.dataextractor.snapshot.GFXDSnapshot;
import com.pivotal.gemfirexd.internal.tools.dataextractor.snapshot.GFXDSnapshotExportStat;

class SequenceIdComparator implements Comparator<GFXDSnapshotExportStat> {
  public RegionViewSortComparator rvSortComparator; 
  
  public SequenceIdComparator(RegionViewSortComparator rvSortComparator) {
    this.rvSortComparator = rvSortComparator;
  }
  
  @Override
  public int compare(GFXDSnapshotExportStat arg0, GFXDSnapshotExportStat arg1) {
    long arg0SeqId = arg0.getMaxSeqId();
    long arg1SeqId = arg1.getMaxSeqId();
    
    if (arg0SeqId < arg1SeqId) {
      return 1;
    }
    else if (arg0SeqId > arg1SeqId) {
      return -1;
    } else {
      return rvSortComparator.compare((GFXDSnapshotExportStat)arg0, (GFXDSnapshotExportStat)arg1);
    } 
  }
}