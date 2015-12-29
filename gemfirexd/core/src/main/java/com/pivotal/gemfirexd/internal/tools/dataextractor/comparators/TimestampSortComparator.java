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

public class TimestampSortComparator implements Comparator<GFXDSnapshotExportStat> {
  private Comparator<GFXDSnapshotExportStat> parentComparator;

  public TimestampSortComparator(Comparator<GFXDSnapshotExportStat> parentComparator) {
    this.parentComparator = parentComparator;
  }
  
  public int compare(GFXDSnapshotExportStat arg0, GFXDSnapshotExportStat arg1) {
    long arg0Time = arg0.getLastModifiedTime();
    long arg1Time = arg1.getLastModifiedTime();
    if (arg0Time < arg1Time) {
      return -1;
    }
    else if (arg0Time > arg1Time) {
      return 1;
    }
    if (parentComparator != null) {
      return parentComparator.compare(arg0, arg1);
    }
    return 0;
  }
}