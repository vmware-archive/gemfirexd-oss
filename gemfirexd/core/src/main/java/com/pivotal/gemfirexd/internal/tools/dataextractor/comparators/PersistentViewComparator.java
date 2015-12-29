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
import java.util.List;

import com.pivotal.gemfirexd.internal.tools.dataextractor.snapshot.GFXDSnapshot;
import com.pivotal.gemfirexd.internal.tools.dataextractor.snapshot.GFXDSnapshotExportStat;

public class PersistentViewComparator implements Comparator<List<GFXDSnapshotExportStat>> {

  private Comparator<List<GFXDSnapshotExportStat>> parentComparator;

  public PersistentViewComparator(Comparator<List<GFXDSnapshotExportStat>> parentComparator) {
    this.parentComparator = parentComparator;
  }

  @Override
  public int compare(List<GFXDSnapshotExportStat> arg0, List<GFXDSnapshotExportStat> arg1) {
    long arg0Size = arg0.size();
    long arg1Size = arg1.size();
    if (arg0Size < arg1Size) {
      return -1;
    } else if (arg0Size > arg1Size) {
      return 1;
    }
    if (parentComparator != null) {
      return parentComparator.compare(arg0, arg1);
    }
    return 0;
  }

}