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

import com.gemstone.gemfire.internal.cache.persistence.PersistentMemberID;
import com.pivotal.gemfirexd.internal.tools.dataextractor.report.views.RegionViewInfoPerMember;
import com.pivotal.gemfirexd.internal.tools.dataextractor.snapshot.GFXDSnapshot;
import com.pivotal.gemfirexd.internal.tools.dataextractor.snapshot.GFXDSnapshotExportStat;

public class RegionViewSortComparator implements Comparator<GFXDSnapshotExportStat> {

  private Comparator<GFXDSnapshotExportStat> parentComparator;

  public RegionViewSortComparator(Comparator<GFXDSnapshotExportStat> parentComparator) {
    this.parentComparator = parentComparator;
  }
  
  @Override
  public int compare(GFXDSnapshotExportStat arg0, GFXDSnapshotExportStat arg1) {
    //first see if there are any corrupt flags.  If both are corrupt we fall through and check online score
    if (arg0.isCorrupt()) {
      if (!arg1.isCorrupt()) {
        //To hit this branch means arg0 is corrupt and arg1 is not.  So arg0 is smaller/worse/less than
        return -1;
      }
    }
    else if (arg1.isCorrupt()) {
      //To hit this branch means arg0 is not corrupt and arg1 is.  So arg0 is larger/better/greater than
      return 1; 
    }
      
    RegionViewInfoPerMember m1View = arg0.getRegionViewInfo();
    RegionViewInfoPerMember m2View = arg1.getRegionViewInfo();
    PersistentMemberID m1Id = m1View.getMyId();
    PersistentMemberID m2Id = m2View.getMyId();
    int m1Score = 0;
    int m2Score = 0;
    
      if(m1View.getOfflineMembers().contains(m2Id) 
          || m1View.getOfflineAndEqualMembers().contains(m2Id)) {
        m2Score--;
      } else if (m1View.getOnlineMembers().contains(m2Id)) {
        m2Score++;
      }
    
      if(m2View.getOfflineMembers().contains(m1Id) 
          || m2View.getOfflineAndEqualMembers().contains(m1Id)) {
        m1Score--;
      } else if (m2View.getOnlineMembers().contains(m1Id)) {
        m1Score++;
      }
    
    //There should not be a scenario where member1 and member2 both don't see each other
    
    if (m1Score < m2Score) {
      return 1;
    } else if (m1Score > m2Score) {
      return -1;
    } else {
      if (parentComparator != null) {
        return parentComparator.compare(arg0, arg1);
      }
    }
    return 0;
  }
}