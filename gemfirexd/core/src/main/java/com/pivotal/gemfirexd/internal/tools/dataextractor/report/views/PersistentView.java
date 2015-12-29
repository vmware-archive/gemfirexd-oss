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
package com.pivotal.gemfirexd.internal.tools.dataextractor.report.views;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.gemstone.gemfire.internal.cache.persistence.PersistentMemberID;
import com.pivotal.gemfirexd.internal.tools.dataextractor.domain.ServerInfo;
import com.pivotal.gemfirexd.internal.tools.dataextractor.snapshot.GFXDSnapshotExportStat;

/******
 * Post data extraction this class is used to build the persistent view for tables and DDL's.
 * Using the view the tool determine which server would have newer data etc.  
 * @author bansods
 */
public class PersistentView {
  
  /****
   * Map of host name to {@link PersistentMemberID}
   */
  Map<String, PersistentMemberID> hostToPersistentMemberId;
  

  @Override
  public String toString() {
    return String
        .format(
            "PersistentView [hostToPersistentMemberId=%s, tableToRegionViewMap=%s]",
            hostToPersistentMemberId, tableToRegionViewMap);
  }


  /****
   * Table to RegionView Map
   */
  Map<String, RegionView> tableToRegionViewMap;
  
  
  public Map<String, RegionView> getTableToRegionViewMap() {
    return tableToRegionViewMap;
  }

  private PersistentView() {
    
  }
  
  public Map<String, PersistentMemberID> getHostToPersistentMemberId() {
    return hostToPersistentMemberId;
  }
  
  public static PersistentView getPersistentViewForDdlStats(Map<ServerInfo, List<GFXDSnapshotExportStat>> hostToDdlMap) {
    Map<String, List<GFXDSnapshotExportStat>> hostToDdlMap2 = new HashMap<String, List<GFXDSnapshotExportStat>>();
    
    //Convert the ServerInfo-stats to Host-stats and reuse the existing getPersistentView method
    for (Entry<ServerInfo, List<GFXDSnapshotExportStat>> entry : hostToDdlMap.entrySet()) {
      String host = entry.getKey().getServerName();
      List<GFXDSnapshotExportStat> listOfStats = entry.getValue();
      hostToDdlMap2.put(host, listOfStats);
    }
    
    return getPersistentView(hostToDdlMap2);
  }
  
  
  public static PersistentView getPersistentView(Map<String, List<GFXDSnapshotExportStat>> hostToStatsMap) {
    PersistentView pv = new PersistentView();
   
    Map<String, List<RegionViewInfoPerMember>> regionNameToRegionViewInfoPerMemberListMap = new HashMap<String, List<RegionViewInfoPerMember>>();
    pv.hostToPersistentMemberId = new HashMap<String, PersistentMemberID>();
    
    
    for (Map.Entry<String, List<GFXDSnapshotExportStat>> entry : hostToStatsMap.entrySet()) {
      String host = entry.getKey();
      List<GFXDSnapshotExportStat> listOfStats = entry.getValue();
      
      //Iterate over the stats get the RegionViewInfoPerMember
      // And create the view of for a region across all the members
      // i.e regionName to List<RegionViewInfoPerMember>
      for (GFXDSnapshotExportStat stat : listOfStats) {
        String regionName = stat.getSchemaTableName();
        RegionViewInfoPerMember regionViewInfo = stat.getRegionViewInfo();
        
        PersistentMemberID pmId =  pv.hostToPersistentMemberId.get(host);
        if (pmId == null) {
          pmId = regionViewInfo.getMyId();
          pv.hostToPersistentMemberId.put(host, pmId);
        }
        List<RegionViewInfoPerMember> regionViewList = regionNameToRegionViewInfoPerMemberListMap.get(regionName);
        
        if (regionViewList == null) {
          regionViewList = new ArrayList<RegionViewInfoPerMember>();
        }
        regionViewList.add(regionViewInfo);
        regionNameToRegionViewInfoPerMemberListMap.put(regionName, regionViewList);
      }
    }
    
    Set<String> regionNames = regionNameToRegionViewInfoPerMemberListMap.keySet();
    pv.tableToRegionViewMap = new HashMap<String, RegionView>();
    
    for (String regionName : regionNames) {
      List<RegionViewInfoPerMember> regionViewInfoList = regionNameToRegionViewInfoPerMemberListMap.get(regionName);
      
      RegionView rv = pv.getRegionView(regionViewInfoList);
      pv.tableToRegionViewMap.put(regionName, rv);
    }
    return pv;
  }
  
  
  public void setRankAndSort(List<RegionViewInfoPerMember> regionViewInfoList){
    for (RegionViewInfoPerMember regionViewInfo : regionViewInfoList) {
      PersistentMemberID myId = regionViewInfo.getMyId();
      int onlineScore = 0;
      for (RegionViewInfoPerMember otherRegionViewInfo : regionViewInfoList) {
        Set<PersistentMemberID> onlineMembers = otherRegionViewInfo.getOnlineMembers();
        
        if (onlineMembers.contains(myId)) {
          onlineScore++;
        }
      }
      regionViewInfo.setOnlineScore(onlineScore);
    }
    Collections.sort(regionViewInfoList);
  }
  
  /******
   * Builds the region view from List<RegionViewInfoPerMember>
   * @param regionViewInfoList
   * @return RegionView
   */
  public RegionView getRegionView(List<RegionViewInfoPerMember> regionViewInfoList) {
    List<RegionViewInfoPerGroup> regionViewInfoGroupList = new ArrayList<RegionViewInfoPerGroup>();
    
    int len = regionViewInfoList.size();
    Set<Integer> skipSet = new HashSet<Integer>();
    
    for (int i=0; i<len; i++) {
      if (!skipSet.contains(i)) {
        RegionViewInfoPerMember rvInfo1 = regionViewInfoList.get(0);
        List<RegionViewInfoPerMember> regionViewInfoGroup = new ArrayList<RegionViewInfoPerMember>();
        regionViewInfoGroup.add(rvInfo1);
        
        for (int j=i+1; j < len; j++) {
          if (!skipSet.contains(j)) {
            RegionViewInfoPerMember rvInfo2 = regionViewInfoList.get(j);
            
            if (rvInfo1.isSameGroup(rvInfo2)) {
              skipSet.add(j);
              regionViewInfoGroup.add(rvInfo2);
            }
          }
        }
        setRankAndSort(regionViewInfoList);
        regionViewInfoGroupList.add(new RegionViewInfoPerGroup(regionViewInfoList));
      }
    }
    
    RegionView rv = new RegionView(regionViewInfoGroupList);
    return rv;
  }
}
