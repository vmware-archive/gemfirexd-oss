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

import java.util.HashSet;
import java.util.Set;

import com.gemstone.gemfire.internal.cache.persistence.DiskRegionView;
import com.gemstone.gemfire.internal.cache.persistence.PersistentMemberID;
import com.gemstone.gemfire.internal.cache.persistence.PersistentMemberPattern;

/******
 * This class holds information about the persistent view of the region from a member's perspective. 
 * @author bansods
 *
 */
public class RegionViewInfoPerMember implements Comparable<RegionViewInfoPerMember> {
  /***
   * Name of the region
   */
  String name;
  
  /****
   * Id of the server hosting this region
   */
  PersistentMemberID myId;
  
  /****
   * Number of members who see this member online
   */
  int onlineScore; 

  private Set<PersistentMemberID> onlineMembers = new HashSet<PersistentMemberID>();
  private Set<PersistentMemberID> offlineMembers = new HashSet<PersistentMemberID>();
  private Set<PersistentMemberPattern> revokedMembers = new HashSet<PersistentMemberPattern>();
  private Set<PersistentMemberID> offlineAndEqualMembers = new HashSet<PersistentMemberID>();
  private Set<PersistentMemberID> allMembers = new HashSet<PersistentMemberID>();
  
  public Set<PersistentMemberID> getAllMembers() {
    return allMembers;
  }

  public RegionViewInfoPerMember(DiskRegionView drv) {
    this.name = drv.getName();
    this.myId = drv.getMyPersistentID();
    this.onlineMembers.addAll(drv.getOnlineMembers());
    this.offlineMembers.addAll(drv.getOfflineMembers());
    this.revokedMembers.addAll(drv.getRevokedMembers());
    this.offlineAndEqualMembers.addAll(drv.getOfflineAndEqualMembers());
    
    
    this.allMembers.add(myId);
    this.allMembers.addAll(onlineMembers);
    this.allMembers.addAll(offlineMembers);
  }
  
  
  private Set<PersistentMemberID> membersWhoSeeMeOnline = new HashSet<PersistentMemberID>();
  private Set<PersistentMemberID> membersWhoSeeMeOffline = new HashSet<PersistentMemberID>();
  
  
  public void addMemberWhoSeesMeOnline(PersistentMemberID member) {
    this.membersWhoSeeMeOnline.add(member);
  }
  
  public void addMemberWhoSeesMeOffline(PersistentMemberID member) {
    this.membersWhoSeeMeOffline.add(member);
  }
  
  public boolean isSameGroup(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    RegionViewInfoPerMember other = (RegionViewInfoPerMember) obj;
    
    if (allMembers == null) {
      if (other.allMembers != null)
        return false;
    } else {
      Set<PersistentMemberID> otherMembers = new HashSet<PersistentMemberID>(other.allMembers);
      Set<PersistentMemberID> myMembers = new HashSet<PersistentMemberID>(other.allMembers);
      
      //Get the intersection of the 2's member
      // If the intersection is empty then they don't belong to same distributed system.
      myMembers.retainAll(otherMembers);
      if (myMembers.isEmpty()) {
        return false;
      } else {
        return true;
      }
    } 
    
    if (name == null) {
      if (other.name != null)
        return false;
    } else if (!name.equals(other.name))
      return false;
    return true;
  }
  
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result
        + ((allMembers == null) ? 0 : allMembers.hashCode());
    result = prime * result + ((name == null) ? 0 : name.hashCode());
    return result;
  }


  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    RegionViewInfoPerMember other = (RegionViewInfoPerMember) obj;
    if (allMembers == null) {
      if (other.allMembers != null)
        return false;
    } else if (!allMembers.equals(other.allMembers))
      return false;
    if (name == null) {
      if (other.name != null)
        return false;
    } else if (!name.equals(other.name))
      return false;
    return true;
  }


  public String getName() {
    return name;
  }
  
  public Set<PersistentMemberID> getOnlineMembers() {
    return onlineMembers;
  }
  
  
  public PersistentMemberID getMyId() {
    return this.myId;
  }


  @Override
  public String toString() {
    return String
        .format(
            "RegionViewInfo [name=%s, myId=%s, onlineMembers=%s, offlineMembers=%s, revokedMembers=%s, offlineAndEqualMembers=%s]",
            name, myId, onlineMembers, offlineMembers, revokedMembers,
            offlineAndEqualMembers);
  }


  public Set<PersistentMemberID> getOfflineMembers() {
    return offlineMembers;
  }

  public Set<PersistentMemberPattern> getRevokedMembers() {
    return revokedMembers;
  }

  public Set<PersistentMemberID> getOfflineAndEqualMembers() {
    return offlineAndEqualMembers;
  }

  public void addOnlineMembers(Set<PersistentMemberID> onlineMembers) {
    this.onlineMembers.addAll(onlineMembers);
  }

  public void addOfflineMembers(Set<PersistentMemberID> offlineMembers) {
    this.offlineMembers.addAll(offlineMembers);
  }
  
  public void addOfflineAndEqualMembers(Set<PersistentMemberID> offlineAndEqualMembers) {
    this.offlineAndEqualMembers.addAll(offlineAndEqualMembers);
  }

  public void addRevokedMembers(Set<PersistentMemberPattern> revokedMembers) {
    this.revokedMembers.addAll(revokedMembers);
  }
  
  public boolean checkRevoked(PersistentMemberID pmId) {
    for (PersistentMemberPattern pmm : revokedMembers) {
      if (pmm.matches(pmId)) {
        return true;
      }
    }
    return false;
  }
  
  
  public void setOnlineScore(int onlineScore) {
    this.onlineScore = onlineScore;
  }
  
  public int getOnlineScore() {
    return this.onlineScore;
  }

  @Override
  public int compareTo(RegionViewInfoPerMember o) {
    if (this.onlineScore < o.onlineScore)
      return -1;
    else if (this.onlineScore > o.onlineScore)
      return 1;
    else
      return 0;
  }
}
