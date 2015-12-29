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
package management.test.cli;

import java.util.HashSet;
import java.util.Set;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.distributed.DistributedMember;

/** Class to contain information about a particular member, regions it defines, etc to be written
 *  to the blackboard and used for validation purposes.
 *  
 * @author lynn
 *
 */
public class MemberDescription implements java.io.Serializable {
  
  private int vmID = 0;
  private DistributedMember member = null;
  private String clientName = null;
  private Set<String> regions = new HashSet(); // contains region full path for each region defined in this member
  

  /**
   * @return the vmID
   */
  public int getVmID() {
    return vmID;
  }
  
  /**
   * @param vmID the vmID to set
   */
  public void setVmID(int vmID) {
    this.vmID = vmID;
  }
  
  /**
   * @return the member
   */
  public DistributedMember getMember() {
    return member;
  }
  
  /**
   * @param member the member to set
   */
  public void setMember(DistributedMember member) {
    this.member = member;
  }
  
  /**
   * @return the clientName
   */
  public String getClientName() {
    return clientName;
  }
  
  /**
   * @param clientName the clientName to set
   */
  public void setClientName(String clientName) {
    this.clientName = clientName;
  }
  
  /** Add a region for this member
   * 
   * @param aRegion The region defined in this member.
   */
  public synchronized void addRegion(Region aRegion) {
    regions.add(aRegion.getFullPath());
  }
  
  /**
   * @return A Set containing the region full path names for each region defined in the member.
   */
  public Set<String> getRegions() {
    return regions;
  }
  
}
