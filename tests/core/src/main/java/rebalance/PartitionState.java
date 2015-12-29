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
package rebalance;

import java.util.*;
import hydra.*;

import com.gemstone.gemfire.cache.control.*;
import com.gemstone.gemfire.cache.partition.PartitionMemberInfo;
import com.gemstone.gemfire.cache.partition.PartitionRegionInfo;

/** 
 *  A class to represent state of the Partition.  This state includes:
 *  redundancySatisfied
 *  localMaxMemoryHonored
 *  and for each PartitionMember (MemberState)
 *     DistributedMember
 *     standard deviation of bucketCount
 *     standard deviation of primaryCount
 */
public class PartitionState {

GsRandom rand = TestConfig.tab().getRandGen();

private boolean redundancySatisfied = true; 
private boolean localMaxMemoryHonored = true;
private Set<MemberState> memberState;

// Constructors
public PartitionState(PartitionRegionInfo prd) {
   this.redundancySatisfied = RebalanceUtil.isRedundancySatisfied(prd);
   Set<PartitionMemberInfo> pmDetails = prd.getPartitionMemberInfo();
   this.localMaxMemoryHonored = RebalanceUtil.isLocalMaxMemoryHonored(pmDetails);
   this.memberState = createMemberState(pmDetails);
}

public PartitionState(Set<PartitionMemberInfo> pmDetails) {
   this.localMaxMemoryHonored = RebalanceUtil.isLocalMaxMemoryHonored(pmDetails);
   this.memberState = createMemberState(pmDetails);
}

public Set<MemberState> createMemberState(Set<PartitionMemberInfo> pmd) {
   Set<MemberState> memberState = new HashSet();
   double averageHeapUtilization = RebalanceUtil.getAverageHeapUtilization(pmd);
   double averageBucketCount = RebalanceUtil.getAverageBucketCount(pmd);
   double averagePrimaryCount = RebalanceUtil.getAveragePrimaryCount(pmd);
   for (PartitionMemberInfo memberDetails : pmd) {
      long localMaxMemory = memberDetails.getConfiguredMaxMemory();
      long size = memberDetails.getSize();
      double heapUtilization = ((double)size) / localMaxMemory;
      double heapUtilizationDev = Math.abs((heapUtilization * 100) - averageHeapUtilization);
      double bucketCountDev = Math.abs(memberDetails.getBucketCount() - averageBucketCount);
      double primaryCountDev = Math.abs(memberDetails.getPrimaryCount() - averagePrimaryCount);
      memberState.add(new MemberState(memberDetails.getDistributedMember(),
                                    heapUtilizationDev,
                                    bucketCountDev,
                                    primaryCountDev));
   }
   return memberState;
}

public PartitionState(boolean redundancySatisfied,
                      boolean localMaxMemoryHonored,
                      Set<MemberState> memberState) {
   this.redundancySatisfied = redundancySatisfied;
   this.localMaxMemoryHonored = localMaxMemoryHonored;
   this.memberState = memberState;
}

// accessors
public boolean isRedundancySatisfied() {
   return redundancySatisfied;
}

public boolean isLocalMaxMemoryHonored() {
   return localMaxMemoryHonored;
}

public Set<MemberState> getMemberState() {
   return memberState;
}

// display
public String toString() { 
   StringBuffer aStr = new StringBuffer();
   aStr.append("   redundancySatisfied: " + this.redundancySatisfied + "\n");
   aStr.append("   localMaxMemoryHonored: " + this.localMaxMemoryHonored + "\n");

   for (MemberState ms : memberState) {
      aStr.append(ms.toString());  
   }
   return aStr.toString();
}

}
