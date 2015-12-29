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

import hydra.*;
import com.gemstone.gemfire.distributed.*;
import com.gemstone.gemfire.cache.control.*;

/**
 *  A class to hold the standard deviation (from avg) for 
 *  each Member's bucketCount and primaryCount.  Members are
 *  identified by DistributedMember.
 */
public class MemberState {

private double heapUtilizationDev;  // deviation from average heapUtilization
private double bucketCountDev;   // deviation from averageBucketCount
private double primaryCountDev;  // deviation from averagePrimaryCount
private DistributedMember distributedMember;

// Constructor
public MemberState(DistributedMember dm,
                   double heapUtilizationDev,
                   double bucketCountDev,
                   double primaryCountDev) {

   this.distributedMember = dm;
   this.heapUtilizationDev = heapUtilizationDev;
   this.bucketCountDev = bucketCountDev;
   this.primaryCountDev = primaryCountDev;
}

// accessors
public DistributedMember getDistributedMember() {
   return this.distributedMember;
}

public double getHeapUtilizationDev() {
   return this.heapUtilizationDev;
}

public double getBucketCountDev() {
   return this.bucketCountDev;
}

public double getPrimaryCountDev() {
   return this.primaryCountDev;
}

// display
public String toString() {
   StringBuffer aStr = new StringBuffer();
   aStr.append("   Member " + this.distributedMember + "\n");
   aStr.append("     heapUtilizationDev: " + heapUtilizationDev + "\n");
   aStr.append("     bucketCountDev: " + bucketCountDev + "\n");
   aStr.append("     primaryCountDev: " + primaryCountDev + "\n");
   return (aStr.toString());
}

}
