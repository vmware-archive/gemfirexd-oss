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

package splitBrain;

import hydra.*;
import util.*;

import com.gemstone.gemfire.admin.*;
import com.gemstone.gemfire.distributed.*;

import java.io.*;

public class CrashEventMemberInfo implements Serializable {

private DistributedMember dm;
private boolean inSurvivingPartition;
private int survivingSideMemberCrashedEvents;
private int losingSideMemberCrashedEvents;

public CrashEventMemberInfo(DistributedMember dm, boolean inSurvivingPartition) {
   this.dm = dm;
   this.inSurvivingPartition = inSurvivingPartition;
   survivingSideMemberCrashedEvents = 0;
   losingSideMemberCrashedEvents = 0;
}

public void incrementCounter(boolean reportedBySurvivingPartition) {
   if (reportedBySurvivingPartition) {
      survivingSideMemberCrashedEvents++;
   } else {
      losingSideMemberCrashedEvents++;
   }
}

public DistributedMember getDm() {
   return this.dm;
}

public void validate() {
   StringBuffer aStr = new StringBuffer();

   if (inSurvivingPartition) {   
      // this distributed member is in the survivingPartition ...
      // SurvivingSide Admin should not see any memberCrashed Events for surviving members
      if (survivingSideMemberCrashedEvents != 0) {
         aStr.append("SurvivingSide Admin processed unexpected memberCrashed Event for " + dm + " (survivingSideMemberCrashedEvents = " + survivingSideMemberCrashedEvents + ")\n");
      }
   } else {                           
      // this distributed member is in the losingPartition
      // SurvivingSide Admin should see a single memberCrashed Event for each losingSide member
      if (survivingSideMemberCrashedEvents == 0) {
         aStr.append("SurvivingSide Admin missed memberCrashed Event for " + dm + " (survivingSideMemberCrashedEvents = " + survivingSideMemberCrashedEvents + ")\n");
      } else if (survivingSideMemberCrashedEvents > 1) {
         aStr.append("SurvivingSide Admin processed unexpected memberCrashed Events for " + dm + " (survivingSideMemberCrashedEvents = " + survivingSideMemberCrashedEvents + ")\n");
      }
   }

   if (aStr.length() > 0) {
      throw new TestException(aStr.toString());
   }
}

public String toString() {
   StringBuffer aStr = new StringBuffer();
   aStr.append("DistributedMember = " + this.dm);
   aStr.append(": inSurvivingPartition = " + this.inSurvivingPartition);
   aStr.append(": survivingSideMemberCrashedEvents = " + this.survivingSideMemberCrashedEvents);
   aStr.append(": losingSideMemberCrashedEvents = " + this.losingSideMemberCrashedEvents);
   return (aStr.toString());
}

}
