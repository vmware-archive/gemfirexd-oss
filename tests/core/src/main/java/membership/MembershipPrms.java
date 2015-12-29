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
package membership;

import hydra.BasePrms;

public class MembershipPrms extends BasePrms {


  /** (Vector of Strings) A list of the operations on a member
   *  Can be one or more of:
   *     join - start a new client vm (client killed and returns)
   *     kill - kill a client vm (client does not return)
   *     disconnect - disconnect client from distributed system
   *     reconnect - disconnect/reconnect client
   */
  public static Long memberOperations;  


  /** Milliseconds to wait between checks for membership updates
   *
   */
    public static Long waitInMembershipCheck;

  /** Milliseconds threshold for expected membership updates 
   *
   */
    public static Long membershipWaitThreshold;

  /** Seconds for test to run
   *
   */
    public static Long totalTaskTimeSec;


  /** (String) Work task clients perform (.e.g. concEntryEvent) while
   *  test is disrupting clients. 
   *
   */
   public static Long clientWorkTask;

  /** (Vector of boolean) Whether to reconnect the member after stop or disconnect
   *
   */
   public static Long reconnectMember;
  
  /** (Vector of Strings) stop mode for killing VM
   *
   */
   public static Long stopMode;

// ================================================================================
static {
   BasePrms.setValues(MembershipPrms.class);
}

}
