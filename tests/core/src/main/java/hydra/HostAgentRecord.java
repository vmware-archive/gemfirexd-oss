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

package hydra;

import java.io.*;
//import java.util.*;

/**
 *
 * A HostAgentRecord represents a running "hostagent" under control of
 * the hydra master controller.  It contains the host and pid, and the RMI
 * object used for communicating instructions to the agent.
 *
 */
public class HostAgentRecord implements Serializable {

   /** Host description for the host on which this process lives */
   private HostDescription hd;

   /** Process id for this process **/
   private int pid;

   /** Remote object (RMI) representing this process */
   private HostAgentIF agent;

   /// CONSTRUCTOR

   public HostAgentRecord(HostDescription hd, int pid, HostAgentIF agent) {
     this.hd = hd;
     this.pid = pid;
     this.agent = agent;
   }

   //// hd
   public HostDescription getHostDescription() {
     return hd;
   }

   //// pid
   public int getProcessId() {
     return pid;
   }

   //// host
   public String getHostName() {
     return hd.getHostName();
   }

   //// agent
   public HostAgentIF getHostAgent() {
     return agent;
   }

   //// printing
   public String toString() {
     return "hostagent@" + getHostName() + ":" + getProcessId();
   }
}
