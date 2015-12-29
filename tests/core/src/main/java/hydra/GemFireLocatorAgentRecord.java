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
 * A GemFireLocatorAgentRecord represents a running "locatoragent" under control
 * of the hydra master controller.  It contains the host, port, and pid, and the
 * RMI object used for communicating instructions to the agent.
 *
 */
public class GemFireLocatorAgentRecord implements Serializable {

   /** Distributed system for this locator */
   private String distributedSystem;

   /** Host name for this process */
   private String host;

   /** Host address for this process */
   private String addr;

   /** Port for this process **/
   private int port;

   /** Process id for this process **/
   private int pid;

   /** Whether this process is a server locator **/
   private boolean isServerLocator;

   /** Remote object (RMI) representing this process */
   private GemFireLocatorAgentIF agent;

   /// CONSTRUCTOR

   public GemFireLocatorAgentRecord() {
   }

   //// distributedSystem
   public String getDistributedSystem() {
     return distributedSystem;
   }
   public void setDistributedSystem(String aStr) {
     distributedSystem = aStr;
   }

   //// host
   public String getHostName() {
     return host;
   }
   public void setHostName(String aStr) {
     host = aStr;
   }

   //// addr
   public String getHostAddress() {
     return addr;
   }
   public void setHostAddress(String aStr) {
     addr = aStr;
   }

   //// port
   public int getPort() {
     return port;
   }
   public void setPort(int anInt) {
     port = anInt;
   }

   //// pid
   public int getProcessId() {
     return pid;
   }
   public void setProcessId(int anInt) {
     pid = anInt;
   }

   //// isServerLocator
   public boolean isServerLocator() {
     return isServerLocator;
   }
   public void setServerLocator(boolean aBool) {
     isServerLocator = aBool;
   }

   //// agent
   public GemFireLocatorAgentIF getGemFireLocatorAgent() {
     return agent;
   }
   public void setGemFireLocatorAgent(GemFireLocatorAgentIF anAgent) {
     agent = anAgent;
   }

   //// printing
   public String toString() {
     return "locatoragent_" + getDistributedSystem() + "_" +
             getHostName() + "_" + getProcessId() + "@" +
             getHostAddress() + "[" + getPort() + "]" +
             "(peer=true,server=" + isServerLocator() + ")";
   }
}
