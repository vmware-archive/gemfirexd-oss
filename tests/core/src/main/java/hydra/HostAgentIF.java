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

import java.io.File;
import java.rmi.*;

/**
 *
 *  This interface allows the hydra master controller to manage local and
 *  remote processes, servers, and systems via host agents.  It provides for
 *  remote execution of scripts and methods that would otherwise be restricted
 *  to operations on local resources.
 *
 *  This interface is for internal hydra use only.  Clients must work through
 *  {@link MasterProxyIF}.
 *
 */
public interface HostAgentIF extends Remote {

  //// process management ////

  public int bgexec(String command, File workdir, File log) throws RemoteException;
  public String fgexec(String command, int maxWaitSec) throws RemoteException;
  public String fgexec(String command, String[] envp, int maxWaitSec) throws RemoteException;
  public String fgexec(String[] command, int maxWaitSec) throws RemoteException;

  public boolean exists(int pid) throws RemoteException;

  public void shutdown(int pid) throws RemoteException;
  public void kill(int pid) throws RemoteException;
  public void printStacks(int pid) throws RemoteException;
  public void dumpHeap(int pid, String userDir, String options) throws RemoteException;

  public String getShutdownCommand(int pid) throws RemoteException;
  public String getKillCommand(int pid) throws RemoteException;
  public String getDumpLocksCommand(int pid) throws RemoteException;
  public String getPrintStacksCommand(int pid) throws RemoteException;
  public String[] getDumpHeapCommand(int pid, String userDir, String options) throws RemoteException;
  public String getNetcontrolCommand(String target, int op) throws RemoteException;
  public String getProcessStatus(int maxWaitSec) throws RemoteException;

  //// hostagent management ////

  public void shutDownHostAgent()
  throws RemoteException;
}
