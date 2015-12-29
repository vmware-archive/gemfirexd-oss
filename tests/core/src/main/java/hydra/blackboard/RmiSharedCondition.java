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
package hydra.blackboard;

import java.rmi.Remote;
import java.rmi.RemoteException;


/**
 * Rmi interface for {@link SharedCondition}
 * @author dsmith
 *
 */
public interface RmiSharedCondition extends Remote {
  
  /**
   * RMI version of {@link SharedCondition#await()}.  Transmits the thread name,
   * logical hydra VM id, and thread id.
   */
  void await(String name, int vmid, long tid) throws RemoteException, InterruptedException;
  
  /**
   * RMI version of {@link SharedCondition#signal()}.  Transmits the thread,
   * logical hydra VM id, and thread id.
   */
  void signal(String name, int vmid, long tid) throws RemoteException;
  
  /**
   * RMI version of {@link SharedCondition#signalAll()}.  Transmits the thread,
   * logical hydra VM id, and thread id.
   */
  void signalAll(String name, int vmid, long tid) throws RemoteException;

}
