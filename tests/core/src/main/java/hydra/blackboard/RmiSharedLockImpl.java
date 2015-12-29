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

import hydra.HydraRuntimeException;
import hydra.Log;
import hydra.RmiRegistryHelper;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.Map;

import com.gemstone.gemfire.LogWriter;

/**
 * Maintains a remotely shared lock for a blackboard instance.
 * <p>
 * Requires a thread name to distinguish a non-hydra thread from a hydra thread
 * with the same logical VM id, in case the enumerative JDK 1.5 thread ids and
 * the logical hydra thread ids overlap (which is highly likely).
 */
public class RmiSharedLockImpl
extends UnicastRemoteObject implements RmiSharedLock {

  private String ownerName = null;
  private int ownerVmid = -1;
  private long ownerTid = -1;
  private int lockval = 0;
  
  private Map conditions = new HashMap(1);

  public RmiSharedLockImpl() throws RemoteException {
    super();
  }

  protected static void bind(String name, RmiSharedLock value) {
    RmiRegistryHelper.bindInMaster(name, value);
    if (log().finerEnabled()) {
      log().finer("Bound " + name + "=" + value + " into master registry");
    }
  }
  protected static RmiSharedLock lookup(String name) throws RemoteException {
    if (log().finerEnabled()) {
      log().finer("Looking up " + name + " in master registry");
    }
    return (RmiSharedLock)RmiRegistryHelper.lookupInMaster(name);
  }
  private static LogWriter log() {
    return Log.getLogWriter();
  }
   
//------------------------------------------------------------------------------
// SharedLock interface
//------------------------------------------------------------------------------

  /**
   * Implements {@link RmiSharedLock#lock(String,int,long)}.
   */
  public void lock(String name, int vmid, long tid) throws RemoteException {
    synchronized (this) {
      if (lockval == 0) {
        ownerName = name;
        ownerVmid = vmid;
        ownerTid = tid;
        lockval = 1;
        return;
      }
      else if (ownerVmid == vmid && ownerTid == tid && ownerName != null
                                 && ownerName.equals(name)) {
        String s = ownerName + " with vmid=" + ownerVmid + " tid=" + ownerTid
                 + " already unlocked lock";
        throw new HydraRuntimeException(s);
      }
      else {
        boolean wasInterrupted = Thread.interrupted();
        try {
          while (true) {
            try {
              wait();
            }
            catch (InterruptedException e) {
              wasInterrupted = true;
            }
            if (lockval == 0) {
              ownerName = name;
              ownerVmid = vmid;
              ownerTid = tid;
              lockval = 1;
              return;
            }
            else if (ownerVmid == vmid && ownerTid == tid && ownerName != null
                                       && ownerName.equals(name)) {
              String s = ownerName + " with vmid=" + ownerVmid + " tid="
                       + ownerTid + " already locked lock";
              throw new HydraRuntimeException(s);
            }
          }
        }
        finally {
          if (wasInterrupted) Thread.currentThread().interrupt();
        }
      }
    }
  }

  /**
   * Implements {@link RmiSharedLock#unlock(String,int,long)}.
   */
  public synchronized void unlock(String name, int vmid, long tid) throws RemoteException {
    if (ownerVmid == vmid && ownerTid == tid && ownerName != null && ownerName.equals(name)) {
      if (lockval == 1) {
        ownerName = null;
        ownerVmid = -1;
        ownerTid = -1;
        lockval = 0;
        notify(); 
      }
      else {
        String s = ownerName + " with vmid=" + ownerVmid + " tid=" + ownerTid
                 + " already unlocked lock";
        throw new HydraRuntimeException(s);
      }
    }
    else {
      String s = "Not lock owner, it is owned by " + ownerName
               + " with vmid=" + ownerVmid + " tid=" + ownerTid;
      throw new HydraRuntimeException(s);
    }
  }

  /**
   * Implements {@link RmiSharedLock#getCondition(String)}.
   */
public synchronized RmiSharedCondition getCondition(String name) throws RemoteException {
  RmiSharedCondition condition = (RmiSharedCondition) conditions.get(name);
  if(condition == null) {
    condition = new RmiSharedConditionImpl();
    conditions.put(name, condition);
  }
  
  return condition;
}

/**
 * A remotely shared condition for a lock object.
 * @author dsmith
 *
 */
private class RmiSharedConditionImpl extends UnicastRemoteObject implements RmiSharedCondition {

  protected RmiSharedConditionImpl() throws RemoteException {
    super();
  }

  private static final long serialVersionUID = 8493062817798851101L;

  /**
   * Implements {@link RmiSharedCondition#await(String,int,long)}.
   */
  public void await(String name, int vmid, long tid) throws RemoteException, InterruptedException {
    if (ownerVmid != vmid || ownerTid != tid ||
       (ownerName != null && !ownerName.equals(name)))
    {
      String s = "Not lock owner, it is owned by " + ownerName
               + " with vmid=" + ownerVmid + " tid=" + ownerTid;
      throw new HydraRuntimeException(s);
    }
    
    synchronized (this) {
      unlock(name, vmid, tid);
      wait();
    }
    
    lock(name, vmid, tid);
  }

  /**
   * Implements {@link RmiSharedCondition#signal(String,int,long)}.
   */
  public void signal(String name, int vmid, long tid) throws RemoteException {
    if (ownerVmid != vmid || ownerTid != tid ||
       (ownerName != null && !ownerName.equals(name)))
    {
      String s = "Not lock owner, it is owned by " + ownerName
               + " with vmid=" + ownerVmid + " tid=" + ownerTid;
      throw new HydraRuntimeException(s);
    }
    
    synchronized (this) {
     notify();
    }
    
  }

  /**
   * Implements {@link RmiSharedCondition#signalAll(String,int,long)}.
   */
  public void signalAll(String name, int vmid, long tid) throws RemoteException {
    if (ownerVmid != vmid || ownerTid != tid ||
       (ownerName != null && !ownerName.equals(name)))
    {
      String s = "Not lock owner, it is owned by " + ownerName
               + " with vmid=" + ownerVmid + " tid=" + ownerTid;
      throw new HydraRuntimeException(s);
    }
    
    synchronized(this) {
      notifyAll();
    }
  }

}

}
