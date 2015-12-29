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

import com.gemstone.gemfire.LogWriter;
import java.rmi.RemoteException;
import java.util.*;

/**
 * This class provides support for {@link ClientVmMgr} and {@link RebootMgr}.
 * For internal hydra use only.
 */
public class DynamicActionUtil {

  // synchronization objects for dynamic actions
  private static Map DynamicActions = new HashMap();

  // unique reservation number for each dynamic action
  private static int actionId = 0;

  protected static synchronized int nextActionId() {
    return ++actionId;
  }

  /**
   * Creates and runs a dynamic action thread to carry out the action on behalf
   * of the client. This allows clients invoking asynchronous versions of the
   * action to immediately get back to work. The thread handles any exceptions
   * that occur during the action. If the action is synchronous, this method
   * does not return until the dynamic action thread completes.
   */ 
  protected static void runActionThread(int actionId, String act,
            Runnable action, String name, boolean synchronous) {
    log().info("Issuing " + act);
    PidSignal signal = null;
    if (synchronous) {
      signal = setUpDynamicActionCompleteSignal(actionId);
    }
    JoinerThread t = new JoinerThread(action, name);
    t.start();
    if (synchronous) {
      try {
        log().info("Waiting for completion of " + act);
        t.join();
      } catch (InterruptedException e) {
        log().severe("Thread was interrupted");
      }
      waitForDynamicActionToComplete(actionId, signal);
      log().info("Completed " + act);
    } else {
      log().info("Not waiting for completion of " + act);
    }
  }

  /**
   * Used by clients to set up a signal to use to wait for the results of
   * synchronous actions.
   */
  private static PidSignal setUpDynamicActionCompleteSignal(int actionId) {
    int mypid = RemoteTestModule.getMyPid();
    PidSignal signal = new PidSignal(mypid);
    synchronized(DynamicActions) {
      DynamicActions.put(Integer.valueOf(actionId), signal);
    }
    return signal;
  }

  /**
   * Used by clients to wait for the results of synchronous actions.
   */
  private static void waitForDynamicActionToComplete(int actionId,
                                                     PidSignal signal) {
    synchronized (signal) {
      try {
        while (!signal.notified()) {
          signal.wait();
        }
      } catch (InterruptedException e) {
        log().severe("Thread was interrupted");
      }
    }
    synchronized (DynamicActions) {
      DynamicActions.remove(Integer.valueOf(actionId));
    }
  }

  /**
   * Used by master to notify the client that the action is complete. Only
   * invoked for synchronous actions. Goes through a client intermediary.
   */
  protected static void notifyDynamicActionComplete(int actionId) {
    PidSignal signal = (PidSignal)DynamicActions.get(Integer.valueOf(actionId));
    if (signal != null && RemoteTestModule.getMyPid() == signal.getPid()) {
      synchronized (signal) {
        signal.signal();
        signal.notify();
      }
    } // else resource has gone down and back up, so drop signal on the floor
  }

  /**
   * Used to synchronize master and synchronously acting clients.
   */
  private static class PidSignal {
    int pid = -1;
    protected PidSignal(int pid) {
      this.pid = pid;
    }
    /** Gets the pid. */
    protected int getPid() {
      return this.pid;
    }
    /** Signals the signal. */
    protected void signal() {
      this.pid = -1;
    }
    /** Returns true if this signal has already been signaled. */
    protected boolean notified() {
      return this.pid == -1;
    }
    public String toString() {
      return String.valueOf(this.pid);
    }
  }

  private static LogWriter log() {
    return Log.getLogWriter();
  }
}
