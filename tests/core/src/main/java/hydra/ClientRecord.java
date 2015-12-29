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

/**
 *  A ClientRecord represents a running "worker" thread under control of the
 *  hydra master controller.  It contains the logical thread ID, the hosting
 *  {@link ClientVmRecord}, and the RMI object used for communicating test
 *  instructions to the thread.  It also contains runtime information used to
 *  manage thread scheduling and task execution.
 */
public class ClientRecord implements Comparable, Serializable {

  //////////////////////////////////////////////////////////////////////////////
  ////    INSTANCE FIELDS                                                   ////
  //////////////////////////////////////////////////////////////////////////////

  /**
   *  The logical ID used to identify and manage this thread.
   */
  private int tid = -1;

  /**
   *  The remote object (RMI) representing this thread.  Set by the client.
   */
  private RemoteTestModuleIF mod;

  /**
   *  Back pointer to the VM record for this thread and its cohorts.
   */
  private transient ClientVmRecord vm;

  /**
   *  The threadgroup to which client is assigned.  Used in scheduling.
   */
  private transient HydraThreadGroup tg;

  /**
   *  The name of the threadgroup the thread is mapped to.  Used for dynamic
   *  start and dunit.
   */
  private String tgname;

  /**
   *  The logical ID used to identify the thread within its threadgroup.  Used
   *  for dynamic start.
   */
  private int tgid = -1;

  //////////////////////////////////////////////////////////////////////////////
  ////    RUNTIME FIELDS                                                    ////
  //////////////////////////////////////////////////////////////////////////////

  /**
   *  Cached descriptive string containing no runtime information except pid.
   */
  private String cstr; // only accessed under synchronization

  /**
   *  Tracks whether the client is still executing a task.
   */
  private volatile boolean busy;

  /**
   *  Tracks whether the client had an error.
   */
  private boolean hadError; // only accessed under synchronization

  /**
   *  Tracks the most recent task (may be completed).
   */
  private TestTask task; // only accessed under synchronization

  /**
   *  Tracks the most recent task index (may be completed).
   */
  private volatile int taskIndex;

  /**
   *  Tracks the start time of most recent task assignment (may be completed).
   */
  private volatile long startTime;

  //////////////////////////////////////////////////////////////////////////////
  ////    CONSTRUCTORS                                                      ////
  //////////////////////////////////////////////////////////////////////////////

  /**
   *  Used by schedulers to create pending clients.
   */
  protected ClientRecord( int tid, ClientVmRecord vm ) {
    this.tid = tid;
    this.vm = vm;

    initCachedString();
  }

  /**
   *  Used by schedulers to initialize and reset the runtime state.
   */
  protected void initRuntime() {
    this.busy = false;
    this.hadError = false;
    this.taskIndex = -1;
    this.task = null;
    this.startTime = 999999;
  }

  //////////////////////////////////////////////////////////////////////////////
  ////    ACCESSORS                                                         ////
  //////////////////////////////////////////////////////////////////////////////

  public ClientVmRecord vm() {
    return this.vm;
  }

  public int getTid() {
    return this.tid;
  }

  public RemoteTestModuleIF getTestModule() {
    return this.mod;
  }
  protected void setTestModule( RemoteTestModuleIF mod ) {
    this.mod = mod;
  }

  public HydraThreadGroup getThreadGroup() {
    return this.tg;
  }
  protected void setThreadGroup( HydraThreadGroup tg ) {
    this.tg = tg;
  }

  public String getThreadGroupName() {
    return this.tgname;
  }
  protected void setThreadGroupName( String tgname ) {
    this.tgname = tgname;
  }

  public int getThreadGroupId() {
    return this.tgid;
  }
  protected void setThreadGroupId( int tgid ) {
    this.tgid = tgid;
  }

  //////////////////////////////////////////////////////////////////////////////
  ////    RUNTIME ACCESSORS                                                 ////
  //////////////////////////////////////////////////////////////////////////////

  protected boolean isBusy() {
    return busy;
  }
  protected void setBusy(boolean aBusy) {
    if ( busy == aBusy )
      throw new HydraInternalException(this + " already has busy set " + busy);
    busy = aBusy;
  }

  protected boolean hadError() {
    return hadError;
  }
  protected void setError() {
    if ( hadError )
      throw new HydraInternalException(this + " already has error set");
    hadError = true;
  }

  protected TestTask getTask() {
    return task;
  }
  protected void setTask(TestTask t) {
    task = t;
    taskIndex = t.getTaskIndex();
  }

  // used as a scheduling marker by the ClientNameTaskScheduler
  protected int getTaskIndex() {
    return taskIndex;
  }
  protected void setTaskIndex(int i) {
    taskIndex = i;
  }

  protected long getStartTime() {
    return startTime;
  }
  protected void setStartTime(long t) {
    startTime = t;
  }

  //////////////////////////////////////////////////////////////////////////////
  ////    COMPARABLE                                                        ////
  //////////////////////////////////////////////////////////////////////////////

  public int compareTo( Object o ) {
    int  me = this.getTid();
    int you = ((ClientRecord)o).getTid();
    if ( me == you ) {
      return 0;
    } else if ( me > you ) {
      return 1;
    } else {
      return -1;
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  ////    PRINTING                                                          ////
  //////////////////////////////////////////////////////////////////////////////

  /**
   *  Print information about the client.
   */
  public String toString() {
    return this.cstr;
  }

  /**
   *  Caches the client string, initialized as part of client registration.
   */
  protected void initCachedString() {
    this.cstr = MasterController.getNameFor( vm().getVmid().intValue(),
                                             getTid(), vm().getClientName(),
                                             vm().getHost(), vm().getPid() );
  }
}
