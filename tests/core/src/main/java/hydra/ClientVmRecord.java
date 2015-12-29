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

import hydra.HostHelper.OSType;
import java.io.Serializable;
import java.rmi.*;
import java.util.*;

/**
 *  A ClientVmRecord represents a running VM under control of the hydra master
 *  controller.  It holds the {@link ClientRecord} objects for the client
 *  threads in this VM, plus runtime VM management information, such as the PID.
 */
public class ClientVmRecord implements Serializable {

  //////////////////////////////////////////////////////////////////////////////
  ////    CONSTANTS
  //////////////////////////////////////////////////////////////////////////////

  /**
   *  Indicates that the process has no valid PID.
   */
  protected static int NO_PID = -1;

  protected static final int POTENTIAL = 0;
  protected static final int PENDING   = 1;
  protected static final int LIVE      = 2;
  protected static final int DEAD      = 3;
  protected static final int DISCONNECTED = 4;

  //////////////////////////////////////////////////////////////////////////////
  ////    INSTANCE FIELDS
  //////////////////////////////////////////////////////////////////////////////

  /**
   *  The logical ID used to identify and manage this VM.
   */
  private Integer vmid;

  /**
   *  The base thread ID used to number client threads for this VM.  Used for
   *  dynamic start.
   */
  private int basetid;

  /**
   *  The {@link ClientDescription} used to create this VM.
   */
  private ClientDescription cd;

  /**
   *  The set of {@link ClientRecord} objects for this VM, one for each logical
   *  client thread.
   */
  private HashMap<Integer,ClientRecord> crs = new HashMap();

  /**
   *  Cached descriptive string containing no runtime information.
   */
  private String cstr;

  //////////////////////////////////////////////////////////////////////////////
  ////    RUNTIME FIELDS
  //////////////////////////////////////////////////////////////////////////////

  /**
   *  The state of this vm.
   */
  private volatile int state;

  /**
   *  The current PID for this VM.  When the VM is not running, the PID is set
   *  to {@link #NO_PID}.
   */
  private volatile int pid;

  /**
   * The Java homes this VM is to be started with, if any.
   * Each time the VM restarts, the Java home is bumped for the next time
   * by removing the first entry from the list until only one remains.
   */
   private volatile List<String> javaHomes;

  /**
   * The product versions this VM is to be started with, if any.
   * Each time the VM restarts, the version is bumped for the next time
   * by removing the first entry from the map until only one remains.
   */
   private volatile Map<String,String> versionMapping;

  /**
   *  The number of client threads currently registered for this vm.
   */
  private int numClientsRegistered; // only accessed under synchronization

  /**
   *  How many clients in this vm are initiating a dynamic action.
   */
  private int actionCount; // only accessed under synchronization

  /**
   *  Whether this vm has been reserved for dynamic action.
   */
  private volatile boolean reserved;

  /**
   *  Whether this vm has been disconnected as part of a dynamic action.
   *  Used to notify the master that the disconnect is complete for a given
   *  action. It is reset to false after master has been notified.
   */
  private volatile boolean disconnected;

  //////////////////////////////////////////////////////////////////////////////
  ////    CONSTRUCTORS
  //////////////////////////////////////////////////////////////////////////////

  public ClientVmRecord( int vmid, int basetid, ClientDescription cd ) {
    this.vmid = new Integer( vmid );
    this.basetid = basetid;
    this.cd = cd;
    this.cstr = "vm_" + vmid;

    JDKVersionDescription jvd = cd.getJDKVersionDescription();
    if (jvd != null) {
      List<String> j = jvd.getJavaHomes();
      if (j != null) {
        Object jcopy = ((ArrayList<String>)j).clone();
        this.javaHomes = (List<String>)jcopy;
      }
    }

    VersionDescription vd = cd.getVersionDescription();
    if (vd != null) {
      Map<String,String> m = vd.getVersionMapping();
      Object mcopy = ((LinkedHashMap<String,String>)m).clone();
      this.versionMapping = (Map<String,String>)mcopy;
    }

    reset();
  }

  //////////////////////////////////////////////////////////////////////////////
  ////    ACCESSORS
  //////////////////////////////////////////////////////////////////////////////

  public Integer getVmid() {
    return this.vmid;
  }

  protected int getBaseThreadId() {
    return this.basetid;
  }

  protected Map<Integer,ClientRecord> getClients() {
    return this.crs;
  }

  public ClientRecord getClient( int tid ) {
    ClientRecord cr = (ClientRecord) this.crs.get( new Integer( tid ) );
    if ( cr == null ) {
      throw new HydraInternalException( this + " missing client #" + tid );
    }
    return cr;
  }

  public ClientRecord getRepresentativeClient() {
    for ( Iterator i = this.crs.values().iterator(); i.hasNext(); ) {
      return (ClientRecord) i.next();
    }
    return null;
  }

  protected void addClient( ClientRecord cr ) {
    this.crs.put( new Integer( cr.getTid() ), cr );
  }

  /**
   *  Returns a copy of the client record values map for use by the mapper.
   *  This is necessary since the mapper is destructive to the collection.
   */
  protected Collection getCopyOfClientCollection() {
    return ((Map)this.crs.clone()).values();
  }

  public ClientDescription getClientDescription() {
    return this.cd;
  }

  public String getClientName() {
    return this.cd.getName();
  }

  protected String getLogicalHost() {
    return this.cd.getVmDescription().getHostDescription().getName();
  }

  protected String getHost() {
    return this.cd.getVmDescription().getHostDescription().getHostName();
  }

  protected OSType getOSType() {
    return this.cd.getVmDescription().getHostDescription().getOSType();
  }

  //////////////////////////////////////////////////////////////////////////////
  ////    RUNTIME ACCESSORS
  //////////////////////////////////////////////////////////////////////////////

  protected int getState() {
    return this.state;
  }
  protected void setState( int state ) {
    if ( this.state == state ) {
      throw new HydraInternalException( "state already set: " + this );
    }
    this.state = state;
  }
  public String getStateString() {
    switch( this.state ) {
      case POTENTIAL: return "potential";
      case PENDING:   return "pending";
      case LIVE:      return "live";
      case DEAD:      return "dead";
      case DISCONNECTED: return "disconnected";
      default:
        throw new HydraInternalException( "Unknown state: " + state );
    }
  }
  public String getStateAsString( int state ) {
    switch( state ) {
      case POTENTIAL: return "potential";
      case PENDING:   return "pending";
      case LIVE:      return "live";
      case DEAD:      return "dead";
      case DISCONNECTED:      return "disconnected";
      default:
        throw new HydraInternalException( "Unknown state: " + state );
    }
  }

  protected String getJavaHome() {
    if (this.javaHomes == null) {
      return this.cd.getVmDescription().getHostDescription().getJavaHome();
    } else {
      String val = this.javaHomes.get(0);
      if (val.equals(BasePrms.DEFAULT)) {
        return this.cd.getVmDescription().getHostDescription().getJavaHome();
      } else {
        return val;
      }
    }
  }

  protected void bumpJavaHome() {
    if (this.javaHomes != null) {
      String first = this.javaHomes.get(0);
      if (this.javaHomes.size() > 1) {
        this.javaHomes.remove(first);
        Log.getLogWriter().info("Bumped Java home from " + first + " to "
           + this.javaHomes.get(0) + " for next startup for VM " + this.vmid);
      } else {
        Log.getLogWriter().info("Sticking with Java home " + first
           + " for VM " + this.vmid);
      }
    }
  }

  protected String getVersion() {
    return this.versionMapping.keySet().iterator().next();
  }

  protected void bumpVersion() {
    if (this.versionMapping != null) {
      String first = this.versionMapping.keySet().iterator().next();
      if (this.versionMapping.size() > 1) {
        this.versionMapping.remove(first);
        Log.getLogWriter().info("Bumped version from " + first + " to "
           + this.versionMapping.keySet().iterator().next() + " for next startup for VM "
           + this.vmid);
      } else {
        Log.getLogWriter().info("Sticking with version " + first
           + " for VM " + this.vmid);
      }
    }
  }

  public int getPid() {
    return this.pid;
  }
  protected void setPid( int pid ) {
    if ( this.pid == pid ) {
      throw new HydraInternalException( "pid already set: " + this );
    }
    this.pid = pid;
    // update the runtime portion of the client string
    for ( Iterator i = getClients().values().iterator(); i.hasNext(); ) {
      ClientRecord cr = (ClientRecord) i.next();
      cr.initCachedString();
    }
  }

  /**
   *  Registers a client with this vm.
   */
  protected void registerClient() {
    ++this.numClientsRegistered;
  }
  /**
   *  Returns true if all clients in this vm have registered.
   */
  protected boolean fullyRegistered() {
    return this.numClientsRegistered == this.crs.size();
  }

  /**
   *  Increments the count of clients initiating a dynamic action.
   */
  protected void incDynamicActionCount() {
    if ( this.reserved ) {
      String s = "attempt to initiate dynamic action from reserved vm";
      throw new HydraInternalException( s );
    }
    ++this.actionCount;
  }
  /**
   *  Decrements the count of clients initiating a dynamic action.
   */
  protected void decDynamicActionCount() {
    --this.actionCount;
  }
  /**
   *  Returns true if any client in the vm is in the process of initiating a
   *  dynamic action.  This method is used to prevent interrupting a client in
   *  between the two RMI calls needed to initiate a dynamic action.  It makes
   *  an atomic action out of reserving a target and invoking (but not waiting
   *  for return of) the action call.
   */
  protected boolean acting() {
    return this.actionCount != 0;
  }

  protected void reserve() {
    if ( this.reserved ) {
      throw new HydraInternalException( "already reserved: " + this );
    } else if ( this.actionCount != 0 ) {
      String s = "attempt to reserve vm that is initiating dynamic action";
      throw new HydraInternalException( s );
    } else {
      this.reserved = true;
    }
  }
  protected void unreserve() {
    if ( ! this.reserved ) {
      throw new HydraInternalException( "already unreserved: " + this );
    } else {
      this.reserved = false;
    }
  }
  protected boolean reserved() {
    return this.reserved;
  }

  /**
   *  Returns true if the vm is undergoing dynamic action.
   *  Used by the complex task scheduler to avoid scheduling inappropriately.
   */
  protected boolean isDynamic() {
    return this.state != LIVE || this.reserved;
  }

  protected void registerDisconnect() {
    if (this.disconnected) {
      throw new HydraInternalException("already disconnected");
    }
    this.disconnected = true;
  }

  protected boolean registeredDisconnect() {
    return this.disconnected;
  }

  /**
   * Returns true if the vm was previously disconnected in a dynamic action and is not
   * otherwise dead. Used by the stop/start machinery to avoid restarting a JVM
   * inappropriately.
   */
  protected boolean isDisconnected() {
    return this.state == DISCONNECTED;
  }

  /**
   *  Returns true if the vm is live.
   *  Used by the simple task scheduler to avoid scheduling inappropriately.
   */
  protected boolean isLive() {
    return this.state == LIVE;
  }

  /**
   * Returns true if the vm is dead. Used by the {@link RebootMgr} to avoid
   * returning permanently dead vms.
   */
  protected boolean isDead() {
    return this.state == DEAD;
  }

  /**
   *  Resets the vm to its primordial state (except for version and java home).
   */
  protected void reset() {
    this.state = POTENTIAL;
    this.pid = NO_PID;
    this.numClientsRegistered = 0;
    this.actionCount = 0;
    this.reserved = false;
    this.disconnected = false;
  }

  /**
   * Resets the vm to the proper state after a dynamic disconnect.
   */
  protected void resetForDisconnect() {
    this.state = DISCONNECTED;
    this.actionCount = 0;
    this.reserved = false;
    this.disconnected = false;
  }

  //////////////////////////////////////////////////////////////////////////////
  ////    MATCHING
  //////////////////////////////////////////////////////////////////////////////

  /**
   *  Answers whether this client VM satisfies the requirements provided by the
   *  {@link ClientVmInfo}.
   */
  protected boolean matches( ClientVmInfo info ) {
    return
      ( info.getVmid() == null
        || info.getVmid().equals( getVmid()  ) ) &&
      ( info.getClientName() == null
        || info.clientNameMatches(getClientName())) &&
      ( info.getLogicalHost() == null
        || info.getLogicalHost().equals( getLogicalHost() ) );
  }

  //////////////////////////////////////////////////////////////////////////////
  ////    NOTIFICATION
  //////////////////////////////////////////////////////////////////////////////

  /**
   *  Notifies this client VM that the dynamic action is complete.  Uses any
   *  handy client record to connect.
   */
  public void notifyDynamicActionComplete( int actionId, int lastKnownPid ) {
    ClientRecord cr = getRepresentativeClient();
    RemoteTestModuleIF rem = cr.getTestModule();
    try {
      rem.notifyDynamicActionComplete( actionId );
    } catch( RemoteException e ) {
      if ( this.getPid() == lastKnownPid ) {
        throw new DynamicActionException("Unable to reach " + cr, e);
      } // else ignore, client is gone or went down and back up again
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  ////    PRINTING
  //////////////////////////////////////////////////////////////////////////////

  /**
   *  Returns a fully specified {@link ClientVmInfo} for this VM.
   */
  protected ClientVmInfo getInfo() {
    return new ClientVmInfo( getVmid(),
                             getClientName(),
                             getLogicalHost() );
  }

  /**
   *  Print (cached) non-runtime information about the vm.
   */
  public String toString() {
    return this.cstr;
  }

  public String toInfoString() {
    int maxtid = getBaseThreadId() + getClients().size() - 1;
    return this
           + "_thr_" + getBaseThreadId() + "_to_" + maxtid
           + "_" + getClientName()
           + "_" + getHost() + "_" + getPid()
           + "_" + this.getStateString();
  }

  public String toStateString() {
    return this.cstr
           + "_" + getClientName()
           + "_" + getHost() + "_" + getPid()
           + "_" + this.getStateString();
  }

  public String toClientString() {
    StringBuffer buf = new StringBuffer();
    buf.append( this.toInfoString() );
    for ( Iterator i = this.crs.values().iterator(); i.hasNext(); ) {
      ClientRecord cr = (ClientRecord) i.next();
      buf.append( " " ).append( cr );
    }
    return buf.toString();
  }
}
