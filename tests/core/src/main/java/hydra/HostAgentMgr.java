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

import java.rmi.RemoteException;
import java.rmi.UnmarshalException;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;

import com.gemstone.gemfire.LogWriter;

/** 
*
* Manages hostagents for master.
*
*/
public class HostAgentMgr {

  // hostagents that are starting but have not registered (hosts).
  private static Vector PendingHostAgents = new Vector();

  // map of hosts to hostagent records
  private static Hashtable HostAgents = new Hashtable();

  //////////////////////////////////////////////////////////////////////////////
  //                           HOSTAGENT CREATION                             //
  //////////////////////////////////////////////////////////////////////////////

  /**
  *
  * Fires up hostagent processes from a set of hosts and waits for them to
  * register with the master.  Computes clock skew information for each host.
  * The hostagent on the master host runs in-process.
  * @param hads the hostagent descriptions.
  *
  */
  protected static void startHostAgents( Map hads ) {
    if ( hads == null ) return;

    // start hostagent for each host
    int pauseSec = TestConfig.getHostAgentInstance().getParameters().intAt(
                       Prms.sleepBeforeStartingHostAgentSec,
                       Prms.DEFAULT_SLEEP_BEFORE_STARTING_HOSTAGENT_SEC);
    log().info( "Starting " + hads.size() + " hostagents with a " + pauseSec
              + " second pause in between each");
    for ( Iterator i = hads.values().iterator(); i.hasNext(); ) {
      HostAgentDescription had = (HostAgentDescription) i.next();
      // do not start a host agent on the localhost unless it's archiving
      if (had.getArchiveStats() || !HostHelper.isLocalHost(had.getHostDescription().getHostName())) {
        if (pauseSec > 0) {
          MasterController.sleepForMs(pauseSec*1000);
        }
        startHostAgent( had );
      }
    }

    // wait for hostagents to register with the master
    waitForHostAgentsToRegister();
    log().info( "Started " + HostAgents.size() + " hostagents" );
  }

  /**
   * Fires up the hostagent on its (remote) host.
   */
  private static void startHostAgent( HostAgentDescription had ) {

    String host = had.getHostDescription().getHostName();

    // start the vm
    log().info( "Starting hostagent on " + host + " using hostagent description " + had );
    int pid = Java.javaHostAgent(had);
    log().info("Started hostagent on " + host);

    // add the hostagent to the pending hostagent list
    synchronized( PendingHostAgents ) {
      PendingHostAgents.add( host );
      log().info( "Added " + host + " to PendingHostAgents" );
    }
  }

  /**
   * Used to fetch the nuke command from nukerun for a hostagent for use
   * after a reboot.
   */
  protected static String getNukePIDCommand(String host) {
    synchronized (HostAgents) {
      HostAgentRecord har = (HostAgentRecord)HostAgents.get(host);
      return Nuker.getInstance().getNukePIDCmd(har.getHostDescription(),
                                               har.getProcessId());
    }
  }

  /**
   * Used to restore a hostagent lost in a reboot. Updating the nukerun script
   * is handled in the ClientMgr.
   */
  protected static void restartHostAgent(String host) {
    log().info("Restarting hostagent on " + host);
    HostAgentRecord har = null;
    synchronized (HostAgents) {
      har = (HostAgentRecord)HostAgents.remove(host);
      if (har == null) {
        String s = "Hostagent for " + host + " not found";
        throw new HydraInternalException(s);
      }
    }
    Map<String,HostAgentDescription> hads =
      TestConfig.getInstance().getHostAgentDescriptions();
    for (HostAgentDescription had : hads.values()) {
      if (had.getHostDescription().getHostName().equals(host)) {
        startHostAgent(had);
        waitForHostAgentToRegister(host);
        break;
      }
    }
    log().info("Restarted hostagent on " + host);
  }

  //////////////////////////////////////////////////////////////////////////////
  //                           HOSTAGENT REGISTRATION                         //
  //////////////////////////////////////////////////////////////////////////////

  /** 
  *
  * Used by hostagents to register and go live so master can use them.
  *
  */
  protected static void registerHostAgent( HostAgentRecord har ) {

    synchronized( PendingHostAgents ) {
      synchronized( HostAgents ) {
        PendingHostAgents.remove( har.getHostName() );
        HostAgents.put( har.getHostName(), har );
      }
    }
    log().info( har + " registered." );
    Nuker.getInstance().recordPIDNoDumps(har.getHostDescription(),
                                         har.getProcessId());
  }

  //////////////////////////////////////////////////////////////////////////////
  //                           HOSTAGENT LOOKUP                               //
  //////////////////////////////////////////////////////////////////////////////

  /**
  *
  * Waits <code>Prms.maxHostAgentStartupWaitSec</code> for pending hostagents
  * to register with master.
  *
  */
  private static void waitForHostAgentsToRegister() {

    int maxWaitSec = TestConfig.getHostAgentInstance()
                               .getParameters()
                               .intAt( Prms.maxHostAgentStartupWaitSec );

    log().info( "Waiting " + maxWaitSec + " seconds for hostagents to register with master." );

    int lastnum = numPendingHostAgents();
    long timeout = System.currentTimeMillis() + maxWaitSec * 1000;
    while ( System.currentTimeMillis() < timeout ) {
      MasterController.sleepForMs(1000);
      int currentnum = numPendingHostAgents();
      if ( currentnum == 0 ) {
        log().info( "All hostagents have registered." );
        return;
      } else if ( currentnum != lastnum ) {
        log().info( "...waiting for " + currentnum + " hostagents..." );
        lastnum = currentnum;
      }
    }
    throw new HydraTimeoutException( "Failed to register hostagents within " +
                                      maxWaitSec + " seconds." );
  }

  /**
   * Waits <code>Prms.maxHostAgentStartupWaitSec</code> for the hostagent
   * to register with master.
   */
  private static void waitForHostAgentToRegister(String host) {
    if (!isRegistered(host)) {
      int maxWaitSec = TestConfig.getHostAgentInstance().getParameters()
                                 .intAt(Prms.maxHostAgentStartupWaitSec);
      log().info("Waiting " + maxWaitSec + " seconds for hostagent on host "
                + host + " to register with master.");
      long timeout = System.currentTimeMillis() + maxWaitSec * 1000;
      while (System.currentTimeMillis() < timeout) {
        MasterController.sleepForMs(1000);
        if (isRegistered(host)) {
          log().info("Hostagent on host " + host + " has registered.");
          return;
        } else {
          log().info("...waiting for hostagent on host " + host + "...");
        }
      }
      String s = "Failed to register hostagent on host" + host
               + " within " + maxWaitSec + " seconds.";
      throw new HydraTimeoutException(s);
    }
  }

  private static boolean isRegistered(String host) {
    synchronized (HostAgents) {
      return HostAgents.containsKey(host);
    }
  }

  /**
  *
  * Returns the number of pending hostagents.
  *
  */
  private static int numPendingHostAgents() {
    synchronized( PendingHostAgents ) {
      synchronized( HostAgents ) {
        for (Iterator i = PendingHostAgents.iterator(); i.hasNext();) {
          String host = (String)i.next();
          if (HostAgents.containsKey(host)) {
            i.remove();
          }
        }
      }
      return PendingHostAgents.size();
    }
  }

  /**
  *
  * Returns the hostagent for the specified host.
  *
  */
  protected static HostAgentIF getHostAgent( String host ) {
    synchronized( HostAgents ) {
      if ( HostHelper.isLocalHost( host ) ) {
        String s = "Use Platform instead of HostAgent for localhost";
        throw new HydraInternalException(s);
      } else if (RemoteTestModule.Master == null) { // hydra master
        // return remote host agent
        HostAgentRecord har = (HostAgentRecord) HostAgents.get( host );
        if ( har == null ) {
          throw new HydraInternalException
          (
            "No hostagent has been configured for remote host " + host
          );
        }
        return har.getHostAgent();
      } else { // hydra client
        // check whether HostAgentIF has previously been looked up
        HostAgentIF ha = (HostAgentIF)HostAgents.get(host);
        if (ha == null) {
          // work through the master to get a hostagent
          try {
            ha = RemoteTestModule.Master.getHostAgent(host);
            // removed client-side caching to support reboot
            //HostAgents.put(host, ha);
          } catch (RemoteException e) {
            String s = "Unable to access master to get remote hostagent";
            throw new HydraRuntimeException(s);
          }
        }
        return ha;
      }
    }
  }

  /**
   * Returns the hostagent for the specified host for a remote client.
   * This method is invoked in the master from a client through the
   * master proxy only when the host is remote from the client.
   */
  public static HostAgentIF getRemoteHostAgentForClient(String host) {
    synchronized(HostAgents) {
      if (RemoteTestModule.Master == null) { // hydra master
        HostAgentRecord har = (HostAgentRecord)HostAgents.get(host);
        if (har == null) {
          String s = null;
          if (HostHelper.isLocalHost(host)) {
            // TBD: ditch the embedded hostagent in the master and just
            // always create one on the master host in case a remote client
            // wants to spawn a job there
            s = "No hostagent has been configured for localhost: " + host;
            throw new UnsupportedOperationException(s);
          } else {
            s = "No hostagent has been configured for remote host " + host;
            throw new HydraInternalException(s);
          }
        }
        return har.getHostAgent();
      } else { // hydra client
        String s = "This method must not be invoked from a hydra client.";
        throw new HydraRuntimeException(s);
      }
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  //                           HOSTAGENT DEATH                                //
  //////////////////////////////////////////////////////////////////////////////

  // @todo lises wait for rmi return here, use separate thread so can timeout
  // based on Prms.maxHostAgentShutdownWaitSec.
  /**
  *
  * Tells all hostagents to exit.  Does not wait for processes to die.
  *
  */
  protected static void shutDownAllHostAgents() {
    Map hads = TestConfig.getHostAgentInstance().getHostAgentDescriptions();

    log().info("Shutting down hostagents");

    if ( numPendingHostAgents() > 0 )
      waitForHostAgentsToRegister();

    for ( Iterator i = HostAgents.values().iterator(); i.hasNext(); )
      shutDownHostAgent( (HostAgentRecord) i.next() );
  }

  /**
  *
  * Tell a particular hostagent to exit.
  *
  */
  private static void shutDownHostAgent( HostAgentRecord har ) {
    try {
      har.getHostAgent().shutDownHostAgent();
    } catch ( UnmarshalException ignore ) {
      // vm is shutting down, don't expect clean reply
    } catch ( RemoteException e ) {
      // vm is shutting down, don't expect clean reply
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  //                             MISC SUPPORT                                 //
  //////////////////////////////////////////////////////////////////////////////

  private static LogWriter log() {
    return Log.getLogWriter();
  }
}
