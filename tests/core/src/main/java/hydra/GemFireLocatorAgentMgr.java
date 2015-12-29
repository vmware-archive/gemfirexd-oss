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
import java.rmi.*;
import java.util.*;

/** 
*
* Manages the gemfire locator agents.
*
*/

public class GemFireLocatorAgentMgr {

  private static HashMap Agents = new HashMap();

  //////////////////////////////////////////////////////////////////////////////
  //                         LOCATOR AGENT BIRTH                              //
  //////////////////////////////////////////////////////////////////////////////

  /**
   *  Starts a locator agent for each distributed system that is configured
   *  to use a locator.
   */
  public static void startLocatorAgents() {
    if (TestConfig.tab().booleanAt(Prms.startLocatorAgentsBeforeTest)) {
      Set distributedSystems = new HashSet();
      Map gfds = TestConfig.getInstance().getGemFireDescriptions();
      String vmArgs = null;
      String classpath = null;
      for (Iterator i = gfds.values().iterator(); i.hasNext();) {
        GemFireDescription gfd = (GemFireDescription)i.next();
        if (gfd.getUseLocator().booleanValue())
          distributedSystems.add(gfd.getDistributedSystem());
        vmArgs = gfd.getExtraLocatorVMArgs(); // same for all names, actually
        classpath = gfd.getLocatorClassPath(); // same for all names, actually
      }
      if (distributedSystems.size() != 0) {
        log().info("Starting " + distributedSystems.size() + " locator agents");
        for (Iterator i = distributedSystems.iterator(); i.hasNext();) {
          String ds = (String)i.next();
          startLocatorAgent(ds, vmArgs, classpath);
        }
      }
    }
  }

  /**
   *  Fires up a locator agent process for the specified distributed
   *  system and waits for it to register with the master.
   */
  public static void startLocatorAgent( String ds, String vmArgs,
                                        String classpath ) {

    log().info( "Starting locator agent for " + ds + "..." );
    Java.javaGemFireLocatorAgent( ds, vmArgs, classpath );
    waitForLocatorAgentToRegister( ds );
    GemFireLocatorAgentRecord lar = (GemFireLocatorAgentRecord) Agents.get( ds );
    if ( lar.getProcessId() == -1 )
      throw new HydraRuntimeException( "Unable to start locator agent for " + ds );
    log().info( "Started locator agent " + lar );
    String host = lar.getHostName();
    String addr = lar.getHostAddress();
    int port = lar.getPort();
    String data = addr + "[" + port + "]" + "," + ds + "," + host + ","
                + addr + "," + port;
    FileUtil.appendToFile(DistributedSystemHelper.DISCOVERY_FILE_NAME, data + "\n");
  }

  //////////////////////////////////////////////////////////////////////////////
  //                         LOCATOR AGENT REGISTRATION                       //
  //////////////////////////////////////////////////////////////////////////////

  /** 
   *  Used by locator agent to register with the master that it's ready.
   */
  public static void registerGemFireLocatorAgent( GemFireLocatorAgentRecord lar ) {
    DistributedSystemHelper.Endpoint endpoint =
                                new DistributedSystemHelper.Endpoint(lar);
    log().info("Generated locator endpoint: " + endpoint);
    DistributedSystemBlackboard.getInstance().getSharedMap()
                               .put(new Integer(lar.getProcessId()), endpoint);
    log().info( lar + " registered." );
    Agents.put( lar.getDistributedSystem(), lar );
  }

  //////////////////////////////////////////////////////////////////////////////
  //                         LOCATOR AGENT LOOKUP                             //
  //////////////////////////////////////////////////////////////////////////////

  /**
   *  Waits for pending locator agent to register with master.
   */
  private static void waitForLocatorAgentToRegister( String ds ) {

    int maxWaitSec = 300;

    log().info( "Waiting " + maxWaitSec + " seconds for locator agent " + ds + " to register with master." );

    long timeout = System.currentTimeMillis() + maxWaitSec * 1000;
    int count = 0;
    while ( System.currentTimeMillis() < timeout ) {
      ++count;
      MasterController.sleepForMs(500);
      if ( Agents.get( ds ) != null ) {
        log().info( "Locator agent " + ds + " has registered." );
        return;
      } else {
        if ( count % 15 == 0 )
        log().info( "...waiting for locator agent " + ds + "..." );
      }
    }
    throw new HydraTimeoutException( "Failed to register locator agent " + ds + " within " + maxWaitSec + " seconds." );
  }

  //////////////////////////////////////////////////////////////////////////////
  //                         LOCATOR AGENT DEATH                              //
  //////////////////////////////////////////////////////////////////////////////

  /**
   *  Stops all of the locator agents in the test configuration.
   */
  public static void stopLocatorAgentsAfterTest() {
    if ( TestConfig.tab().booleanAt( Prms.stopLocatorAgentsAfterTest ) ) {
      if ( Agents.size() != 0 ) {
        log().info( "Stopping " + Agents.size() + " locator agents" );
        for ( Iterator i = Agents.keySet().iterator(); i.hasNext(); ) {
          String ds = (String) i.next();
          stopLocatorAgent( ds );
        }
      }
    } else {
      log().info( "Leaving the locator agents running" );
    }
  }

  /**
   *  Tells the locator agent to exit.  Does not wait for it to die.
   */
  public static void stopLocatorAgent( String ds ) {
    log().info("Shutting down locator agent for " + ds );
    GemFireLocatorAgentRecord lar =
      (GemFireLocatorAgentRecord) Agents.get( ds );
    if ( lar == null ) {
      waitForLocatorAgentToRegister( ds );
    }
    try {
      lar.getGemFireLocatorAgent().shutDownGemFireLocatorAgent();
    } catch ( UnmarshalException ignore ) {
      // vm is shutting down, don't expect clean reply
    } catch ( RemoteException e ) {
      throw new HydraRuntimeException( "Unable to reach locator agent " + lar, e );
    }
  }

  /**
   * Does a stack dump on all locator agents.
   * @since 5.0
   */
  public static void printProcessStacks() {
    for (Iterator i = Agents.values().iterator(); i.hasNext();) {
      GemFireLocatorAgentRecord lar = (GemFireLocatorAgentRecord)i.next();
      ProcessMgr.printProcessStacks(lar.getHostName(), lar.getProcessId());
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  //                             MISC SUPPORT                                 //
  //////////////////////////////////////////////////////////////////////////////

  private static LogWriter log() {
    return Log.getLogWriter();
  }
}
