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

package hydra.gemfirexd;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import hydra.BasePrms;
import hydra.DistributedSystemHelper;
import hydra.HostDescription;
import hydra.HydraInternalException;
import hydra.HydraRuntimeException;
import hydra.Log;
import hydra.MasterController;
import hydra.RemoteTestModule;
import hydra.VmDescription;
import java.util.*;

/**
 * Helps hydra clients manage loners.  Methods are thread-safe.
 */
public class LonerHelper {

  /** Description used to connect the current loner distributed system */
  protected static LonerDescription TheLonerDescription;

  /** Properties used to connect the current loner distributed system */
  private static Properties TheLonerProperties;

  private static LogWriter log = Log.getLogWriter();

//------------------------------------------------------------------------------
// Loner
//------------------------------------------------------------------------------

  /**
   * Connects to a loner distributed system properties from the {@link
   * LonerDescription} to which this VM is wired using {@link
   * LonerPrms#clientNames}.  Returns the existing distributed system
   * if it is already started.
   *
   * @throws HydraRuntimeException if an attempt is made to reconfigure an
   *         existing system.
   */
  public static synchronized DistributedSystem connect() {
    log.info("Connecting the loner distributed system");
    Properties p = getLonerProperties();
    return connect(p);

  }

  private static synchronized DistributedSystem connect(Properties p) {
    DistributedSystem loner = DistributedSystemHelper.getDistributedSystem();
    if (loner == null) {

      // connect to the distributed system
      log.info("Connecting to loner distributed system: " + prettyprint(p));
      loner = DistributedSystem.connect(p);
      log.info("Connected to loner distributed system");

      // save the distributed system config for future reference
      TheLonerProperties = p;

    } else {
      if (TheLonerProperties == null) {
        // block attempt to create distributed system in multiple ways
        String s = "Distributed system was already connected without"
                 + " LonerHelper using an unknown, and possibly"
                 + " different, configuration";
        throw new HydraRuntimeException(s);

      } else {
        if (!TheLonerProperties.equals(p)) {
          // block attempt to reconnect system with clashing properties
          String s = "Distributed system already exists using properties "
                   + TheLonerProperties + ", cannot also use " + p;
          throw new HydraRuntimeException(s);

        } // else it was already created with these properties, which is fine
      }
      log.info("Already connected to loner distributed system");
    }
    return loner;
  }

  /**
   * Returns the loner distributed system if it is connected, otherwise null.
   */
  public static synchronized DistributedSystem getDistributedSystem() {
    return InternalDistributedSystem.getAnyInstance();
  }

  /**
   * Disconnects the loner distributed system if it is connected.
   */
  public static synchronized void disconnect() {
    DistributedSystem loner = getDistributedSystem();
    if (loner != null) {
      log.info("Disconnecting from the loner distributed system: "
         + loner.getName());
      loner.disconnect();
      log.info("Disconnected from the loner distributed system");
      TheLonerProperties = null; // so the next connect can have a
                                 // different config
    }
  }

//------------------------------------------------------------------------------
// Properties
//------------------------------------------------------------------------------

  /**
   * Returns the system properties from the {@link LonerDescription}
   * to which this VM is wired using {@link LonerPrms#clientNames}.
   */
  public static Properties getLonerProperties() {
    LonerDescription ld = getLonerDescription();
    return getLonerProperties(ld);
  }

  /**
   * Returns system properties using the description.
   */
  private static synchronized Properties getLonerProperties(
                                         LonerDescription ld) {
    log.info("Looking up loner distributed system properties");
    Properties p = ld.getLonerProperties();
    log.info("Looked up loner distributed system properties: "
            + prettyprint(p));
    return p;
  }

  /**
   * Returns a string containing indented properties, one per line.
   */
  private static String prettyprint(Properties p) {
    List l = Collections.list(p.propertyNames());
    SortedSet set = new TreeSet(l);
    StringBuffer buf = new StringBuffer();
    for (Iterator i = set.iterator(); i.hasNext();) {
      String key = (String)i.next();
      String val = p.getProperty(key);
      buf.append("\n  " + key + "=" + val);
    }
    return buf.toString();
  }

//------------------------------------------------------------------------------
// LonerDescription
//------------------------------------------------------------------------------

  /**
   * Returns the {@link LonerDescription} to which this VM is wired
   * using {@link LonerPrms#clientNames}.  Caches the result.
   *
   * @throws HydraRuntimeException if no configuration is wired to this client,
   */
  private static LonerDescription getLonerDescription() {
    String clientName = RemoteTestModule.getMyClientName();
    if (TheLonerDescription == null) {
      log.info("Looking up loner config for " + clientName);
      Map<String,LonerDescription> lds =
              GfxdTestConfig.getInstance().getLonerDescriptions();
      for (LonerDescription ld : lds.values()) {
        if (ld.getClientNames().contains(clientName)) {
          log.info("Looked up loner config for " + clientName + ":\n" + ld);
          TheLonerDescription = ld; // cache it
          break;
        }
      }
      if (TheLonerDescription == null) {
        String s = clientName + " is not wired to any loner description"
                 + " using " + BasePrms.nameForKey(LonerPrms.clientNames)
                 + ". Either add it or use an alternate method that takes a "
                 + BasePrms.nameForKey(LonerPrms.names) + " argument.";
        throw new HydraRuntimeException(s);
      }
    }
    return TheLonerDescription;
  }
}
