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
import hydra.BasePrms;
import hydra.HydraRuntimeException;
import hydra.Log;
import hydra.RemoteTestModule;
import java.util.*;

/**
 * Helps hydra clients manage thin clients.  Methods are thread-safe.
 */
public class ThinClientHelper {

  /** Description used by the client */
  protected static ThinClientDescription TheThinClientDescription;

  // @todo lises replace with thin client usable log writer based on gemfire
  //             and transparently accessible to thin clients through
  //             Log.getLogWriter(), needed for all logging by hydra/test code
  private static LogWriter log = Log.getLogWriter();

//------------------------------------------------------------------------------
// Properties
//------------------------------------------------------------------------------

  /**
   * Returns the connection properties from the {@link ThinClientDescription}
   * to which this VM is wired using {@link ThinClientPrms#clientNames}.
   */
  public static Properties getConnectionProperties() {
    ThinClientDescription tcd = getThinClientDescription();
    return getConnectionProperties(tcd);
  }

  /**
   * Returns connection properties using the description.
   */
  private static synchronized Properties getConnectionProperties(
                                         ThinClientDescription tcd) {
    log.info("Looking up thin client connection properties");
    Properties p = tcd.getConnectionProperties();
    log.info("Looked up thin client connection properties: " + prettyprint(p));
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
// ThinClientDescription
//------------------------------------------------------------------------------

  /**
   * Returns the {@link ThinClientDescription} to which this VM is wired
   * using {@link ThinClientPrms#clientNames}.  Caches the result.
   *
   * @throws HydraRuntimeException if no configuration is wired to this client,
   */
  private static synchronized ThinClientDescription getThinClientDescription() {
    String clientName = RemoteTestModule.getMyClientName();
    if (TheThinClientDescription == null) {
      log.info("Looking up thin client config for " + clientName);
      Map<String,ThinClientDescription> tcds =
              GfxdTestConfig.getInstance().getThinClientDescriptions();
      for (ThinClientDescription tcd : tcds.values()) {
        if (tcd.getClientNames().contains(clientName)) {
          log.info("Looked up thin client config for " + clientName + ":\n"
                  + tcd);
          TheThinClientDescription = tcd; // cache it
          break;
        }
      }
      if (TheThinClientDescription == null) {
        String s = clientName + " is not wired to any thin client description"
                 + " using " + BasePrms.nameForKey(ThinClientPrms.clientNames)
                 + ".";
        throw new HydraRuntimeException(s);
      }
    }
    return TheThinClientDescription;
  }
}
