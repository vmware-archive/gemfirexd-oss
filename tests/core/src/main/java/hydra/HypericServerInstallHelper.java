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
import java.util.*;

/**
 * Provides support for hyperic server installers running in hydra client VMs.
 */
public class HypericServerInstallHelper {

  private static LogWriter log = Log.getLogWriter();

//------------------------------------------------------------------------------
// HypericServerInstallDescription
//------------------------------------------------------------------------------

  /**
   * Returns the {@link HypericServerInstallDescription} with the given
   * physical host name from {@link HypericServerInstallPrms#names}.
   */
  public static HypericServerInstallDescription
                getHypericServerInstallDescription(String hostName) {
    if (hostName == null) {
      throw new IllegalArgumentException("hostName cannot be null");
    }
    log.info("Looking up hyperic server install config for host: " + hostName);
    Collection hqds = TestConfig.getInstance()
                                .getHypericServerInstallDescriptions().values();
    for (Iterator i = hqds.iterator(); i.hasNext();) {
      HypericServerInstallDescription hqd =
            (HypericServerInstallDescription)i.next();
      if (hqd.hostDescription.getHostName().equals(hostName)) {
        log.info("Looked up hyperic server install config:\n" + hqd);
        return hqd;
      }
    }
    String s = hostName + " not found in "
             + BasePrms.nameForKey(HypericServerInstallPrms.names);
    throw new HydraRuntimeException(s);
  }
}
