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
import hydra.HydraRuntimeException;
import hydra.Log;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Provides support for gateway receivers running in hydra-managed client VMs.
 */
public class GatewayReceiverHelper {

  private static LogWriter log = Log.getLogWriter();

//------------------------------------------------------------------------------
// GatewayReceiver
//------------------------------------------------------------------------------

  /**
   * Generates the DDL for creating gateway receivers with the given distributed
   * system ID, from the {@link GatewayReceiverDescription#distributedSystem}.
   * This method can be invoked from anywhere, including servers, peer clients,
   * and thin clients.
   * <p>
   * Each matching gateway receiver description generates a DDL statement.
   * The receiver is started upon execution of the DDL.
   * <p>
   * It is up to the test to execute the DDL exactly once.
   *
   * @return the (possibly empty) list of DDL statements
   */
  public static synchronized List<String> getGatewayReceiverDDL(int dsid) {
    log.info("Generating gateway receiver DDL for DSID=" + dsid);
    List<String> ddls = new ArrayList();
    Collection<GatewayReceiverDescription> grds =
        GfxdTestConfig.getInstance().getGatewayReceiverDescriptions().values();
    for (GatewayReceiverDescription grd : grds) {
      if (grd.getDistributedSystemId() == dsid) {
        if (log.fineEnabled()) {
          log.fine("Generating gateway receiver DDL for " + grd);
        }
        String ddl = grd.getDDL();
        log.info("Generated DDL: " + ddl);
        ddls.add(ddl);
      }
    }
    log.info("Generated gateway receiver DDL for DSID=" + dsid + ": " + ddls);
    return ddls;
  }

  /**
   * Generates the DDL for creating the gateway receiver with the given ID and
   * distributed system ID, derived from the {@link GatewayReceiverDescription}.
   * This method can be invoked from anywhere, including servers, peer clients,
   * and thin clients.
   * <p>
   * The matching gateway receiver description generates a DDL statement.
   * The receiver is started upon execution of the DDL.
   * <p>
   * It is up to the test to execute the DDL exactly once.
   *
   * @return the DDL string
   */
  public static synchronized String getGatewayReceiverDDL(String id, int dsid) {
    log.info("Generating gateway receiver DDL for ID=" + id + " DSID=" + dsid);
    Collection<GatewayReceiverDescription> grds =
        GfxdTestConfig.getInstance().getGatewayReceiverDescriptions().values();
    for (GatewayReceiverDescription grd : grds) {
      if (grd.getId().equals(id) && grd.getDistributedSystemId() == dsid) {
        if (log.fineEnabled()) {
          log.fine("Generating gateway receiver DDL for " + grd);
        }
        String ddl = grd.getDDL();
        log.info("Generated gateway receiver DDL for ID=" + id
                + " DSID=" + dsid + ": " + ddl);
      }
    }
    String s = "No GatewayReceiverDescription found with id=" + id
             + " dsid=" + dsid;
    throw new HydraRuntimeException(s);
  }
}
