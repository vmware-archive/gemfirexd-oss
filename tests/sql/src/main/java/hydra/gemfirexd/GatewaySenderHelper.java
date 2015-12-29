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
import hydra.Log;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Provides support for gateway senders running in hydra-managed client VMs.
 */
public class GatewaySenderHelper {

  private static LogWriter log = Log.getLogWriter();

//------------------------------------------------------------------------------
// GatewaySender
//------------------------------------------------------------------------------

  /**
   * Generates the DDL for creating gateway senders with the given distributed
   * system ID, as derived from {@link GatewaySenderDescription
   * #distributedSystem}.
   * <p>
   * Each matching gateway sender description generates a DDL statement for each
   * system specified in {@link GatewaySenderPrms#remoteDistributedSystems}.
   * The ID for each sender is formed from its {@link GatewaySenderPrms#id}
   * plus the local and remote distributed system names.  For example, a sender
   * named "orders" and sending from distributed system "1" to system "3" would
   * generate the ID "orders_1_to_3".
   * <p>
   * If {@link GatewaySenderPrms#manualStart} is false (default) for the
   * specified configuration, each sender is started upon execution of the DDL.
   * Otherwise the senders start upon execution of the appropriate system
   * procedure.
   * <p>
   * This method can be invoked from anywhere, including servers, peer clients,
   * and thin clients.  The generated DDL must be executed by a member of the
   * specified distributed system or by a thin client that is connected to a
   * member of the specified distributed system.
   *
   * @return the (possibly empty) list of DDL statements
   */
  public static synchronized List<String> getGatewaySenderDDL(int dsid) {
    log.info("Generating gateway sender DDL for DSID=" + dsid);
    List<String> ddls = new ArrayList();
    Collection<GatewaySenderDescription> gsds =
        GfxdTestConfig.getInstance().getGatewaySenderDescriptions().values();
    for (GatewaySenderDescription gsd : gsds) {
      if (gsd.getDistributedSystemId() == dsid) {
        log.info("Generating gateway sender DDL for " + gsd);
        List<String> ddl = gsd.getDDL();
        log.info("Generated DDL: " + ddl);
        ddls.addAll(ddl);
      }
    }
    log.info("Generated gateway sender DDL for DSID=" + dsid + ": " + ddls);
    return ddls;
  }

  /**
   * Generates the gateway sender clause to add to table DDL for the specified
   * sender ID as determined by {@link GatewaySenderDescription#id} and
   * distributed system ID, as derived from the {@link GatewaySenderDescription
   * #distributedSystem}.
   * <p>
   * This method can be invoked from anywhere, including servers, peer clients,
   * and thin clients.  The DDL using these clauses must be executed by a member
   * of the specified distributed system or by a thin client that is connected
   * to a member of the specified distributed system.
   *
   * @return the (possibly empty) gateway sender clause
   */
  public static synchronized String getGatewaySenderClause(String senderID,
                                                           int dsid) {
    log.info("Generating gateway sender clause for ID=" + senderID
            + " DSID=" + dsid);
    String clause = "";
    SortedSet<String> ids = new TreeSet();
    Collection<GatewaySenderDescription> gsds =
        GfxdTestConfig.getInstance().getGatewaySenderDescriptions().values();
    for (GatewaySenderDescription gsd : gsds) {
      if (gsd.getId().equals(senderID) && gsd.getDistributedSystemId() == dsid) {
        ids.addAll(gsd.getIDs());
      }
    }
    for (String id : ids) {
      if (clause.length() > 0) clause += ", ";
      clause += id;
    }
    if (clause.length() > 0) {
      clause = " gatewaysender ( " + clause + " )";
    }
    log.info("Generated gateway sender clause for "
            + "ID=" + senderID + " DSID=" + dsid + ": " + clause);
    return clause;
  }

  /**
   * Generates the starting gateway sender IDs for the given distributed system
   * ID, as derived from {@link GatewaySenderDescription#distributedSystem}.
   * <p>
   * Each matching gateway sender description generates an ID for each
   * system specified in {@link GatewaySenderPrms#remoteDistributedSystems}.
   * The ID for each sender is formed from its {@link GatewaySenderPrms#id}
   * plus the local and remote distributed system names.  For example, a sender
   * named "orders" and sending from distributed system "1" to system "3" would
   * generate the ID "orders_1_to_3".
   * <p>
   * This method can be invoked from anywhere, including servers, peer clients,
   * and thin clients.  Statements and procedure calls using the generated IDs
   * must be executed by a member of the specified distributed system or by a
   * thin client that is connected to a member of the specified distributed
   * system.
   *
   * @return the (possibly empty) list of gateway sender IDs
   */
  public static synchronized List<String> getGatewaySenderIds(int dsid) {
    log.info("Generating gateway sender IDs for DSID=" + dsid);
    List<String> ids = new ArrayList();
    Collection<GatewaySenderDescription> gsds =
        GfxdTestConfig.getInstance().getGatewaySenderDescriptions().values();
    for (GatewaySenderDescription gsd : gsds) {
      if (gsd.getDistributedSystemId() == dsid) {
        if (log.fineEnabled()) {
          log.fine("Generating gateway sender IDs for " + gsd);
        }
        List<String> id = gsd.getIDs();
        log.info("Generated IDs: " + id);
        ids.addAll(id);
      }
    }
    log.info("Generated gateway sender IDs for DSID=" + dsid + ": " + ids);
    return ids;
  }
}
