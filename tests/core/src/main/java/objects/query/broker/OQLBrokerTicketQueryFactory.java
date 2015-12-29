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
package objects.query.broker;

import com.gemstone.gemfire.cache.Region;

import hydra.Log;
import hydra.RegionHelper;

import java.util.ArrayList;
import java.util.List;

import objects.query.BaseQueryFactory;
import objects.query.QueryObjectException;

public class OQLBrokerTicketQueryFactory extends BaseQueryFactory {

  private static Region BrokerTicketRegion;

  private static synchronized void setRegion(Region r) {
    BrokerTicketRegion = r;
  }

  //--------------------------------------------------------------------------
  // QueryFactory : regions
  //--------------------------------------------------------------------------

  public void createRegions() {
    setRegion(RegionHelper.createRegion(BrokerTicket.getTableName(),
                           BrokerPrms.getBrokerTicketRegionConfig()));
  }

  //--------------------------------------------------------------------------
  // QueryFactory : inserts
  //--------------------------------------------------------------------------

  /**
   * Generate the list of insert objects required to create {@link
   * BrokerPrms#numTicketsPerBroker} tickets for the broker with the given
   * broker id.
   *
   * @param bid the unique broker id
   */
  public List getInsertObjects(int bid) {
    int numTicketsPerBroker = BrokerPrms.getNumTicketsPerBroker();
    int numTicketPrices = BrokerPrms.getNumTicketPrices();
    List objs = new ArrayList();
    for (int i = 0; i < numTicketsPerBroker; i++) {
      BrokerTicket obj = new BrokerTicket();
      obj.init(i, bid, numTicketsPerBroker, numTicketPrices);
      objs.add(obj);
    }
    return objs;
  }

  public List getPreparedInsertObjects() {
    List pobjs = new ArrayList();
    Object pobj = new BrokerTicket();
    pobjs.add(pobj);
    return pobjs;
  }

  // @todo Logging (either move it or implement it here)
  /**
   * @throw QueryObjectException if bid has already been inserted.
   */
  public void fillAndExecutePreparedInsertObjects(List pobjs, int bid)
  throws QueryObjectException {
    int numTicketsPerBroker = BrokerPrms.getNumTicketsPerBroker();
    int numTicketPrices = BrokerPrms.getNumTicketPrices();
    BrokerTicket pobj = (BrokerTicket)pobjs.get(0);
    for (int i = 0; i < numTicketsPerBroker; i++) {
      pobj.init(i, bid, numTicketsPerBroker, numTicketPrices);
      if (logUpdates) {
        Log.getLogWriter().info("Executing update: " + pobj);
      }
      // @todo use ObjectHelper to generate configurable key types
      BrokerTicketRegion.put(String.valueOf(pobj.getId()), pobj);
      if (logUpdates) {
        Log.getLogWriter().info("Executed update: " + pobj);
      }
    }
  }
}
