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
package parReg.tx; 

import util.*;
import hydra.*;
import com.gemstone.gemfire.cache.*;
import java.util.*;

public class TxRegionListener extends util.AbstractListener implements TransactionListener {

//==============================================================================
// implementation TransactionListener methods
// note that RegionEvents are not transactional ... they occur immediately
// on commit state.
//==============================================================================

public void afterCommit(TransactionEvent event) {
  logTxEvent("afterCommit", event);
  verifyRegionsExist(event);
}

public void afterRollback(TransactionEvent event) {
  logTxEvent("afterRollback", event);
  verifyRegionsExist(event);
}

public void afterFailedCommit(TransactionEvent event) {
  logTxEvent("afterFailedCommit", event);
  verifyRegionsExist(event);
}

public void close() {
  Log.getLogWriter().info("TxRegionListener: close()");
}

protected void verifyRegionsExist(TransactionEvent txEvent) {
  Set existingRegions = CacheHelper.getCache().rootRegions();
  ArrayList regionNames = new ArrayList();
  for (Iterator it = existingRegions.iterator(); it.hasNext(); ) {
    Region aRegion = (Region)it.next();
    regionNames.add(aRegion.getFullPath());
  }
  Log.getLogWriter().info("List of regions defined in this VM = " + regionNames);

  List events = txEvent.getEvents();
  for (Iterator it = events.iterator(); it.hasNext(); ) {
     CacheEvent e = (CacheEvent)it.next();
     String regionName = e.getRegion().getFullPath();
     if (!regionNames.contains(regionName)) {
       String s = "TxEvent contains CacheEvent for a region not defined in this VM.  Offending event = " + e.toString() + " with region " + regionName + ".  List of defined regions = " + regionNames;
       throwException(s);
     }
  }
}

/** Utility method to log and store an error string into the EventBB and
 *  throw a TestException containing the given string.
 *
 *  @param errStr string to log, store in EventBB and include in TestException
 *  @throws TestException containing the given string
 *
 *  @see TestHelper.checkForEventError
 */
protected void throwException(String errStr) {
      hydra.blackboard.SharedMap aMap = PrTxBB.getBB().getSharedMap();
      aMap.put(TestHelper.EVENT_ERROR_KEY, errStr + " " + TestHelper.getStackTrace());  
      Log.getLogWriter().info(errStr);
      throw new TestException(errStr);
}

}

