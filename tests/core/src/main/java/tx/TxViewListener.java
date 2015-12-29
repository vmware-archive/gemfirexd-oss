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
package tx; 

//import util.*;
import hydra.Log;
import com.gemstone.gemfire.cache.*;
//import java.util.*;

public class TxViewListener extends util.AbstractListener implements TransactionListener {

//==============================================================================
// implementation TransactionListener methods
// note that RegionEvents are not transactional ... they occur immediately
// on commit state.
//==============================================================================

public void afterCommit(TransactionEvent event) {
  logTxEvent("afterCommit", event);
  tx.TxBB.getBB().getSharedMap().put(tx.TxBB.afterCommitProcessed, new Boolean(true));
}

public void afterRollback(TransactionEvent event) {
  logTxEvent("afterRollback", event);
}

public void afterFailedCommit(TransactionEvent event) {
  logTxEvent("afterFailedCommit", event);
}

public void close() {
  Log.getLogWriter().info("TxViewListener: close()");
}

}

