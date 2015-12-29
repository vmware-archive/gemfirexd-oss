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

import util.*;
import hydra.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.internal.cache.*;
import com.gemstone.gemfire.distributed.*;
import java.util.*;

/**
 * A logging TxWriter for handling transaction related events (beforeCommit).  
 *
 * @author lhughes
 * @see TransactionWriter
 */
public class TxLogWriter extends util.AbstractWriter implements TransactionWriter {

/** Called after successful conflict checking (in GemFire) and prior to commit
 *
 * @param event the TransactionEvent
 */
public void beforeCommit(TransactionEvent event) throws TransactionWriterException {
  logTxEvent("beforeCommit", event);
}

/**  Called when the region containing this callback is destroyed, when the 
 *   cache is closed, or when a callback is removed from a region using an
 *   <code>AttributesMutator</code>.
 */
public void close() {
  Log.getLogWriter().info("TxLogWriter: close()");
}

}

