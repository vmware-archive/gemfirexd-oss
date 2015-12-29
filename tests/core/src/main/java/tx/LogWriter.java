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
import diskReg.DiskRegUtil;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.internal.cache.Token;
import com.gemstone.gemfire.distributed.*;

/** Logging CacheWriter (for TxView and DistIntegrity tests)
 *
 * @author Lynn Hughes-Godfrey
 * @since 6.0
 */
public class LogWriter extends util.AbstractWriter implements CacheWriter, Declarable {

//==============================================================================
// implementation of CacheWriter methods
public void beforeCreate(EntryEvent event) {
   logCall("beforeCreate", event);
}

public void beforeDestroy(EntryEvent event) {
   logCall("beforeDestroy", event);
}

public void beforeUpdate(EntryEvent event) {
   logCall("beforeUpdate", event);
}

public void beforeRegionDestroy(RegionEvent event) {
   logCall("beforeRegionDestroy", event);
}

public void beforeRegionClear(RegionEvent event) {
   logCall("beforeRegionClear", event);
}

public void close() {
   logCall("close", null);
}

public void init(java.util.Properties prop) {
   logCall("init(Properties)", null);
}

}
