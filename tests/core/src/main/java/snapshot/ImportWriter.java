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
package snapshot; 

import pdx.PdxTest;
import util.*;
import hydra.*;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.pdx.PdxInstance;
import com.gemstone.gemfire.SerializationException;

import java.io.IOException;

/** Import Test Writer. 
 *  CacheWriter that will tolerate the class of the old/new values in the
 *  event not being on the class path.
 *  Increments a SnapshotBB counter (eventsDuringImport) for each event processed.  
 *  The test installs this listener during re-initialization (prior to import) and 
 *  deinstalls it once import has completed.
 *
 * @see SnapshotBB#eventsDuringImport
 * @see SnapshotBB#importInProgress
 *
 * @author Lynn Hughes-Godfrey
 * @since 7.0
 */
public class ImportWriter extends util.AbstractWriter implements CacheWriter, Declarable {

public void beforeCreate(EntryEvent event) {
   logCall("beforeCreate", event);
   SnapshotBB.getBB().getSharedCounters().increment(SnapshotBB.eventsDuringImport);
}

public void beforeDestroy(EntryEvent event) {
   logCall("beforeDestroy", event);
   SnapshotBB.getBB().getSharedCounters().increment(SnapshotBB.eventsDuringImport);
}

public void beforeUpdate(EntryEvent event) {
   logCall("beforeUpdate", event);
   SnapshotBB.getBB().getSharedCounters().increment(SnapshotBB.eventsDuringImport);
}

// not used
public void beforeRegionDestroy(RegionEvent event) {
}
public void beforeRegionInvalidate(RegionEvent event) {
}
public void beforeRegionClear(RegionEvent event) {
}
public void beforeRegionCreate(RegionEvent event) {
}
public void beforeRegionLive(RegionEvent event) {
}
public void close() {
}
public void init(java.util.Properties prop) {
}  

/* (non-Javadoc)
 * @see util.AbstractListener#getOldValueStr(com.gemstone.gemfire.cache.EntryEvent)
 */
@Override
public String getOldValueStr(EntryEvent eEvent) {
  return PdxTest.getOldValueStr(eEvent);
}

/* (non-Javadoc)
 * @see util.AbstractListener#getNewValueStr(com.gemstone.gemfire.cache.EntryEvent)
 */
@Override
public String getNewValueStr(EntryEvent eEvent) {
  return PdxTest.getNewValueStr(eEvent);
}
}
