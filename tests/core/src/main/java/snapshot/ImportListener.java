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

/** Import Test Listener. 
 *  CacheListener that will tolerate the class of the old/new values in the
 *  event not being on the class path.
 *
 *  Increments a SnapshotBB counter (eventsDuringImport) for each event processed.  
 *  Test installs prior import (during re-initialization) and deinstalls once import completed.  
 *  Test must also check SnapshotBB counter.
 *
 * @see SnapshotBB#eventsDuringImport
 *
 * @author Lynn Hughes-Godfrey
 * @since 7.0
 */
public class ImportListener extends util.AbstractListener implements CacheListener, Declarable {

//==============================================================================
// implementation CacheListener methods
public void afterCreate(EntryEvent event) {
   logCall("afterCreate", event);
   SnapshotBB.getBB().getSharedCounters().increment(SnapshotBB.eventsDuringImport);
}

public void afterDestroy(EntryEvent event) {
   logCall("afterDestroy", event);
   SnapshotBB.getBB().getSharedCounters().increment(SnapshotBB.eventsDuringImport);
}

public void afterInvalidate(EntryEvent event) {
   logCall("afterDestroy", event);
   SnapshotBB.getBB().getSharedCounters().increment(SnapshotBB.eventsDuringImport);
}

public void afterUpdate(EntryEvent event) {
   logCall("afterUpdate", event);
   SnapshotBB.getBB().getSharedCounters().increment(SnapshotBB.eventsDuringImport);
}

// not used
public void afterRegionDestroy(RegionEvent event) {
}
public void afterRegionInvalidate(RegionEvent event) {
}
public void afterRegionClear(RegionEvent event) {
}
public void afterRegionCreate(RegionEvent event) {
}
public void afterRegionLive(RegionEvent event) {
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
