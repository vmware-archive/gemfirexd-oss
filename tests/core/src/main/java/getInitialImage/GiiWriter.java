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
package getInitialImage; 

//import util.*;
//import hydra.*;
import com.gemstone.gemfire.cache.*;

public class GiiWriter implements CacheWriter {

public void beforeCreate(EntryEvent event) {
   if (InitImageTest.isLocalGiiInProgress(event.getRegion()))
      InitImageBB.getBB().getSharedCounters().increment(InitImageBB.CACHE_WRITER_INVOKED_DURING_GII);
   else
      InitImageBB.getBB().getSharedCounters().increment(InitImageBB.CACHE_WRITER_INVOKED_NO_GII);
}

public void beforeDestroy(EntryEvent event) {
   if (InitImageTest.isLocalGiiInProgress(event.getRegion()))
      InitImageBB.getBB().getSharedCounters().increment(InitImageBB.CACHE_WRITER_INVOKED_DURING_GII);
   else
      InitImageBB.getBB().getSharedCounters().increment(InitImageBB.CACHE_WRITER_INVOKED_NO_GII);
}

public void beforeRegionDestroy(RegionEvent event) {
   if (InitImageTest.isLocalGiiInProgress(event.getRegion()))
      InitImageBB.getBB().getSharedCounters().increment(InitImageBB.CACHE_WRITER_INVOKED_DURING_GII);
   else
      InitImageBB.getBB().getSharedCounters().increment(InitImageBB.CACHE_WRITER_INVOKED_NO_GII);
}

public void beforeRegionClear(RegionEvent event) {
   if (InitImageTest.isLocalGiiInProgress(event.getRegion()))
      InitImageBB.getBB().getSharedCounters().increment(InitImageBB.CACHE_WRITER_INVOKED_DURING_GII);
   else
      InitImageBB.getBB().getSharedCounters().increment(InitImageBB.CACHE_WRITER_INVOKED_NO_GII);
}

public void beforeUpdate(EntryEvent event) {
   if (InitImageTest.isLocalGiiInProgress(event.getRegion()))
      InitImageBB.getBB().getSharedCounters().increment(InitImageBB.CACHE_WRITER_INVOKED_DURING_GII);
   else
      InitImageBB.getBB().getSharedCounters().increment(InitImageBB.CACHE_WRITER_INVOKED_NO_GII);
}
 
public void close() {
   if (InitImageTest.isLocalGiiInProgress())
      InitImageBB.getBB().getSharedCounters().increment(InitImageBB.CACHE_WRITER_INVOKED_DURING_GII);
   else
      InitImageBB.getBB().getSharedCounters().increment(InitImageBB.CACHE_WRITER_INVOKED_NO_GII);
}

}
