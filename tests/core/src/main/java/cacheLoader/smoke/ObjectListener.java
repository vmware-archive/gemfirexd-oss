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
package cacheLoader.smoke;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.util.*;

import hydra.*;
import hydra.blackboard.*;

public class ObjectListener extends CacheListenerAdapter  implements  CacheWriter {


    /**
     * Called when an object is about to be created in cache.
     */
    public void beforeCreate(EntryEvent objEvent) throws CacheWriterException {
	Region region = objEvent.getRegion();
	Object objName = objEvent.getKey();
	Log.getLogWriter().info
	    ("About to create " + Util.log(region, objName, 
					   objEvent.getOldValue()));
    }

    /**
     * Called when an object is newly loaded into cache.
     */
    public void afterCreate(EntryEvent objEvent) {
	Region region = objEvent.getRegion();
	Object objName = objEvent.getKey();
	Log.getLogWriter().info
	    ("Creating " + Util.log(region, objName, objEvent.getNewValue()));
    }


    /**
     * Called when an object is about to be destroyed.
     */
    public void beforeDestroy(EntryEvent objEvent)
      throws CacheWriterException {
	Region region = objEvent.getRegion();
	Object objName = objEvent.getKey();
	Log.getLogWriter().info
	    ("About to destroy " + Util.log(region, objName, 
					    objEvent.getOldValue()));
    }


    /**
     * Called when an object is destroyed.
     */
    public void afterDestroy(EntryEvent objEvent) {
	Region region = objEvent.getRegion();
	Object objName = objEvent.getKey();
	Log.getLogWriter().info
	    ("Destroying " + Util.log(region, objName, 
				      objEvent.getOldValue()));

	// increment counters
	SharedCounters counters = BB.getInstance().getSharedCounters();
	counters.increment(BB.NUM_DESTROY_CALLBACKS);
    }


    /**
     * Called when an object is invalidated.
     */
    public void afterInvalidate(EntryEvent objEvent) {
	Region region = objEvent.getRegion();
	Object objName = objEvent.getKey();
	Log.getLogWriter().info
	    ("Invalidating " + Util.log(region, objName, 
					objEvent.getOldValue()));

	// increment counters
	SharedCounters counters = BB.getInstance().getSharedCounters();
        if (!objEvent.isDistributed()) {
          counters.increment(BB.NUM_UNLOAD_CALLBACKS);
        } else {
          counters.increment(BB.NUM_INVALIDATE_CALLBACKS);
        }
    }


    /**
     * Called when an object is about to be replaced.
     */
    public void beforeUpdate(EntryEvent objEvent)
      throws CacheWriterException {
	Region region = objEvent.getRegion();
	Object objName = objEvent.getKey();
	Log.getLogWriter().info
	    ("About to update " + Util.log(region, objName, 
					    objEvent.getOldValue()));
    }


    /**
     * Called when an object is replaced.
     */
    public void afterUpdate(EntryEvent objEvent) {
	Region region = objEvent.getRegion();
	Object objName = objEvent.getKey();
	Log.getLogWriter().info
	    ("Updating - original object = " + 
	     Util.log(region, objName, objEvent.getOldValue()) +
	     " current object = " + 
	     Util.log(region, objName, objEvent.getNewValue()));
    }

    public void afterRegionInvalidate(RegionEvent objEvent) { 
    }

    public void beforeRegionDestroy(RegionEvent objEvent) throws CacheWriterException { 
    }

    public void beforeRegionClear(RegionEvent objEvent) throws CacheWriterException { 
    }

    public void afterRegionDestroy(RegionEvent objEvent) { 
    }

    /**
     *  Called when region is destroyed, cache is closed, or callback
     *  is removed.  
     */
    public void close() { 
    }

}
