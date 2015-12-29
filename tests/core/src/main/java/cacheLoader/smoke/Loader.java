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

import hydra.*;
import hydra.blackboard.*;

/**
 * A <code>CacheLoader</code> that loads a value from the {@linkplain
 * Util#DATA_NAME data} entry in the {@link Util#MASTER_REGION_NAME
 * master} region.
 *
 * @author Marilyn Daum
 *
 * @since 2.0
 */
public class Loader extends Util implements CacheLoader {

    /**
     * Loads current value of MasterData.
     */
    public Object load(LoaderHelper helper) { 

        Log.getLogWriter().info("--Executing CacheLoader");
	Region region = helper.getRegion();

	// increment counters
	SharedCounters counters = BB.getInstance().getSharedCounters();
	counters.increment(BB.NUM_LOAD_CALLS);

	// get value from MasterData
	Integer masterValue;
        Region master =
          region.getParentRegion().getSubregion(MASTER_REGION_NAME);
	try {
          masterValue = (Integer) master.get(DATA_NAME);

	} catch (Exception ex) {
          String s = "Unable to read " + MASTER_REGION_NAME + "/"
            + DATA_NAME;
	    throw new HydraRuntimeException(s , ex);
	}

	if (masterValue == null) {
          String s = MASTER_REGION_NAME + "/" + DATA_NAME +
            " value is null";
          throw new HydraRuntimeException(s);
        }

	if (TestParms.getLogDetails())
	    Log.getLogWriter().info
		("Loading " + Util.log(region, DATA_NAME) + 
		 " from " + Util.log(master, DATA_NAME, masterValue));
	return new Integer(masterValue.intValue());
    }

    public void close() { 
    }

}
