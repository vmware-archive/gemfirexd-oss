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

package cacheperf.gemfire;

import com.gemstone.gemfire.cache.*;
import cacheperf.*;
import hydra.*;
import objects.*;

public class SleepLoader implements CacheLoader {

  public SleepLoader() {
  }

  /**
   *  Loads an object at the given key.  Sleeps to simulate going to an RDB.
   */
  public Object load( LoaderHelper helper ) { 
    String key = (String) helper.getKey();
    int index;
    try {
      index = ( new Integer( key ) ).intValue();
    } catch( NumberFormatException e ) {
      throw new CachePerfException( key + " is not a stringified integer", e );
    }
    String classname = CachePerfPrms.getObjectType();
    Object obj = ObjectHelper.createObject( classname, index );
    int sleep = GemFireCachePrms.getLoaderSleepMs();
    if ( sleep > 0 ) {
      MasterController.sleepForMs( sleep );
    }
    return obj;
  }
  public void close() {
  }
}
