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
package cacheRunner;

//import com.gemstone.gemfire.internal.LocalLogWriter;
import com.gemstone.gemfire.cache.*;

/**
 * A <code>CacheWriter</code> 
 *
 * @author GemStone Systems, Inc.
 * @since 4.0
 */
public class LoggingCacheWriter extends LoggingCacheCallback
  implements CacheWriter<Object, Object> {

  /**
   * Zero-argument constructor required for declarative caching
   */
  public LoggingCacheWriter() {
    super();
  }

  public final void beforeUpdate(EntryEvent<Object, Object> event)
    throws CacheWriterException {

    log("CacheWriter.beforeUpdate", event);
  }

  public final void beforeCreate(EntryEvent<Object, Object> event)
    throws CacheWriterException {

    log( "CacheWriter.beforeCreate", event);
  }

  public final void beforeDestroy(EntryEvent<Object, Object> event)
    throws CacheWriterException {

    log("CacheWriter.beforeDestroy", event);
  }

  public final void beforeRegionDestroy(RegionEvent<Object, Object> event)
    throws CacheWriterException {

    log("CacheWriter.beforeRegionDestroy", event);
  }

  public final void beforeRegionClear(RegionEvent<Object, Object> event)
    throws CacheWriterException {

    log("CacheWriter.beforeRegionClear", event);
  }

}
