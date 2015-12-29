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
package diskRecovery;

import com.gemstone.gemfire.cache.CacheWriter;
import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.RegionEvent;

import java.util.Properties;

import util.TestException;

/** CacheWriter that can be configured to throw an exception if the disk
 *  converter tool tries to instantiate it. 
 * @author lynn
 *
 */
public class ConverterWriter extends util.AbstractWriter implements CacheWriter, Declarable {
  
  /** Constructor. This allows or disallows instantiation of this class
   *  depending on the value of a boolean serialized in a well-known file.
   *  If the file does not exist, allow instantiation. If the file does
   *  exist, allow instantiation if the Boolean in the file is true, disallow
   *  instantiation by throwing an exception if the boolean in the file is
   *  false.
   * 
   */
  public ConverterWriter() {
    super();
    if (!InstantiationHelper.allowInstantiation()) {
       throw new TestException(this.getClass().getName() + " should not be instantiated");
    }
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.CacheCallback#close()
   */
  public void close() {
    logCall("close", null);
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.CacheWriter#beforeUpdate(com.gemstone.gemfire.cache.EntryEvent)
   */
  public void beforeUpdate(EntryEvent event) throws CacheWriterException {
    logCall("beforeUpdate", event);
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.CacheWriter#beforeCreate(com.gemstone.gemfire.cache.EntryEvent)
   */
  public void beforeCreate(EntryEvent event) throws CacheWriterException {
    logCall("beforeCreate", null);
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.CacheWriter#beforeDestroy(com.gemstone.gemfire.cache.EntryEvent)
   */
  public void beforeDestroy(EntryEvent event) throws CacheWriterException {
    logCall("beforeDestroy", event);
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.CacheWriter#beforeRegionDestroy(com.gemstone.gemfire.cache.RegionEvent)
   */
  public void beforeRegionDestroy(RegionEvent event)
      throws CacheWriterException {
    logCall("beforeRegionDestroy", event);
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.CacheWriter#beforeRegionClear(com.gemstone.gemfire.cache.RegionEvent)
   */
  public void beforeRegionClear(RegionEvent event) throws CacheWriterException {
    logCall("beforeRegionClear", event);
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.Declarable#init(java.util.Properties)
   */
  public void init(Properties props) {
    
  }

}
