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

import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.RegionEvent;

import java.util.Properties;

import util.AbstractListener;
import util.TestException;

/**
 * @author lynn
 *
 */
public class ConverterListener extends AbstractListener implements CacheListener, Declarable {

  /** Constructor. This allows or disallows instantiation of this class
   *  depending on the value of a boolean serialized in a well-known file.
   *  If the file does not exist, allow instantiation. If the file does
   *  exist, allow instantiation if the Boolean in the file is true, disallow
   *  instantiation by throwing an exception if the boolean in the file is
   *  false.
   * 
   */
  public ConverterListener() {
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
   * @see com.gemstone.gemfire.cache.CacheListener#afterCreate(com.gemstone.gemfire.cache.EntryEvent)
   */
  public void afterCreate(EntryEvent event) {
    logCall("afterCreate", event);
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.CacheListener#afterUpdate(com.gemstone.gemfire.cache.EntryEvent)
   */
  public void afterUpdate(EntryEvent event) {
    logCall("afterUpdate", event);
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.CacheListener#afterInvalidate(com.gemstone.gemfire.cache.EntryEvent)
   */
  public void afterInvalidate(EntryEvent event) {
    logCall("afterInvalidate", event);
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.CacheListener#afterDestroy(com.gemstone.gemfire.cache.EntryEvent)
   */
  public void afterDestroy(EntryEvent event) {
    logCall("afterDestroy", event);
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.CacheListener#afterRegionInvalidate(com.gemstone.gemfire.cache.RegionEvent)
   */
  public void afterRegionInvalidate(RegionEvent event) {
    logCall("afterRegionInvalidate", event);
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.CacheListener#afterRegionDestroy(com.gemstone.gemfire.cache.RegionEvent)
   */
  public void afterRegionDestroy(RegionEvent event) {
    logCall("afterRegionDestroy", event);
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.CacheListener#afterRegionClear(com.gemstone.gemfire.cache.RegionEvent)
   */
  public void afterRegionClear(RegionEvent event) {
    logCall("afterRegionClear", event);
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.CacheListener#afterRegionCreate(com.gemstone.gemfire.cache.RegionEvent)
   */
  public void afterRegionCreate(RegionEvent event) {
    logCall("afterRegionCreate", event);
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.CacheListener#afterRegionLive(com.gemstone.gemfire.cache.RegionEvent)
   */
  public void afterRegionLive(RegionEvent event) {
    logCall("afterRegionLive", event);
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.Declarable#init(java.util.Properties)
   */
  public void init(Properties props) {
    
  }

}
