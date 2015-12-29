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
package scale64.parReg.bridge;

import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.RegionEvent;
import hydra.Log;
import java.util.Properties;
import util.BaseValueHolder;

public class GutterListener implements CacheListener, Declarable {

  public void afterCreate(EntryEvent event) {
    gutEventValue(event);
  }

  public void afterDestroy(EntryEvent event) {
  }

  public void afterInvalidate(EntryEvent event) {
  }

  public void afterRegionDestroy(RegionEvent event) {
  }

  public void afterRegionInvalidate(RegionEvent event) {
  }

  public void afterUpdate(EntryEvent event) {
    gutEventValue(event);
  }

  public void close() {
  }

  public void afterRegionClear(RegionEvent event) {
  }

  public void afterRegionCreate(RegionEvent event) {
  }

  public void afterRegionLive(RegionEvent event) {
  }

  public void init(Properties prop) {
  }

  /**
   * Guts the extra object to avoid excess heap use by the client (though
   * it will generate lots of garbage).  The remaining object parts are
   * needed to validate later.
   */
  private void gutEventValue(EntryEvent event) {
    BaseValueHolder val = (BaseValueHolder)event.getNewValue();
    val.extraObject = null;
    Log.getLogWriter().info("gutted " + val);
  }
}
