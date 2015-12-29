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
package management.operations;

import java.io.Serializable;

import hydra.Log;

import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.RegionEvent;

//TODO fit into overall operations framework

public class OperationsCacheListener implements CacheListener, Serializable {

  @Override
  public void close() {

    String string = "region.close called";
    //Log.getLogWriter().info(string);
    Log.getLogWriter().finest(string);
  }

  @Override
  public void afterCreate(EntryEvent arg0) {
    String string = "region.afterCreate called with : " + arg0;
    Log.getLogWriter().info(string);
    //Log.getLogWriter().finest(string);
  }

  @Override
  public void afterDestroy(EntryEvent arg0) {
    String string = "region.afterDestroy called with key : " + arg0.getKey();
    //Log.getLogWriter().info(string);
    Log.getLogWriter().finest(string);
  }

  @Override
  public void afterInvalidate(EntryEvent arg0) {
    String string = "region.invalidate called with key : " + arg0.getKey();
    //Log.getLogWriter().info(string);
    Log.getLogWriter().finest(string);
  }

  @Override
  public void afterRegionClear(RegionEvent arg0) {
    String string = "region.clear called with : " + arg0;
    //Log.getLogWriter().info(string);
    Log.getLogWriter().finest(string);
  }

  @Override
  public void afterRegionCreate(RegionEvent arg0) {
    String string = "region.afterRegionCreate called with : " + arg0;
    //Log.getLogWriter().info(string);
    Log.getLogWriter().finest(string);
  }

  @Override
  public void afterRegionDestroy(RegionEvent arg0) {
    String string = "region.afterRegionDestroy called with : " + arg0;
    //Log.getLogWriter().info(string);
    Log.getLogWriter().finest(string);
  }

  @Override
  public void afterRegionInvalidate(RegionEvent arg0) {
    String string = "region.afterRegionInvalidate called with : " + arg0;
    //Log.getLogWriter().info(string);
    Log.getLogWriter().finest(string);
  }

  @Override
  public void afterRegionLive(RegionEvent arg0) {
    String string = "region.afterRegionLive called with : " + arg0;
    //Log.getLogWriter().info(string);
    Log.getLogWriter().finest(string);
  }

  @Override
  public void afterUpdate(EntryEvent arg0) {
    String string = "region.afterRegionUpdate called with : " + arg0;
    //Log.getLogWriter().info(string);
    Log.getLogWriter().finest(string);
  }

}
