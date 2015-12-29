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
package com.gemstone.gemfire.internal.cache.wan;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.RegionEvent;

public class QueueListener implements CacheListener{
  public List createList = Collections.synchronizedList(new ArrayList());
  public List destroyList = Collections.synchronizedList(new ArrayList());
  public List updateList = Collections.synchronizedList(new ArrayList());
  
  public void afterCreate(EntryEvent event) {
    createList.add(event.getKey());
  }

  public void afterDestroy(EntryEvent event) {
    destroyList.add(event.getKey());
  }

  public void afterInvalidate(EntryEvent event) {
    // TODO Auto-generated method stub
    
  }

  public void afterRegionClear(RegionEvent event) {
    // TODO Auto-generated method stub
    
  }

  public void afterRegionCreate(RegionEvent event) {
    // TODO Auto-generated method stub
    
  }

  public void afterRegionDestroy(RegionEvent event) {
    // TODO Auto-generated method stub
    
  }

  public void afterRegionInvalidate(RegionEvent event) {
    // TODO Auto-generated method stub
    
  }

  public void afterRegionLive(RegionEvent event) {
    // TODO Auto-generated method stub
    
  }

  public void afterUpdate(EntryEvent event) {
    updateList.add(event.getKey());
  }

  public void close() {
    // TODO Auto-generated method stub
    
  }
  
}