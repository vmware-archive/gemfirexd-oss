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
package com.pivotal.gemfirexd.internal.tools.dataextractor.report.views;

import java.util.List;

/*****
 * Region view based on persistent view
 * Its made of List<RegionViewInfoPerGroup> 
 * @author bansods
 *
 */
public class RegionView {
  
  List<RegionViewInfoPerGroup> regionViewInfoPerGroupList;

  boolean viewConflict = false;
  
  public boolean isViewConflicted() {
    return viewConflict;
  }

  public RegionView (List<RegionViewInfoPerGroup> regionViewInfoPerGroupList){
    this.regionViewInfoPerGroupList = regionViewInfoPerGroupList;
    if (regionViewInfoPerGroupList.size() > 1) {
      viewConflict = true;
    }
  }
  
  public List<RegionViewInfoPerGroup> getRegionViewInfoPerGroupList() {
    return regionViewInfoPerGroupList;
  }
  
}
