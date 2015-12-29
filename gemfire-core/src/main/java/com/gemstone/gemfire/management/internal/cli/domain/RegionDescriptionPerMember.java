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
package com.gemstone.gemfire.management.internal.cli.domain;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Scope;

public class RegionDescriptionPerMember implements Serializable {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  private int size = 0;
  private RegionAttributesInfo regionAttributesInfo;
  private String hostingMember;
  private String name;
  private boolean isAccessor = false;
  
  public RegionDescriptionPerMember(Region<?,?> region, String hostingMember) {
    this.regionAttributesInfo = new RegionAttributesInfo(region.getAttributes());
    this.hostingMember = hostingMember;
    this.size = region.size();
    this.name = region.getFullPath().substring(1);
    
    //For the replicated proxy
    if ((this.regionAttributesInfo.getDataPolicy().equals(DataPolicy.EMPTY)
        && this.regionAttributesInfo.getScope().equals(Scope.DISTRIBUTED_ACK))) {
      setAccessor(true);
    } 
    
    //For the partitioned proxy
    if (this.regionAttributesInfo.getPartitionAttributesInfo() != null && this.regionAttributesInfo.getPartitionAttributesInfo().getLocalMaxMemory() == 0) {
      setAccessor(true);
    }
  }

  public boolean equals(Object obj) {
    if (obj instanceof RegionDescriptionPerMember) {
      RegionDescriptionPerMember regionDesc = (RegionDescriptionPerMember) obj;

      return this.name.equals(regionDesc.getName())
          && this.getScope().equals(regionDesc.getScope())
          && this.getDataPolicy().equals(regionDesc.getDataPolicy())
          && this.isAccessor == regionDesc.isAccessor;
    }
    return true;
  }

  public String getHostingMember() {
    return hostingMember;
  }

  public int getSize() {
    return this.size;
  }

  public String getName() {
    return this.name;
  }
  
  public Scope getScope() {
    return this.regionAttributesInfo.getScope();
  }
  
  public DataPolicy getDataPolicy() {
    return this.regionAttributesInfo.getDataPolicy();
  }
  
  public Map<String, String> getNonDefaultRegionAttributes() {
    this.regionAttributesInfo.getNonDefaultAttributes().put("size", Integer.toString(this.size));
    return this.regionAttributesInfo.getNonDefaultAttributes();
  }
  
  public Map<String, String> getNonDefaultEvictionAttributes() {
    EvictionAttributesInfo eaInfo = regionAttributesInfo.getEvictionAttributesInfo();
    if (eaInfo != null) {
      return eaInfo.getNonDefaultAttributes();
    } else {
      return Collections.emptyMap();
    }
  }
  
  public Map<String, String> getNonDefaultPartitionAttributes() {
    PartitionAttributesInfo paInfo = regionAttributesInfo.getPartitionAttributesInfo();
    if (paInfo != null) {
      return paInfo.getNonDefaultAttributes();
    } else {
      return Collections.emptyMap();
    }
  }
  
  public List<FixedPartitionAttributesInfo> getFixedPartitionAttributes() {
   PartitionAttributesInfo paInfo = regionAttributesInfo.getPartitionAttributesInfo();
   List<FixedPartitionAttributesInfo> fpa = null;
   
   if (paInfo != null) {
     fpa = paInfo.getFixedPartitionAttributesInfo();
   }
   
   return fpa;
  }

  public boolean isAccessor() {
    return isAccessor;
  }

  public void setAccessor(boolean isAccessor) {
    this.isAccessor = isAccessor;
  }
}
