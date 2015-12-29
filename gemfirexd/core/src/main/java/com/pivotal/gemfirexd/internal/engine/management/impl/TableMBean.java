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
package com.pivotal.gemfirexd.internal.engine.management.impl;

import java.util.List;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.management.EvictionAttributesData;
import com.gemstone.gemfire.management.PartitionAttributesData;
import com.gemstone.gemfire.management.internal.beans.RegionMBeanBridge;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.management.TableMXBean;
import com.pivotal.gemfirexd.internal.engine.management.impl.TableMBeanBridge.IndexInfoData;
import com.pivotal.gemfirexd.internal.engine.management.impl.TableMBeanBridge.IndexStatsData;


/**
*
* @author Abhishek Chaudhari, Ajay Pande
* @since gfxd 1.0
*/
public class TableMBean implements TableMXBean, Cleanable {

  private TableMBeanBridge  tableBridge;
  private RegionMBeanBridge<?, ?> regionBridge;

  LogWriter log =  Misc.getCacheLogWriter(); 

  
  public <K, V> TableMBean(TableMBeanBridge tableBridge, RegionMBeanBridge<K, V> regionBridge) {
    this.tableBridge  = tableBridge;
    this.regionBridge = regionBridge;
  }

  @Override
  public String getName() {
    return this.tableBridge.getName();
  }

  @Override
  public String getParentSchema() {
    return this.tableBridge.getParentSchema();
  }

  @Override
  public String[] getServerGroups() {
    return this.tableBridge.getServerGroups();
  }

  @Override
  public List<String> getDefinition() {
    return this.tableBridge.getDefinition();
  }

  @Override
  public String getPolicy() {
    return this.tableBridge.getPolicy();
  }

  @Override
  public String getPartitioningScheme() {
    return this.tableBridge.getPartitioningScheme();
  }

 /* @Override
  public String getColocationScheme() {
    String colocatedWith = null;
    PartitionAttributesData listPartitionAttributes = this.regionBridge.listPartitionAttributes();
    if (listPartitionAttributes != null) {
      colocatedWith = listPartitionAttributes.getColocatedWith();
    }
    if (colocatedWith != null) {
      colocatedWith = ManagementUtils.NA;
    }
    return colocatedWith;
  }

  @Override
  public String getPersistenceScheme() {
    return this.tableBridge.getPersistenceScheme();
  } */


//  @Override
//  public int getScans() {
//    return this.tableBridge.getScans();
//  }

  @Override
  public int getInserts() {
    return this.tableBridge.getInserts();
  }

  @Override
  public int getUpdates() {
    return this.tableBridge.getUpdates();
  }

  @Override
  public int getDeletes() {
    return this.tableBridge.getDeletes();
  }

  @Override
  public double getEntrySize() {
    return this.tableBridge.getEntrySize();
  }

  @Override
  public double getKeySize() {
    return this.tableBridge.getKeySize();
  }

  @Override
  public long getNumberOfRows() {
    return Long.valueOf(this.regionBridge.getEntryCount()).longValue();
  }

  @Override
  public TableMetadata fetchMetadata() {
    return this.tableBridge.fetchMetadata();
  }

 /* @Override
  public PartitionAttributesData showPartitionAttributes() {
    return this.regionBridge.listPartitionAttributes();
  } */

//  @Override
//  public String[] listDiskStores() {
//    return ManagementUtils.EMPTY_STRING_ARRAY;
//  }

  @Override
  public EvictionAttributesData showEvictionAttributes() {
    return regionBridge.listEvictionAttributes();
  }

  public IndexInfoData[] listIndexInfo() {
    return this.tableBridge.listIndexInfo();
  }
  
  public IndexStatsData[] listIndexStats() {
    return this.tableBridge.listIndexStats();
  }  

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((tableBridge == null) ? 0 : tableBridge.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    TableMBean other = (TableMBean) obj;
    if (tableBridge == null) {
      if (other.tableBridge != null) {
        return false;
      }
    } else if (!tableBridge.equals(other.tableBridge)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append(getClass().getSimpleName()).append(" [");
    builder.append(getParentSchema()).append(".").append(getName());
    builder.append(", Policy=").append(getPolicy()).append("]");
    return builder.toString();
  }

  @Override
  public void cleanUp() {
    this.tableBridge.cleanUp();
    this.tableBridge  = null;
    this.regionBridge = null;
  }
  
  public void setDefinition(List<String> newDefinition) {
    this.tableBridge.setDefinition(newDefinition);
   
  }
  
  public LocalRegion getRegion(){
    return this.tableBridge.getRegion();
  }
}
