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

import com.pivotal.gemfirexd.internal.engine.management.AggregateTableMXBean;


/**
*
* @author Abhishek Chaudhari, Ajay Pande
* @since gfxd 1.0
*/
public class AggregateTableMBean implements AggregateTableMXBean {

  private AggregateTableMBeanBridge bridge;
  public AggregateTableMBean(AggregateTableMBeanBridge bridge) {
    this.bridge = bridge;
  }

 /* @Override
  public String getName() {
    return this.bridge.getName();
  }

  @Override
  public String getParentSchema() {
    return this.bridge.getParentSchema();
  }
  
  @Override
  public String[] getServerGroups() {
    return ManagementUtils.EMPTY_STRING_ARRAY;
  }

  @Override
  public String getPolicy() {
    return this.bridge.getPolicy();
  }

  @Override
  public String getPartitioningScheme() {
    return this.bridge.getPartitioningScheme();
  }
  @Override
  public String getColocationScheme() {
    return this.bridge.getColocationScheme();
  }

  @Override
  public String getPersistenceScheme() {
    return this.bridge.getPersistenceScheme();
  }


  @Override
  public int getScans() {
    return this.bridge.getScans();
  }

  @Override
  public int getInserts() {
    return this.bridge.getInserts();
  }
  
    @Override
  public int getUpdates() {
    return this.bridge.getUpdates();
  }

  @Override
  public int getDeletes() {
    return this.bridge.getDeletes();
  }
  
   @Override
  public long getKeySize() {
    return this.bridge.getKeySize();
  }

  @Override
  public int getNumberOfEntries() {
    return this.bridge.getNumberOfEntries();
  }

  @Override
  public TableMXBean.TableMetadata fetchMetadata() {
    return this.bridge.fetchMetadata();
  }
*/



  @Override
  public double getEntrySize() {
    return this.bridge.getEntrySize();
  }

  @Override
  public long getNumberOfRows() {
    return this.bridge.getNumberOfRows();
  }
  
  public AggregateTableMBeanBridge getBridge(){
    return this.bridge;
  }
  
 /* @Override
  public String showDefinition() {
    return this.bridge.showDefinition();
  }

  @Override
  public PartitionAttributesData showPartitionAttributes() {
    return this.bridge.showPartitionAttributes();
  }

  @Override
  public TabularData showDiskAttributes() {
    return this.bridge.showDiskAttributes();
  }

  @Override
  public EvictionAttributesData showEvictionAttributes() {
    return this.bridge.showEvictionAttributes();
  }

  @Override
  public String[] showColocatedWith() {
    return ManagementConstants.NO_DATA_STRING;
  }*/

  /* (non-Javadoc)
   * @see java.lang.Object#toString()
   */
 /* @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append(getClass().getSimpleName()).append(" [");
    builder.append(getParentSchema()).append(".").append(getName());
    builder.append(", Policy=").append(getPolicy()).append("]");
    return builder.toString();
  }*/
}
