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
import javax.management.ObjectName;
import javax.management.openmbean.TabularData;
import com.gemstone.gemfire.management.internal.FederationComponent;
import com.gemstone.gemfire.management.EvictionAttributesData;
import com.gemstone.gemfire.management.PartitionAttributesData;
import com.pivotal.gemfirexd.internal.engine.management.TableMXBean;
/**
*
* @author Abhishek Chaudhari, Ajay Pande
* @since gfxd 1.0
*/
public class AggregateTableMBeanBridge {

  private String tableName;
  private String parentSchema;
  private List<String> definition;
  
  private GfxdAggregateTableStatsMonitor gfxdAggregateTableStatsMonitor;

  public AggregateTableMBeanBridge(String tableName, String parentSchema, List<String> definition) {
    this.tableName = tableName;
    this.parentSchema = parentSchema;
    this.definition = definition;
    this.gfxdAggregateTableStatsMonitor = new GfxdAggregateTableStatsMonitor();
  }

  public String getName() {
    return tableName;
  }

  public String getParentSchema() {
    return parentSchema;
  }

  public int getScans() {
	    return 0;
	  }

	  public int getInserts() {
	    return 0;
	  }

	  public int getUpdates() {
	    return 0;
	  }

	  public int getDeletes() {
	    return 0;
	  }


	  public long getKeySize() {
		    return 0;
		  }

		  public int getNumberOfEntries() {
		    return 0;
		  }

		  public String getPolicy() {
		    return null;
		  }

		  public String getPartitioningScheme() {
		    return null;
		  }

  public double getEntrySize() {
    return this.gfxdAggregateTableStatsMonitor.getEntrySize();
  }

 

  public long getNumberOfRows() {
    return this.gfxdAggregateTableStatsMonitor.getNumberOfRows();
  }
  
 public void update(FederationComponent newState,
     FederationComponent oldState, ObjectName objectName){

   this.gfxdAggregateTableStatsMonitor.aggregate(newState, oldState, objectName);
 }
 
 public String getColocationScheme() {
	    return null;
	  }

	  public String getPersistenceScheme() {
	    return null;
	  }

	  public TableMXBean.TableMetadata fetchMetadata() {
	    return null;
	  }

	  public List<String> showDefinition() {
	    return definition;
	  }

	  public PartitionAttributesData showPartitionAttributes() {
	    return null;
	  }


	  public TabularData showDiskAttributes() {
		    return null;
		  }

		  public EvictionAttributesData showEvictionAttributes() {
		    return null;
		  }

		  public String[] showColocatedWith() {
		    return null;
		  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append(getClass().getSimpleName()).append(" [");
    builder.append(parentSchema).append(".");
    builder.append(tableName);
    builder.append(", policy=").append(tableName).append("]");
    return builder.toString();
  }
}
