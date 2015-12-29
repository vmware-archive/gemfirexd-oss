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
package com.pivotal.gemfirexd.internal.engine.management;

/**
 *
 * @author Abhishek Chaudhari, Ajay Pande
 * @since gfxd 1.0
 */
public interface AggregateTableMXBean {
  //[A] MBean Attributes
  //0. Basic
 /* String getName();
  String getParentSchema();
  String[] getServerGroups();*/

/*	  //1. from Region.getAttributes()
	  String getPolicy(); // Region.getAttributes()
	  String getPartitioningScheme(); // Region.getAttributes().getPartitionAttributes()
	  String getColocationScheme(); // Region.getAttributes().getPartitionAttributes()
	  String getPersistenceScheme(); // TODO - Abhishek yet TBD

	  // 2. from procedure sys.statementStats
	  int getScans();
	  int getInserts();
	  int getUpdates();
	  int getDeletes();*/
  // 3. from MemoryAnalytics VTI
	/**
	 * Aggregation of Entry overhead, in kilobytes. 
	 * Only reflects the amount of memory required to hold the table row in memory 
	 * but not including the memory to hold its key and value
	 * 
	 */
  double getEntrySize();
 // long getKeySize();
  
  /**
   * Aggregation of Number of rows in a table
   *  
   */
  long  getNumberOfRows();// TODO Abhishek - should this be called number of rows??

  //[B] MBean Operations
 /* TableMetadata fetchMetadata(); // GFXD VTI sys.tables
  String      showDefinition();
  PartitionAttributesData showPartitionAttributes(); // GFXD Stored procedure?
  TabularData showDiskAttributes(); // GFXD Stored procedure?
  EvictionAttributesData showEvictionAttributes(); // GFXD Stored procedure?
  String[]    showColocatedWith(); // GFXD Stored procedure?*/
}
