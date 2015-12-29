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
package cacheperf.comparisons.gemfirexd.useCase1.src.matcher.utils;

public class MatchStoredProcParams {

  /**
   * Name of schema
   */
  public static final String schemaNameString = "SEC_OWNER.";
  
  /**
   * Name of channel data table
   */
  public static final String channelTableName = "SECT_CHANNEL_DATA";

  /**
   * Name of primary key field in channel data table 
   */
  public static final String channelPrimaryKey = "CHANNEL_TXN_ID";
  
  /**
   * QueryExecutor Stored procedure name
   */
  public static final String queryExecutorStoredProcName = "queryExecutorStoredProc";
  
  /**
   * ResultProcessor name 
   */
  public static final String resultProcessorName = "cacheperf.comparisons.gemfirexd.useCase1.src.matcher.storedproc.MatchResultProcessor";
}
