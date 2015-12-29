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
package sql.mbeans;

public class MBeanTestConstants {

  public static final String[] TABLE_ATTRS = { 
      "Name", 
      "ParentSchema", 
      "ServerGroups",
      "Definition", 
      "Policy", 
     /* "PartitioningScheme",
      "ColocationScheme", 
      "PersistenceScheme", */ 
      "Inserts", 
      "Updates",
      "Deletes", 
      "EntrySize", 
      "KeySize", 
      "NumberOfRows" };
  
  public static final String[] TABLE_METHODS = {
      "listIndexInfo",
      "fetchMetadata",
   /*   "showPartitionAttributes", */
      "showEvictionAttributes"
  };
  
  public static final String[] MEMBER_ATTRS = { 
        "Name",
        "Id",
        "Groups",
        "DataStore",
        "Locator",
        "ProcedureCallsCompleted",
        "NetworkServerClientConnectionStats",
        "NetworkServerPeerConnectionStats",
        "NetworkServerNestedConnectionStats",
        "NetworkServerInternalConnectionStats",
        "ProcedureCallsInProgress"          
    };

  public static final String[] AGGR_MEMBER_ATTRS = { 
      "Members",
      "NetworkServerClientConnectionStats", 
      "NetworkServerPeerConnectionStats",
      "NetworkServerNestedConnectionStats",
      "NetworkServerInternalConnectionStats", 
      "ProcedureCallsInProgress",
      "ProcedureCallsCompleted" 
    };

  public static final String[] MEMBER_METHODS = { 
      "fetchMetadata", 
      "fetchEvictionPercent",
      "fetchCriticalPercent", 
      "detectDeadlocks", 
      "updateEvictionPercent",
      "updateCriticalPercent"
      };
  
  public static final String[] STATEMENT_ATTRS = {
      "QNNumTimesCompiled",
      "QNNumExecutions",
      "QNNumExecutionsInProgress",
      "QNNumTimesGlobalIndexLookup",
      "QNNumRowsModified",
      "QNParseTime",
      "QNBindTime",
      "QNOptimizeTime",
      "QNRoutingInfoTime",
      "QNGenerateTime",
      "QNTotalCompilationTime",
      "QNExecuteTime",
      "QNProjectionTime",
      "QNTotalExecutionTime",
      "QNRowsModificationTime",
     /* "NumProjectedRows",
      "NumNLJoinRowsReturned",*/ //- TODO Viren - chck why it is not coming.
      "NumHASHJoinRowsReturned",
      "NumTableRowsScanned",
      "NumRowsHashScanned",
      "NumIndexRowsScanned",
      "NumRowsSorted",
      "NumSortRowsOverflowed",
      "NumSingleHopExecutions",
      "SubQueryNumRowsSeen",
      "MsgDeSerTime",
      "MsgProcessTime",
      "ResultIterationTime",
      "RespSerTime",
      "ThrottleTime",
      "NLJoinTime",
      "HASHJoinTime",
      "TableScanTime",
      "HashScanTime",
      "IndexScanTime",
      "SortTime",
      "SubQueryExecutionTime",
      "DNNumTimesCompiled",
      "DNNumExecution",
      "DNNumExecutionsInProgress",
      "DNNumTimesGlobalIndexLookup",
      "DNNumRowsModified",
      "DNParseTime",
      "DNBindTime",
      "DNOptimizeTime",
      "DNRoutingInfoTime",
      "DNGenerateTime",
      "DNTotalCompilationTime",
      "DNExecutionTime",
      "DNProjectionTime",
      "DNTotalExecutionTime",
      "DNRowsModificationTime",
      "QNNumRowsSeen",
      "QNMsgSendTime",
      "QNMsgSerTime",
      "QNRespDeSerTime"};
}
