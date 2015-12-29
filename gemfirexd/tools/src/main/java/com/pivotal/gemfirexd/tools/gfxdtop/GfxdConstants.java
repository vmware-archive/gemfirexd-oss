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
package com.pivotal.gemfirexd.tools.gfxdtop;


public class GfxdConstants {

  public static final String OBJECT_DOMAIN_NAME_GEMFIREXD = "GemFireXD";
  public static final String OBJECT_NAME_STATEMENT_DISTRIBUTED = OBJECT_DOMAIN_NAME_GEMFIREXD
      + ":service=Statement,type=Aggregate,*";

  public static final String MBEAN_ATTRIBUTE_NAME = "Name";

  public static final String MBEAN_ATTRIBUTE_NUMTIMESCOMPILED = "NumTimesCompiled";
  public static final String MBEAN_ATTRIBUTE_NUMEXECUTION = "NumExecution";
  public static final String MBEAN_ATTRIBUTE_NUMEXECUTIONSINPROGRESS = "NumExecutionsInProgress";
  public static final String MBEAN_ATTRIBUTE_NUMTIMESGLOBALINDEXLOOKUP = "NumTimesGlobalIndexLookup";
  public static final String MBEAN_ATTRIBUTE_NUMROWSMODIFIED = "NumRowsModified";
  public static final String MBEAN_ATTRIBUTE_PARSETIME = "ParseTime";
  public static final String MBEAN_ATTRIBUTE_BINDTIME = "BindTime";
  public static final String MBEAN_ATTRIBUTE_OPTIMIZETIME = "OptimizeTime";
  public static final String MBEAN_ATTRIBUTE_ROUTINGINFOTIME = "RoutingInfoTime";
  public static final String MBEAN_ATTRIBUTE_GENERATETIME = "GenerateTime";
  public static final String MBEAN_ATTRIBUTE_TOTALCOMPILATIONTIME = "TotalCompilationTime";
  public static final String MBEAN_ATTRIBUTE_EXECUTIONTIME = "ExecutionTime";
  public static final String MBEAN_ATTRIBUTE_PROJECTIONTIME = "ProjectionTime";
  public static final String MBEAN_ATTRIBUTE_TOTALEXECUTIONTIME = "TotalExecutionTime";
  public static final String MBEAN_ATTRIBUTE_ROWSMODIFICATIONTIME = "RowsModificationTime";
  public static final String MBEAN_ATTRIBUTE_QNNUMROWSSEEN = "QNNumRowsSeen";
  public static final String MBEAN_ATTRIBUTE_QNMSGSENDTIME = "QNMsgSendTime";
  public static final String MBEAN_ATTRIBUTE_QNMSGSERTIME = "QNMsgSerTime";
  public static final String MBEAN_ATTRIBUTE_QNRESPDESERTIME = "QNRespDeSerTime";
  public static final String MBEAN_ATTRIBUTE_QUERYDEFINITION = "Query";

  public static final String[] STATEMENT_MBEAN_ATTRIBUTES = { MBEAN_ATTRIBUTE_NAME, MBEAN_ATTRIBUTE_NUMTIMESCOMPILED,
      MBEAN_ATTRIBUTE_NUMEXECUTION, MBEAN_ATTRIBUTE_NUMEXECUTIONSINPROGRESS, MBEAN_ATTRIBUTE_NUMTIMESGLOBALINDEXLOOKUP,
      MBEAN_ATTRIBUTE_NUMROWSMODIFIED, MBEAN_ATTRIBUTE_PARSETIME, MBEAN_ATTRIBUTE_BINDTIME,
      MBEAN_ATTRIBUTE_OPTIMIZETIME, MBEAN_ATTRIBUTE_ROUTINGINFOTIME, MBEAN_ATTRIBUTE_GENERATETIME,
      MBEAN_ATTRIBUTE_TOTALCOMPILATIONTIME, MBEAN_ATTRIBUTE_EXECUTIONTIME, MBEAN_ATTRIBUTE_PROJECTIONTIME,
      MBEAN_ATTRIBUTE_TOTALEXECUTIONTIME, MBEAN_ATTRIBUTE_ROWSMODIFICATIONTIME, MBEAN_ATTRIBUTE_QNNUMROWSSEEN,
      MBEAN_ATTRIBUTE_QNMSGSENDTIME, MBEAN_ATTRIBUTE_QNMSGSERTIME };

  // column names
  public static final String MBEAN_COLNAME_NUMTIMESCOMPILED = "NumTimesCompiled";
  public static final String MBEAN_COLNAME_NUMEXECUTION = "NumExecution";
  public static final String MBEAN_COLNAME_NUMEXECUTIONSINPROGRESS = "NumExecutionsInProgress";
  public static final String MBEAN_COLNAME_NUMTIMESGLOBALINDEXLOOKUP = "NumTimesGlobalIndexLookup";
  public static final String MBEAN_COLNAME_NUMROWSMODIFIED = "NumRowsModified";
  public static final String MBEAN_COLNAME_PARSETIME = "ParseTime(ms)";
  public static final String MBEAN_COLNAME_BINDTIME = "BindTime(ms)";
  public static final String MBEAN_COLNAME_OPTIMIZETIME = "OptimizeTime(ms)";
  public static final String MBEAN_COLNAME_ROUTINGINFOTIME = "RoutingInfoTime(ms)";
  public static final String MBEAN_COLNAME_GENERATETIME = "GenerateTime(ms)";
  public static final String MBEAN_COLNAME_TOTALCOMPILATIONTIME = "TotalCompilationTime(ms)";
  public static final String MBEAN_COLNAME_EXECUTIONTIME = "ExecutionTime(ns)";
  public static final String MBEAN_COLNAME_PROJECTIONTIME = "ProjectionTime(ns)";
  public static final String MBEAN_COLNAME_TOTALEXECUTIONTIME = "TotalExecutionTime(ns)";
  public static final String MBEAN_COLNAME_ROWSMODIFICATIONTIME = "RowsModificationTime(ns)";
  public static final String MBEAN_COLNAME_QNNUMROWSSEEN = "QNNumRowsSeen";
  public static final String MBEAN_COLNAME_QNMSGSENDTIME = "QNMsgSendTime(ns)";
  public static final String MBEAN_COLNAME_QNMSGSERTIME = "QNMsgSerTime(ns)";
  public static final String MBEAN_COLNAME_QNRESPDESERTIME = "QNRespDeSerTime(ns)";
  public static final String MBEAN_COLNAME_QUERYDEFINITION = "Query";

}
