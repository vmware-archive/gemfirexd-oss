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
package sql.dataextractor;

import hydra.BasePrms;


public class DataExtractorPrms extends BasePrms{
  /** (long) The length of time for task in seconds. This
   *  can be used to specify granularity of a task. */
  public static Long opsTaskGranularitySec;
  
  public static Long threadGroupNames;
  
  public static Long clientVMNamesForRestart;
  
  public static Long ddlCreateTableStatements;
  
  public static Long ddlCreateTableExtensions;
  
  public static Long performDDLOps;
  
  public static Long simultaneousShutdownVMs;
  
  public static Long performUpdatesWhileShuttingDown;
  
  public static Long DDLOpsBatchSize;
  
  public static Long fileExtensionForCorruptOrDelete;
  
  public static Long corruptOrDeleteOP;
  
  // ================================================================================
  static {
    BasePrms.setValues(DataExtractorPrms.class);
  }
}
