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
package sql.sqlDAP;

import java.util.Vector;

import sql.SQLBB;
import sql.SQLPrms;
import hydra.BasePrms;
import hydra.HydraVector;
import hydra.TestConfig;

public class SQLDAPPrms extends SQLPrms {
  static {
    setValues( SQLDAPPrms.class );
  }

  /** (boolean) whether test is partitioned on tid by List which can run certain DAP
  *
  */
  public static Long tidByList;
  
  /** (boolean) whether test is partitioned on cid by range which can run certain DAP
  *
  */
  public static Long cidByRange;
  
  /** (boolean) whether test is currently create/drop certain procedures
   * need to synchronize operations (create/drop and call procedures) if needs verification 
   *
   */
  public static Long concurrentDropDAPOp;
  
  /** (boolean) whether test only update to server groups that do not have the table
   *
   */
  public static Long updateWrongSG;
  
  /** (boolean) whether DAP procedure uses custom Result Processor
  *
  */
  public static Long testCustomProcessor;
  
}
