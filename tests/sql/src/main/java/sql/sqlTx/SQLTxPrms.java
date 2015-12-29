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
package sql.sqlTx;

import hydra.BasePrms;

public class SQLTxPrms extends BasePrms {

  static {
    setValues( SQLTxPrms.class );
  }

  /** (boolean) whether only one thread performing transaction at a given time
  *
  */
  public static Long doOpByOne;
  
  /** (boolean) whether use lock timeout (default conflict does not use timeout)
  *
  */
  public static Long useTimeout;
  
  /** (boolean) whether the trigger calls procedure
  *
  */
  public static Long callTriggerProcedrue;
  
  
  /** (boolean) whether ticket 43591 is fixed
  *
  */
  public static Long is43591fixed;
    
  
  /** (boolean) whether to reproduce ticket 49947 -- start up hang having trigger with DAP  
   * 
   */
  public static Long reproduce49947;
  
  /** (boolean) whether the test use thin client driver in tx
   */
  public static Long useThinClientDriverInTx;
   
  /** (boolean) whether use both RepeatableRead and ReadCommitted isolations level in tests
   */
  public static Long mixRR_RC;
  
  /** (boolean) whether disallow batching with secondary data when using thin client driver
   * if use default batching but without secondary data -- no batching needs to be set as true 
   * as this does not affect the default test setting
   */
  public static Long nobatching; 
  
  /**
   *  (boolean) reuse the original non txn test.
   */
  public static Long useOriginalNonTx;
     

}
