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

package roles;

import hydra.*;

/**
 *  Test Parameters for the roles tests
 */

public class RolesPrms extends BasePrms {

  /** (int) Used in dynamic task stop (how to stop the target vm)
   *  Choices are MEAN_KILL, NICE_KILL, MEAN_EXIT, NICE_EXIT
   *
   *  @see hydra.ClientVmMgr <b>stop mode</b>
   *
   */
  public static Long stopMode;

  /** (int) Used in dynamic task start-up (how the client VM will be 
   *  restarted.  Choices are IMMEDIATE, ON_DEMAND, NEVER
   *
   *  @see hydra.ClientVmMgr <b>startMode</b>
   *
   */
  public static Long startMode;

  /** (String) client(s) to target for stop/start operations
   *
   *  @see hydra.ClientVmInfo
   */
  public static Long clientsToStop;

  static {
    setValues( RolesPrms.class );
  }
}
