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
package com.gemstone.gemfire.distributed;

import com.gemstone.gemfire.GemFireException;

/**
 * A <code>LeaseExpiredException</code> is thrown when GemFire
 * detects that a distributed lock obtained by the current thread
 * with a limited lease (see @link DistributedLockService} has 
 * expired before it was explicitly released.
 */

public class LeaseExpiredException extends GemFireException  {
private static final long serialVersionUID = 6216142987243536540L;

  /**
   * Creates a new <code>LeaseExpiredException</code>
   */
  public LeaseExpiredException(String s) {
    super(s);
  }

}
