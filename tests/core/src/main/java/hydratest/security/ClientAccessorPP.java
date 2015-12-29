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
package hydratest.security;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.security.AccessControl;
import com.gemstone.gemfire.cache.operations.OperationContext;
import com.gemstone.gemfire.distributed.DistributedMember;

import java.security.Principal;

public class ClientAccessorPP implements AccessControl {

  public static AccessControl create() {
    return new ClientAccessorPP();
  }

  public void init(Principal principal, DistributedMember remoteMember,
      Cache cache) {
  }

  public boolean authorizeOperation(String regionName,
                                    OperationContext context) {
    return true;
  }

  public void close() {
  }
}
