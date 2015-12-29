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
package management.operations.events;

import com.gemstone.gemfire.distributed.DistributedLockService;

public interface DLockOperationEvents {
  
  /*
   * Region Operations
   */
  public void dlockCreated(String name, DistributedLockService service);

  public void dlockDestroyed(String name, DistributedLockService service);

  public void dlockLockUnLock(String name, DistributedLockService service, long l1, long l2, String lockName);

  public void dlockBecomeGrantor(String name, DistributedLockService service);
  

  public static String EVENT_DLOCK_ADDED = "EVENT_DLOCK_ADDED";
  public static String EVENT_DLOCK_DESTROYED = "EVENT_DLOCK_DESTROYED";
  public static String EVENT_DLOCK_LOCKUNLOCK = "EVENT_DLOCK_LOCKUNLOCK";
  public static String EVENT_DLOCK_BECOMEGRANTOR = "EVENT_DLOCK_BECOMEGRANTOR";
  

}
