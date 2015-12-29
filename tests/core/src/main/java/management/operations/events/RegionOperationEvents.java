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

import java.util.Set;

import com.gemstone.gemfire.cache.Region;

public interface RegionOperationEvents {
  
  /*
   * Region Operations
   */
  public void regionAdded(Region region);
  
  void regionAdded(String regionPath);

  public void regionDestroyed(String name, Set<String> chlildren);

  public void regionInvalidated(String name);

  public void regionClosed(String name);

  public void regionCleared(String name);

  public static String EVENT_REGION_ADDED = "EVENT_REGION_ADDED";
  public static String EVENT_REGION_DESTROYED = "EVENT_REGION_DESTROYED";
  public static String EVENT_REGION_CLOSED = "EVENT_REGION_CLOSED";
  public static String EVENT_REGION_INVALIDATED = "EVENT_REGION_INVALIDATED";
  public static String EVENT_REGION_CLEARED = "EVENT_REGION_CLEARED";
  

}
