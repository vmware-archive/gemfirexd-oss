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
package com.gemstone.gemfire.admin;

/**
 * An event that describes an operation on a region.
 * Instances of this are delivered to a {@link SystemMemberCacheListener} when a
 * a region comes or goes.
 *
 * @author Darrel Schneider
 * @since 5.0
 * @deprecated as of 7.0 use the {@link com.gemstone.gemfire.management} package instead
 */
public interface SystemMemberRegionEvent extends SystemMemberCacheEvent {
  /**
   * Returns the full path of the region the event was done on.
   */
  public String getRegionPath();
}
