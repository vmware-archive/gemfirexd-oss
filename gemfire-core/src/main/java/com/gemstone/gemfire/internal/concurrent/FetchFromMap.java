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

package com.gemstone.gemfire.internal.concurrent;

/**
 * Passed in some map/set methods to fetch additional information as a result of
 * lookup. Currently only map/set key is fetched.
 * 
 * @author swale
 * @since gfxd 1.0
 */
public interface FetchFromMap {

  /** set the actual key object in the map and its version */
  public void setMapKey(Object key, int version);

  /** set the key set via the {@link #setMapKey(Object, int)} callback */
  public Object getCurrentKey();

  /** get the version set vial the {@link #setMapKey(Object, int)} callback */
  public int getCurrentNodeVersion();
}
