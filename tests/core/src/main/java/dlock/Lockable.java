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

package dlock;

import com.gemstone.gemfire.cache.Region;

public interface Lockable {

  /**
   *  Creates a lockable object and puts it in the cache.
   */
  public void createDataInCache( Region region, Object name );

  /**
   *  Locks a lockable object.
   */
  public void lock( Region aRegion, Object name );

  /**
   *  Unlocks a lockable object.
   */
  public void unlock( Region aRegion, Object name );

  /**
   *  Reads a lockable object.
   */
  public void read( Region region, Object name );

  /**
   *  Updates a lockable object.
   */
  public void update( Region region, Object name );

  /**
   *  Tests whether a lockable object has the expected state.
   */
  public void validate( Region region, Info info );
}
