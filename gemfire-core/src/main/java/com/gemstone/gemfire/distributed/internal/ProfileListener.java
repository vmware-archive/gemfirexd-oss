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
package com.gemstone.gemfire.distributed.internal;

import com.gemstone.gemfire.distributed.internal.DistributionAdvisor.Profile;

/**
 * Callback for changes to profiles in a DistributionAdvisor. A ProfileListener
 * can be registered with a DistributionAdvisor.
 * 
 * These methods are called while the monitor is held on the DistributionAdvisor.
 * @author dsmith
 *
 */
public interface ProfileListener {
  /**
   * Method is invoked after
   * a new profile is created/added to profiles.
   * @param profile the created profile
   */
  void profileCreated(Profile profile);
  
  /**
   * Method is invoked after
   * a profile is updated in profiles.
   * @param profile the updated profile
   */
  void profileUpdated(Profile profile);

  /**
   * Method is invoked after a profile is removed from profiles.
   * 
   * @param profile
   *          the removed profile
   * @param destroyed
   *          indicated that the profile member was destroyed, rather than
   *          closed (used for persistence)
   */
  void profileRemoved(Profile profile, boolean destroyed);

}
