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
package com.gemstone.gemfire.internal.cache;

import java.util.HashSet;

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.cache.CacheDistributionAdvisor.InitialImageAdvice;

/**
 * Used to exchange profiles during region initialization and determine the
 * targets for GII. There are currently two implementations, one for persistent
 * regions and one of non persistent regions. The persistent region
 * implementation will wait for members to come online that may have a later
 * copies of the region.
 * 
 * @author dsmith
 * 
 */
public interface ProfileExchangeProcessor {
  /** Exchange profiles with other members to initialize the region*/
  void initializeRegion();
  /** Get, and possibling wait for, the members that we should initialize from. */ 
  InitialImageAdvice getInitialImageAdvice(InitialImageAdvice previousAdvice);
  
  void setOnline(InternalDistributedMember target);
}
