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
package com.gemstone.gemfire.internal.cache.wan;

/**
 * This interface is for Jayesh's use case for WAN BootStrapping and will not be part of the product release.
 * 
 * @author kbachhav
 *
 */
public interface DistributedSystemListener {

  // remoteDSId is the distributed-system-id of the distributed system that has joined existing sites
  public void addedDistributedSystem(int remoteDsId);
  
  // This is invoked when user explicitly removed the distributed system from the membership
  public void removedDistributedSystem(int remoteDsId);
}
