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

import java.util.Collection;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.distributed.DistributedMember;

/**
 * The InternalCache interface is contract for implementing classes for defining internal cache operations that should
 * not be part of the "public" API of the implementing class.
 * </p>
 * @author jblum
 * @see com.gemstone.gemfire.cache.Cache
 * @since 7.0
 */
public interface InternalCache extends Cache {

  public DistributedMember getMyId();

  public Collection<DiskStoreImpl> listDiskStores();

  public Collection<DiskStoreImpl> listDiskStoresIncludingDefault();

  public Collection<DiskStoreImpl> listDiskStoresIncludingRegionOwned();

}
