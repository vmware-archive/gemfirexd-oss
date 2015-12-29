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
package com.pivotal.gemfirexd.internal.engine.store.offheap;

import java.util.List;

/**
 * The methods on this interface exist for unit tests.
 * Their implementation is not optimized so they should
 * not be used from product code.
 * 
 * @author darrel
 *
 */
public interface CollectionBasedOHAddressCache extends OHAddressCache {
  /**
   * Returns the number of values in the OHAddressCache
   */
  public int testHook_getSize();
  /**
   * Copies all the values that are in the OHAddressCache into a List and returns the list.
   */
  public List<Long> testHook_copyToList();

}
