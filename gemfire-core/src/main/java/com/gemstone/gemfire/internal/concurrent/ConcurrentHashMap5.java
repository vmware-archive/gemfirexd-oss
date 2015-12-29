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

import com.gemstone.gemfire.internal.size.SingleObjectSizer;

/**
 * CM implementation for JDK 5.
 * @author darrel
 */
class ConcurrentHashMap5
  extends java.util.concurrent.ConcurrentHashMap
  implements CM {
  private static final long serialVersionUID = 721923746435919150L;
  public ConcurrentHashMap5() {
    super();
  }
  public ConcurrentHashMap5(int initialCapacity) {
    super(initialCapacity);
  }
  public ConcurrentHashMap5(int initialCapacity, float loadFactor, int concurrencyLevel) {
    super(initialCapacity, loadFactor, concurrencyLevel);
  }
  public ConcurrentHashMap5(java.util.Map m) {
    super(m);
  }
  public long estimateMemoryOverhead(SingleObjectSizer sizer) {
    return 0;
  }
}
