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

import com.gemstone.gnu.trove.TObjectHashingStrategy;

/**
 * A hashing strategy that uses reference equality for equals implementation,
 * and regular object hashcode for computeHashCode.
 * 
 * @author swale
 * @since gfxd 1.4
 */
@SuppressWarnings("serial")
public final class ObjectEqualsHashingStrategy implements
    TObjectHashingStrategy {

  private static final ObjectEqualsHashingStrategy instance =
      new ObjectEqualsHashingStrategy();

  public static ObjectEqualsHashingStrategy getInstance() {
    return instance;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final int computeHashCode(final Object o) {
    return o != null ? o.hashCode() : 0;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final boolean equals(Object o1, Object o2) {
    return o1 == o2;
  }
}
