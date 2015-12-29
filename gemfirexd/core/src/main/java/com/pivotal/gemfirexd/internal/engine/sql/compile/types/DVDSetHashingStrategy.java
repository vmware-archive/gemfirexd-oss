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
package com.pivotal.gemfirexd.internal.engine.sql.compile.types;

import com.gemstone.gnu.trove.TObjectHashingStrategy;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.types.BooleanDataValue;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;

/*
 * @author vivekb
 */
public final class DVDSetHashingStrategy implements TObjectHashingStrategy {

  private static final long serialVersionUID = 2173762090709422691L;

  @Override
  public final int computeHashCode(Object object) {
    DataValueDescriptor ele = (DataValueDescriptor)object;
    // passing a value < strlen in maxWidth so that *CHAR never get truncated
    // 7 is just a prime number
    return ele.computeHashCode(-1, 7);
  }

  /*
   * For implementation details, @see DataType.in()
   */
  @Override
  public final boolean equals(Object o1, Object o2) {
    DataValueDescriptor leftEle = (DataValueDescriptor)o1;
    DataValueDescriptor rightEle = (DataValueDescriptor)o2;
    DataValueDescriptor comparator = (leftEle.typePrecedence() < rightEle
        .typePrecedence()) ? rightEle : leftEle;
    try {
      BooleanDataValue bdv = comparator.equals(leftEle, rightEle);
      return bdv.getBoolean();
    } catch (StandardException e) {
      SanityManager.THROWASSERT("Failure in DVDSetHashingStrategy#equals", e);
    }
    return false;
  }

  private static final DVDSetHashingStrategy instance = new DVDSetHashingStrategy();

  public static DVDSetHashingStrategy getInstance() {
    return instance;
  }
} // DVDSetHashingStrategy
