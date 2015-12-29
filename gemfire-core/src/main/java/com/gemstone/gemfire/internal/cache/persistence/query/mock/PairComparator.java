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
package com.gemstone.gemfire.internal.cache.persistence.query.mock;

import java.util.Comparator;

/**
 * A comparator for Pair objects that uses two
 * passed in comparators.
 * @author dsmith
 *
 */
public class PairComparator implements Comparator<Pair> {
  private Comparator xComparator;
  private Comparator yComparator;

  public PairComparator(Comparator xComparator, Comparator yComparator) {
    this.xComparator = xComparator;
    this.yComparator = yComparator;
    
  }

  @Override
  public int compare(Pair o1, Pair o2) {
    int result = xComparator.compare(o1.getX(), o2.getX());
    if(result == 0) {
      result = yComparator.compare(o1.getY(), o2.getY());
    }
    return result;
  }

}
