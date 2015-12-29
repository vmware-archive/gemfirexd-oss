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
import java.util.Iterator;

public class SetOfKeysIterator implements Iterator<RegionEntry> {

  private Iterator keyIterator;
  private DistributedRegion region;

  public SetOfKeysIterator(HashSet<Object> unfinishedKeys,
      DistributedRegion region) {
    this.keyIterator = unfinishedKeys.iterator();
    this.region = region;
  }

  @Override
  public boolean hasNext() {
    return keyIterator.hasNext();
  }

  @Override
  public RegionEntry next() {
    Object nextKey = keyIterator.next();
    if(nextKey == null) {
      return null;
    }
    return region.getRegionMap().getEntry(nextKey);
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

}
