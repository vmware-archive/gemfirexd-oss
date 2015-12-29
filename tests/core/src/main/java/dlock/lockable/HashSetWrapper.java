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

package dlock.lockable;

import com.gemstone.gemfire.cache.*;

import java.util.HashSet;
import util.NameFactory;

import util.CacheUtil;
import dlock.*;

/**
 *  Lockable wrapper for cached HashSet.
 */

public class HashSetWrapper extends BaseWrapper {

  public HashSetWrapper() {
    super();
  }
  public void createDataInCache( Region region, Object name ) {
    HashSet obj = new HashSet();
    CacheUtil.put( region, name, obj );
  }
  public void read( Region region, Object name ) {
    HashSet obj = (HashSet) CacheUtil.get( region, name );
    this.counters.increment( DLockBlackboard.HashSetReads );
  }
  protected Object update( Object obj ) {
    HashSet newobj = (HashSet) obj;
    newobj.add( NameFactory.getNextPositiveObjectName() );
    return newobj;
  }
  protected void noteUpdate() {
    this.counters.increment( DLockBlackboard.HashSetUpdates );
  }
  public void validate( Region region, Info info ) {
    HashSet obj = (HashSet) CacheUtil.get( region, info.getName() );
    if ( obj.size() != info.getUpdates() )
      throw new DLockTestException( "Object data is invalid for " + info.getName() +
                                       " with expected count " + info.getUpdates() + ": " + obj );
  }
}
