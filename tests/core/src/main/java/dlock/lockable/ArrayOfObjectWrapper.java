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

import util.CacheUtil;
import dlock.*;

/**
 *  Lockable wrapper for cached Object[].
 */

public class ArrayOfObjectWrapper extends BaseWrapper {

  public ArrayOfObjectWrapper() {
    super();
  }
  public void createDataInCache( Region region, Object name ) {
    Object[] obj = new Object[100];
    for ( int i = 0; i < 100; i++ )
      obj[i] = new Counter();
    CacheUtil.put( region, name, obj );
  }
  public void read( Region region, Object name ) {
    Object[] obj = (Object[]) CacheUtil.get( region, name );
    this.counters.increment( DLockBlackboard.ArrayOfObjectReads );
  }
  protected Object update( Object obj ) {
    Object[] newobj = (Object[]) obj;
    for ( int i = 0; i < newobj.length; i++ )
      ((Counter)newobj[i]).increment();
    return newobj;
  }
  protected void noteUpdate() {
    this.counters.increment( DLockBlackboard.ArrayOfObjectUpdates );
  }
  public void validate( Region region, Info info ) {
    Object[] obj = (Object[]) CacheUtil.get( region, info.getName() );
    for ( int i = 0; i < obj.length; i++ )
      if ( ((Counter)obj[i]).value() != info.getUpdates() )
        throw new DLockTestException( "Object data is invalid for " + info.getName() +
                                         " with expected count " + info.getUpdates() + ": " + obj );
  }
}
