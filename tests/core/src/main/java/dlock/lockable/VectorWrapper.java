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
import com.gemstone.gemfire.internal.NanoTimer;

import java.util.Vector;

import util.CacheUtil;
import dlock.*;


/**
 *  Lockable wrapper for cached Vector.
 */

public class VectorWrapper extends BaseWrapper {

  public VectorWrapper() {
    super();
  }
  public void createDataInCache( Region region, Object name ) {
    Vector obj = new Vector();
    CacheUtil.put( region, name, obj );
  }
  public void read( Region region, Object name ) {
    Vector obj = (Vector) CacheUtil.get( region, name );
    this.counters.increment( DLockBlackboard.VectorReads );
  }
  protected Object update( Object obj ) {
    Vector newobj = (Vector) obj;
    newobj.add( String.valueOf( NanoTimer.getTime() ) );
    return newobj;
  }
  protected void noteUpdate() {
    this.counters.increment( DLockBlackboard.VectorUpdates );
  }
  public void validate( Region region, Info info ) {
    Vector obj = (Vector) CacheUtil.get( region, info.getName() );
    if ( obj.size() != info.getUpdates() )
      throw new DLockTestException( "Object data is invalid for " + info.getName() +
                                       " with expected count " + info.getUpdates() + ": " + obj );
  }
}
