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

import com.gemstone.gemfire.*;
import com.gemstone.gemfire.cache.*;

import hydra.*;
import hydra.blackboard.*;

import java.io.Serializable;

import util.CacheUtil;
import dlock.*;

/**
 *  Abstract superclass for wrappers.
 */

public abstract class BaseWrapper implements Lockable, Serializable {

  protected transient SharedCounters counters;
  protected transient boolean debug = false;
  protected transient boolean debugFine = false;

  public BaseWrapper() {
    this.counters = DLockBlackboard.getInstance().getSharedCounters();
    this.debug = log().infoEnabled();
    this.debugFine = log().fineEnabled();
  }
  public abstract void createDataInCache( Region region, Object name );
  public void lock( Region aRegion, Object name ) {
//    if ( DLockUtil.hasLock( name ) )
//      throw new DLockTestException( "Attempt to lock already locked object " + name );
    if (TestConfig.tab().booleanAt(DLockPrms.useEntryLock)) {
       if ( this.debug ) log().info( "Getting entry lock on region " + aRegion.getName() + ", key " +name );
       DLockUtil.getEntryLock(aRegion, name);
       if ( this.debug ) log().info( "Got entry lock on region " + aRegion.getName() + ", key " +name );
    } else {
       if ( this.debug ) log().info( "Getting lock on " + name );
       DLockUtil.getLock( name );
       if ( this.debug ) log().info( "Got lock on " + name );
    }
    this.counters.increment( DLockBlackboard.hasLock );
  }
  public void unlock( Region aRegion, Object name ) {
    if (TestConfig.tab().booleanAt(DLockPrms.useEntryLock)) {
       if ( this.debug ) log().info( "Unlocking entry lock on region " + aRegion.getName() + ", key " + name );
       DLockUtil.unlockEntryLock( aRegion, name );
       if ( this.debug ) log().info( "Unlocked entry lock on region " + aRegion.getName() + ", key " + name );
    } else {
       if ( ! DLockUtil.hasLock( name ) )
         throw new DLockTestException( "Attempt to unlock unlocked object " + name );
       if ( this.debug ) log().info( "Unlocking lock on " + name );
       DLockUtil.unlock( name );
       if ( this.debugFine ) log().fine( "Unlocked " + name );
    }
    this.counters.increment( DLockBlackboard.hasNoLock );
  }
  public abstract void read( Region region, Object name );
  public void update( Region region, Object name ) {
    if ( this.debug ) log().info( "Updating " + name );
//    Exception ex = null;
    Object oldobj, newobj = null;
    synchronized( this ) {
       if ( this.debug ) log().info( "Getting " + name );
       oldobj = CacheUtil.get( region, name );
       if ( this.debugFine ) log().fine( "Got " + name + ": " + oldobj.getClass().getName() );
       newobj = this.update( oldobj );
       if ( this.debugFine ) log().fine( "New value for " + name + " is " + newobj.getClass().getName() );
       CacheUtil.put( region, name, newobj );
       if ( this.debug ) log().info( "Updated " + name + ": changed " + oldobj.getClass().getName() + " to " + newobj.getClass().getName() );
    }
    this.noteUpdate();
    return;
  }
  protected abstract Object update( Object obj );
  protected abstract void noteUpdate();
  public abstract void validate( Region region, Info info );
  public String toString( Region region, Object name ) {
    StringBuffer buf = new StringBuffer( 200 );
    buf.append( region.getName() + ":" + name )
       .append( " = " )
       .append( CacheUtil.get( region, name ) );
    return buf.toString();
  }
  protected LogWriter log() {
    return Log.getLogWriter();
  }
}
