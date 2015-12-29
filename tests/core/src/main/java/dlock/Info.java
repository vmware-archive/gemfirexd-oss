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

package dlock;

import hydra.*;
import java.io.Serializable;

/**
 *  Information about a cached object from a single client's point of view.
 */

public class Info implements Serializable {

  private Object name;      // the name of the object in the cache
  private Lockable wrapper; // the Lockable wrapper for this object
  private boolean locked;   // whether this client plans to lock the object
  private long locks;       // the number of times this client has locked the object
  private long unlocks;     // the number of times this client has unlocked the object
  private long reads;       // the number of times this client has read the object
  private long updates;     // the number of times this client has updated the object
  private boolean debugFine = false;  // Skip log message if it wont be displayed

  public Info( Object name, Lockable wrapper ) {
    this.name    = name;
    this.wrapper = wrapper;
    this.locked   = false;
    this.locks    = 0;
    this.unlocks = 0;
    this.reads   = 0;
    this.updates = 0;
    debugFine = Log.getLogWriter().fineEnabled();
  }
  public Object getName() {
    return this.name;
  }
  public Lockable getWrapper() {
    return this.wrapper;
  }
  public boolean isLocked() {
    return this.locked;
  }
  public void setLocked( boolean locked ) {
    this.locked = locked;
  }
  public long getLocks() {
    return this.locks;
  }
  public void incrementLocks() {
    ++this.locks;
    if ( this.debugFine ) Log.getLogWriter().fine("Details " + name + "; incremented locks: " + this.toString());
  }
  public long getUnlocks() {
    return this.unlocks;
  }
  public void incrementUnlocks() {
    ++this.unlocks;
    if ( this.debugFine ) Log.getLogWriter().fine("Details " + name + "; incremented unlocks: " + this.toString());
  }
  public long getReads() {
    return this.reads;
  }
  public void incrementReads() {
    ++this.reads;
    if ( this.debugFine ) Log.getLogWriter().fine("Details " + name + "; incremented reads: " + this.toString());
  }
  public long getUpdates() {
    return this.updates;
  }
  public void incrementUpdates() {
    ++this.updates;
    if ( this.debugFine ) Log.getLogWriter().fine("Details " + name + "; incremented updates: " + this.toString());
  }
  public void addInfo( Info info ) {

    if ( ! getName().equals( info.getName() ) )
      throw new DLockTestException( "Mismatched name!!!" );
    if ( ! getWrapper().getClass().getName().equals( info.getWrapper().getClass().getName() ) )
      throw new DLockTestException( "Mismatched wrapper!!! " + getWrapper().getClass().getName() + " != " + info.getWrapper().getClass().getName() );
    if ( isLocked() || info.isLocked() )
      throw new DLockTestException( "Object still locked!!!" );

    this.locks    += info.getLocks();
    this.unlocks += info.getUnlocks();
    this.reads   += info.getReads();
    this.updates += info.getUpdates();
    Log.getLogWriter().info("Info for name " + name + "; added info: " + this.toString());
  }
  public String toShortString() {
    return this.name + " (" + this.wrapper.getClass().getName() + ")";
  }
  public String toString() {
    return this.name + " locks=" + locks + " unlocks=" + unlocks +
                       " reads=" + reads + " updates=" + updates;
  }
}
