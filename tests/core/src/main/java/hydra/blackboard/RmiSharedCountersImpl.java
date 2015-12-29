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

package hydra.blackboard;

import com.gemstone.gemfire.LogWriter;

import hydra.*;

import java.rmi.*;
import java.rmi.server.*;

/**
*
* Maintains a set of remotely shared thread-safe counters. 
*
*/

public class RmiSharedCountersImpl extends UnicastRemoteObject implements RmiSharedCounters {

  private SharedCounters counters;

  public RmiSharedCountersImpl( long[] initialValues ) throws RemoteException {
    super();
    this.counters = new SharedCountersImpl( initialValues );
  }
  protected static void bind( String name, RmiSharedCounters value ) {
    RmiRegistryHelper.bindInMaster(name, value);
    if (log().finerEnabled()) {
      log().finer( "Bound " + name + "=" + value + " into master registry" );
    }
  }
  protected static RmiSharedCounters lookup( String name ) throws RemoteException {
    if (log().finerEnabled()) {
      log().finer( "Looking up " + name + " in master registry" );
    }
    return (RmiSharedCounters)RmiRegistryHelper.lookupInMaster(name);
  }
  private static LogWriter log() {
    return Log.getLogWriter();
  }
   
////////////////////////////////////////////////////////////////////////////////
////                        SHARED COUNTER INTERFACE                       /////
////////////////////////////////////////////////////////////////////////////////

  /**
   *  Implements {@link RmiSharedCounters#read(int)}.
   */
  public long read( int index ) throws RemoteException {
    synchronized( counters ) {
      return counters.read( index );
    }
  }
  /**
   *  Implements {@link RmiSharedCounters#add(int,long)}.
   */
  public long add( int index, long i ) throws RemoteException {
    synchronized( counters ) {
      return counters.add( index, i );
    }
  }
  /**
   *  Implements {@link RmiSharedCounters#subtract(int,long)}.
   */
  public long subtract( int index, long i ) throws RemoteException {
    synchronized( counters ) {
      return counters.subtract( index, i );
    }
  }
  /**
   *  Implements {@link RmiSharedCounters#increment(int)}.
   */
  public void increment( int index ) throws RemoteException {
    synchronized( counters ) {
      counters.increment( index );
    }
  }
  /**
   *  Implements {@link RmiSharedCounters#decrement(int)}.
   */
  public void decrement( int index ) throws RemoteException {
    synchronized( counters ) {
      counters.decrement( index );
    }
  }
  /**
   *  Implements {@link RmiSharedCounters#incrementAndRead(int)}.
   */
  public long incrementAndRead( int index ) throws RemoteException {
    synchronized( counters ) {
      return counters.incrementAndRead( index );
    }
  }
  /**
   *  Implements {@link RmiSharedCounters#decrementAndRead(int)}.
   */
  public long decrementAndRead( int index ) throws RemoteException {
    synchronized( counters ) {
      return counters.decrementAndRead( index );
    }
  }
  /**
   *  Implements {@link RmiSharedCounters#zero(int)}.
   */
  public void zero( int index ) throws RemoteException {
    synchronized( counters ) {
      counters.zero( index );
    }
  }
  /**
   *  Implements {@link RmiSharedCounters#setIfLarger(int,long)}.
   */
  public void setIfLarger( int index, long i ) throws RemoteException {
    synchronized( counters ) {
      counters.setIfLarger( index, i );
    }
  }
  /**
   *  Implements {@link RmiSharedCounters#setIfSmaller(int,long)}.
   */
  public void setIfSmaller( int index, long i ) throws RemoteException {
    synchronized( counters ) {
      counters.setIfSmaller( index, i );
    }
  }
  /**
   *  Implements {@link RmiSharedCounters#getCounterValues}.
   */
  public long[] getCounterValues() throws RemoteException {
    synchronized( counters ) {
      return counters.getCounterValues();
    }
  }
}
