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

import hydra.HydraInternalException;
import hydra.HydraRuntimeException;
import hydra.Log;

import com.gemstone.gemfire.LogWriter;

import java.rmi.*;
//import java.rmi.registry.*;
//import java.rmi.server.*;
//import java.util.*;

/**
*
* Facade over a transport-specific implementation of shared counters
* ({@link RmiSharedCountersImpl}).
*
*/

public class AnySharedCountersImpl implements SharedCounters {

  private String    name;
  private int       type = -1;
  private LogWriter log;

  private RmiSharedCounters     rmicounters;

  /**
   *  Create a facade with the specified name and transport type.
   *  @param name the name of the set of shared counters.
   *  @param type the transport type of the set of shared counters.
   */
  public AnySharedCountersImpl( String name, int type ) {
    this.name = name;
    this.type = type;
    this.log  = Log.getLogWriter();
  }
  protected void setRmiCounters( RmiSharedCounters rmisc ) {
    this.rmicounters = rmisc;
  }
  /**
   *  Binds a fixed number of shared counters with the specified initial
   *  values in the location appropriate to the transport type
   *  (rmiregistry for RMI).
   *  @param initialValues the initial values for the counters.
   */
  public static SharedCounters bind( String name, int type, long[] initialValues ) {
    switch( type ) {
      case Blackboard.RMI:
        try {
          RmiSharedCounters rmisc = new RmiSharedCountersImpl( initialValues );
          RmiSharedCountersImpl.bind( name, rmisc );
          AnySharedCountersImpl asci = new AnySharedCountersImpl( name, type );
          asci.setRmiCounters( rmisc );
          return asci;
        } catch( RemoteException e ) {
          throw new HydraRuntimeException( "Unable to bind RMI counters: " + name, e );
        }
      default: throw new HydraInternalException( "Illegal transport type: " + type );
    }
  }
  /**
   *  Looks up the shared counters in the location appropriate to the transport type
   *  (rmiregistry for RMI).
   *  @return the shared counters or <code>null</code> if not there.
   */
  public static SharedCounters lookup( String name, int type ) {
    switch( type ) {
      case Blackboard.RMI:
        RmiSharedCounters rmisc = null;
        try {
          rmisc = RmiSharedCountersImpl.lookup( name );
        } catch( RemoteException e ) {
          throw new HydraRuntimeException( "Unable to look up RMI counters: " + name, e );
        }
        if ( rmisc == null )
          return null;
        else {
          AnySharedCountersImpl asci = new AnySharedCountersImpl( name, type );
          asci.setRmiCounters( rmisc );
          return asci;
        }
      default: throw new HydraInternalException( "Illegal transport type: " + type );
    }
  }

////////////////////////////////////////////////////////////////////////////////
////                        SHARED COUNTER INTERFACE                       /////
////////////////////////////////////////////////////////////////////////////////

  /**
   *  Implements {@link SharedCounters#read(int)}.
   */
  public long read( int index ) {
    switch( this.type ) {
      case Blackboard.RMI:
        try {
          return this.rmicounters.read( index );
        } catch( RemoteException e ) {
          throw new HydraRuntimeException( "Unable to access RMI counters: " + this.name, e );
        }
      default: throw new HydraInternalException( "Illegal transport type: " + this.type );
    }
  }
  /**
   *  Implements {@link SharedCounters#add(int,long)}.
   */
  public long add( int index, long i ) {
    switch( this.type ) {
      case Blackboard.RMI:
        try {
          return this.rmicounters.add( index, i );
        } catch( RemoteException e ) {
          throw new HydraRuntimeException( "Unable to access RMI counters: " + this.name, e );
        }
      default: throw new HydraInternalException( "Illegal transport type: " + this.type );
    }
  }
  /**
   *  Implements {@link SharedCounters#subtract(int,long)}.
   */
  public long subtract( int index, long i ) {
    switch( this.type ) {
      case Blackboard.RMI:
        try {
          return this.rmicounters.subtract( index, i );
        } catch( RemoteException e ) {
          throw new HydraRuntimeException( "Unable to access RMI counters: " + this.name, e );
        }
      default: throw new HydraInternalException( "Illegal transport type: " + this.type );
    }
  }
  /**
   *  Implements {@link SharedCounters#increment(int)}.
   */
  public void increment( int index ) {
    switch( this.type ) {
      case Blackboard.RMI:
        try {
          this.rmicounters.increment( index );
        } catch( RemoteException e ) {
          throw new HydraRuntimeException( "Unable to access RMI counters: " + this.name + " " + this.rmicounters, e );
        }
        break;
      default: throw new HydraInternalException( "Illegal transport type: " + this.type );
    }
  }
  /**
   *  Implements {@link SharedCounters#decrement(int)}.
   */
  public void decrement( int index ) {
    switch( this.type ) {
      case Blackboard.RMI:
        try {
          this.rmicounters.decrement( index );
        } catch( RemoteException e ) {
          throw new HydraRuntimeException( "Unable to access RMI counters: " + this.name, e );
        }
        break;
      default: throw new HydraInternalException( "Illegal transport type: " + this.type );
    }
  }
  /**
   *  Implements {@link SharedCounters#incrementAndRead(int)}.
   */
  public long incrementAndRead( int index ) {
    switch( this.type ) {
      case Blackboard.RMI:
        try {
          return this.rmicounters.incrementAndRead( index );
        } catch( RemoteException e ) {
          throw new HydraRuntimeException( "Unable to access RMI counters: " + this.name, e );
        }
      default: throw new HydraInternalException( "Illegal transport type: " + this.type );
    }
  }
  /**
   *  Implements {@link SharedCounters#decrementAndRead(int)}.
   */
  public long decrementAndRead( int index ) {
    switch( this.type ) {
      case Blackboard.RMI:
        try {
          return this.rmicounters.decrementAndRead( index );
        } catch( RemoteException e ) {
          throw new HydraRuntimeException( "Unable to access RMI counters: " + this.name, e );
        }
      default: throw new HydraInternalException( "Illegal transport type: " + this.type );
    }
  }
  /**
   *  Implements {@link SharedCounters#zero(int)}.
   */
  public void zero( int index ) {
    switch( this.type ) {
      case Blackboard.RMI:
        try {
          this.rmicounters.zero( index );
        } catch( RemoteException e ) {
          throw new HydraRuntimeException( "Unable to access RMI counters: " + this.name, e );
        }
        break;
      default: throw new HydraInternalException( "Illegal transport type: " + this.type );
    }
  }
  /**
   *  Implements {@link SharedCounters#setIfLarger(int,long)}.
   */
  public void setIfLarger( int index, long i ) {
    switch( this.type ) {
      case Blackboard.RMI:
        try {
          this.rmicounters.setIfLarger( index, i );
        } catch( RemoteException e ) {
          throw new HydraRuntimeException( "Unable to access RMI counters: " + this.name, e );
        }
        break;
      default: throw new HydraInternalException( "Illegal transport type: " + this.type );
    }
  }
  /**
   *  Implements {@link SharedCounters#setIfSmaller(int,long)}.
   */
  public void setIfSmaller( int index, long i ) {
    switch( this.type ) {
      case Blackboard.RMI:
        try {
          this.rmicounters.setIfSmaller( index, i );
        } catch( RemoteException e ) {
          throw new HydraRuntimeException( "Unable to access RMI counters: " + this.name, e );
        }
        break;
      default: throw new HydraInternalException( "Illegal transport type: " + this.type );
    }
  }
  /**
   *  Implements {@link SharedCounters#getCounterValues}.
   */
  public long[] getCounterValues() {
    switch( this.type ) {
      case Blackboard.RMI:
        try {
          return this.rmicounters.getCounterValues();
        } catch( RemoteException e ) {
          throw new HydraRuntimeException( "Unable to access RMI counters: " + this.name, e );
        }
      default: throw new HydraInternalException( "Illegal transport type: " + this.type );
    }
  }
}
