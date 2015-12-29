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
import java.util.*;

/**
*
* Facade over a transport-specific implementation of a shared map
* ({@link RmiSharedMapImpl}).
*
*/

public class AnySharedMapImpl implements SharedMap {

  private String    name;
  private int       type = -1;
  private LogWriter log;

  private RmiSharedMap     rmimap;

  /**
   *  Create a facade with the specified name and transport type.
   *  @param name the name of the map.
   *  @param type the transport type of the map.
   */
  public AnySharedMapImpl( String name, int type ) {
    this.name = name;
    this.type = type;
    this.log  = Log.getLogWriter();
  }
  protected void setRmiMap( RmiSharedMap rmim ) {
    this.rmimap = rmim;
  }
  /**
   *  Binds a shared map in the location appropriate to the transport type
   *  (rmiregistry for RMI).
   */
  public static SharedMap bind( String name, int type ) {
    switch( type ) {
      case Blackboard.RMI:
        try {
          RmiSharedMap rmim = new RmiSharedMapImpl();
          RmiSharedMapImpl.bind( name, rmim );
          AnySharedMapImpl asmi = new AnySharedMapImpl( name, type );
          asmi.setRmiMap( rmim );
          return asmi;
        } catch( RemoteException e ) {
          throw new HydraRuntimeException( "Unable to bind RMI map: " + name, e );
        }
      default: throw new HydraInternalException( "Illegal transport type: " + type );
    }
  }
  /**
   *  Looks up the shared map in the location appropriate to the transport type
   *  (rmiregistry for RMI).
   *  @return the shared map or <code>null</code> if not there.
   */
  public static SharedMap lookup( String name, int type ) {
    switch( type ) {
      case Blackboard.RMI:
        RmiSharedMap rmim = null;
        try {
          rmim = RmiSharedMapImpl.lookup( name );
        } catch( RemoteException e ) {
          throw new HydraRuntimeException( "Unable to look up RMI map: " + name, e );
        }
        if ( rmim == null )
          return null;
        else {
          AnySharedMapImpl asmi = new AnySharedMapImpl( name, type );
          asmi.setRmiMap( rmim );
          return asmi;
        }
      default: throw new HydraInternalException( "Illegal transport type: " + type );
    }
  }

////////////////////////////////////////////////////////////////////////////////
////                          SHARED MAP INTERFACE                         /////
////////////////////////////////////////////////////////////////////////////////

  /**
   *  Implements {@link SharedMap#size}.
   */
  public int size() {
    switch( this.type ) {
      case Blackboard.RMI:
        try {
          return this.rmimap.size();
        } catch( RemoteException e ) {
          throw new HydraRuntimeException( "Unable to access RMI map: " + this.name, e );
        }
      default: throw new HydraInternalException( "Illegal transport type: " + this.type );
    }
  }
  /**
   *  Implements {@link SharedMap#isEmpty}.
   */
  public boolean isEmpty() {
    switch( this.type ) {
      case Blackboard.RMI:
        try {
          return this.rmimap.isEmpty();
        } catch( RemoteException e ) {
          throw new HydraRuntimeException( "Unable to access RMI map: " + this.name, e );
        }
      default: throw new HydraInternalException( "Illegal transport type: " + this.type );
    }
  }
  /**
   *  Implements {@link SharedMap#containsKey(Object)}.
   */
  public boolean containsKey(Object key) {
    switch( this.type ) {
      case Blackboard.RMI:
        try {
          return this.rmimap.containsKey( key );
        } catch( RemoteException e ) {
          throw new HydraRuntimeException( "Unable to access RMI map: " + this.name, e );
        }
      default: throw new HydraInternalException( "Illegal transport type: " + this.type );
    }
  }
  /**
   *  Implements {@link SharedMap#containsValue(Object)}.
   */
  public boolean containsValue(Object value) {
    switch( this.type ) {
      case Blackboard.RMI:
        try {
          return this.rmimap.containsValue( value );
        } catch( RemoteException e ) {
          throw new HydraRuntimeException( "Unable to access RMI map: " + this.name, e );
        }
      default: throw new HydraInternalException( "Illegal transport type: " + this.type );
    }
  }
  /**
   *  Implements {@link SharedMap#get(Object)}.
   */
  public Object get(Object key) {
    switch( this.type ) {
      case Blackboard.RMI:
        try {
          return this.rmimap.get( key );
        } catch( RemoteException e ) {
          throw new HydraRuntimeException( "Unable to access RMI map: " + this.name, e );
        }
      default: throw new HydraInternalException( "Illegal transport type: " + this.type );
    }
  }
  /**
   *  Implements {@link SharedMap#put(Object, Object)}.
   */
  public Object put(Object key, Object value) {
    switch( this.type ) {
      case Blackboard.RMI:
        try {
          return this.rmimap.put( key, value );
        } catch( RemoteException e ) {
          throw new HydraRuntimeException( "Unable to access RMI map: " + this.name, e );
        }
      default: throw new HydraInternalException( "Illegal transport type: " + this.type );
    }
  }
  /**
   *  Implements {@link SharedMap#putIfLarger(Object, long)}.
   */
  public long putIfLarger(Object key, long value) {
    switch( this.type ) {
      case Blackboard.RMI:
        try {
          return this.rmimap.putIfLarger( key, value );
        } catch( RemoteException e ) {
          throw new HydraRuntimeException( "Unable to access RMI map: " + this.name, e );
        }
      default: throw new HydraInternalException( "Illegal transport type: " + this.type );
    }
  }
  /**
   *  Implements {@link SharedMap#putIfSmaller(Object, long)}.
   */
  public long putIfSmaller(Object key, long value) {
    switch( this.type ) {
      case Blackboard.RMI:
        try {
          return this.rmimap.putIfSmaller( key, value );
        } catch( RemoteException e ) {
          throw new HydraRuntimeException( "Unable to access RMI map: " + this.name, e );
        }
      default: throw new HydraInternalException( "Illegal transport type: " + this.type );
    }
  }
  /**
   *  Implements {@link SharedMap#remove(Object)}.
   */
  public Object remove(Object key) {
    switch( this.type ) {
      case Blackboard.RMI:
        try {
          return this.rmimap.remove( key );
        } catch( RemoteException e ) {
          throw new HydraRuntimeException( "Unable to access RMI map: " + this.name, e );
        }
      default: throw new HydraInternalException( "Illegal transport type: " + this.type );
    }
  }
  /**
   *  Implements {@link SharedMap#putAll(Map)}.
   */
  public void putAll(Map t) {
    switch( this.type ) {
      case Blackboard.RMI:
        try {
          this.rmimap.putAll( t );
          break;
        } catch( RemoteException e ) {
          throw new HydraRuntimeException( "Unable to access RMI map: " + this.name, e );
        }
      default: throw new HydraInternalException( "Illegal transport type: " + this.type );
    }
  }
  /**
   *  Implements {@link SharedMap#clear}.
   */
  public void clear() {
    switch( this.type ) {
      case Blackboard.RMI:
        try {
          this.rmimap.clear();
        } catch( RemoteException e ) {
          throw new HydraRuntimeException( "Unable to access RMI map: " + this.name, e );
        }
        break;
      default: throw new HydraInternalException( "Illegal transport type: " + this.type );
    }
  }
  /**
   *  Implements {@link SharedMap#getMap}.
   */
  public Map getMap() {
    switch( this.type ) {
      case Blackboard.RMI:
        try {
          return this.rmimap.getMap();
        } catch( RemoteException e ) {
          throw new HydraRuntimeException( "Unable to access RMI map: " + this.name, e );
        }
      default: throw new HydraInternalException( "Illegal transport type: " + this.type );
    }
  }

  /**
   * Implements {@link SharedMap#getRandomKey}.
   *
   * @since 2.0.3
   */
  public Object getRandomKey() {
    switch( this.type ) {
      case Blackboard.RMI:
        try {
          return this.rmimap.getRandomKey();
        } catch( RemoteException e ) {
          throw new HydraRuntimeException( "Unable to access RMI map: " + this.name, e );
        }
      default: throw new HydraInternalException( "Illegal transport type: " + this.type );
    }
  }

  /**
   * Implements {@link SharedMap#replace}.
   *
   * @since 2.0.3
   */
  public boolean replace(Object key, Object expectedValue,
                         Object newValue) {
    switch( this.type ) {
      case Blackboard.RMI:
        try {
          return this.rmimap.replace(key, expectedValue, newValue);
        } catch( RemoteException e ) {
          throw new HydraRuntimeException( "Unable to access RMI map: " + this.name, e );
        }
      default: throw new HydraInternalException( "Illegal transport type: " + this.type );
    }
  }
}
