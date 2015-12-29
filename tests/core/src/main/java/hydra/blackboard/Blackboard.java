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

import hydra.*;

import com.gemstone.gemfire.LogWriter;

import java.lang.reflect.*;
import java.rmi.RemoteException;
import java.util.*;

/**
 *
 * A class used for communication between vms.
 *
 */

public abstract class Blackboard {

  public static final int RMI      = 0;
  public static final int GemFire  = 1;

  private String    name;
  private int       type = -1;
  private LogWriter log;

  private SharedCounters sharedCounters;
  private String         sharedCountersName;
  protected String[]     counterNames;

  /** Maps a <code>Class</code> to the initial values of its counters
   * (an array of <code>long</code>) */
  private static Map initialCounterValues = new HashMap();

  private SharedMap      sharedMap;
  private String         sharedMapName;

  private SharedLock     sharedLock;
  private String         sharedLockName;

  public Blackboard() {
  }
  /**
   *  Creates an empty blackboard with the specified name and transport type
   *  ("RMI" or "GemFire").
   *  @param name the name of the blackboard.
   *  @param type the transport type of the blackboard.
   *  @param cls
   *         The class from which counter information is extracted
   *
   * @throws IllegalArgumentException if the type is invalid.  */
  public Blackboard( String name, String type, Class cls ) {
    this( name, typeStringToInt( type ), cls );
  }

  /**
   *  Creates an empty blackboard with the specified name and transport type
   *  ("RMI" or "GemFire").
   *  @param name the name of the blackboard.
   *  @param type the transport type of the blackboard.
   *  @param cls
   *         The class from which counter information is extracted
   *
   * @throws IllegalArgumentException if the type is invalid.
   *
   * @since 3.5
   */
  public Blackboard( String name, int type, Class cls ) {
    this.name = name;
    this.sharedCountersName = name + "SharedCounters";
    this.sharedMapName      = name + "SharedMap";
    this.sharedLockName     = name + "SharedLock";
    this.type = type;
    this.log = Log.getLogWriter();
    setValues( cls, this );
  }

  /**
   *  Creates a fixed number of shared counters and stores them in a location
   *  appropriate to the blackboard's transport type (rmiregistry for RMI and
   *  namespace for GemFire).
   *
   *  @param name the blackboard name.
   *  @param type the transport type.
   * @param cnames
   *        The names of the shared counters
   * @param cvals
   *        The initial value of the shared counters
   */
  public SharedCounters createSharedCounters( String name, String type, String[] cnames, long[] cvals ) {
    this.counterNames = cnames;

    initialCounterValues.put(this.getClass(), cvals);

    SharedCounters sharedCounters = AnySharedCountersImpl.lookup( name + "SharedCounters", typeStringToInt( type ) );
    if ( sharedCounters == null )
      sharedCounters = AnySharedCountersImpl.bind( name + "SharedCounters", typeStringToInt( type ), cvals );
    return sharedCounters;
  }

  /**
   *  Gets the shared counters associated with this blackboard.  Lazily creates them.
   *  @return the shared counters.
   */
  public SharedCounters getSharedCounters() {
    if ( this.sharedCounters == null ) {
      this.sharedCounters = AnySharedCountersImpl.lookup( this.sharedCountersName, this.type );
      if ( this.sharedCounters == null ) {
        // @todo lises need only do this on master if rmi, so branch here on type
        String classname = this.getClass().getName();
        String methodname = "createSharedCounters";
        long[] initValues =
          (long[]) initialCounterValues.get(this.getClass());
        Class[] types = new Class[4]; types[0] = String.class;
                                      types[1] = String.class;
                                      types[2] = String[].class;
                                      types[3] = long[].class;
        Object[] args = new Object[4]; args[0] = this.name;
                                       args[1] = typeIntToString( this.type);
                                       args[2] = counterNames;
                                       args[3] = initValues;
        invoke(classname, methodname, types, args);
      }
      this.sharedCounters = AnySharedCountersImpl.lookup( this.sharedCountersName, this.type );
    }
    return this.sharedCounters;
  }
  /**
   *  Returns the index of the sharedCounter for the given shared counter name.
   *
   *  @param sharedCounterName - the name of the shared counter.
   *
   *  @return The index for the sharedCounter with name sharedCounterName.
   */
  public int getSharedCounter(String sharedCounterName) {
    for ( int i = 0; i < counterNames.length; i++ ) {
      String name = counterNames[i];
      if (name.equals(sharedCounterName)) {
         return i;
      }
    }
    throw new HydraRuntimeException( "Unable to find shared counter with name " + sharedCounterName);
  }
  /**
   *  Prints the contents of the shared counters using the "info" log level.
   */
  public void printSharedCounters() {
    // @todo lises print "not exist yet" if null, delegate print to any guy
    long[] counterValues = getSharedCounters().getCounterValues();
    if ( counterValues.length != counterNames.length )
      throw new IllegalArgumentException( "Number of names not equal to number of counters" );
    StringBuffer buf = new StringBuffer( counterValues.length * 20 );
    buf.append( this.name ).append( "SharedCounters:" ).append( "\n" );
    for ( int i = 0; i < counterValues.length; i++ ) {
      String name = counterNames[i];
      if ( name == null )
        throw new IllegalArgumentException( "Null counter name at index " + i );
      buf.append( " " ).append( name ).append( "=" ).append( counterValues[i] ).append( "\n" );
    }
    log.info( buf.toString() );
  }

  /**
   *  Creates an empty shared map and stores it in a location appropriate
   *  to the blackboard's transport type (rmiregistry for RMI and namespace
   *  for GemFire).
   *  @param name the blackboard name.
   *  @param type the transport type.
   */
  public SharedMap createSharedMap( String name, String type ) {
    SharedMap sharedMap = AnySharedMapImpl.lookup( name + "SharedMap", typeStringToInt( type ) );
    if ( sharedMap == null )
      sharedMap = AnySharedMapImpl.bind( name + "SharedMap", typeStringToInt( type ) );
    return sharedMap;
  }
  /**
   *  Prints the contents of the blackboard.
   */
  public void print() {
    printSharedCounters();
    printSharedMap();
  }
  /**
   *  Gets the shared map associated with this blackboard.  Lazily creates it.
   *  @return the shared map.
   */
  public SharedMap getSharedMap() {
    if ( this.sharedMap == null ) {
      this.sharedMap = AnySharedMapImpl.lookup( this.sharedMapName, this.type );
      if ( this.sharedMap == null ) {
        // @todo lises need only do this on master if rmi, so branch here on type
        String classname = this.getClass().getName();
        String methodname = "createSharedMap";
        Class[] types = new Class[2]; types[0] = String.class; types[1] = String.class;
        Object[] args = new Object[2]; args[0] = this.name; args[1] = typeIntToString( this.type);
        try {
          if ( RemoteTestModule.Master == null )
            MasterController.invoke( classname, methodname, types, args );
          else
            RemoteTestModule.Master.invoke( classname, methodname, types, args );
        } catch( RemoteException e ) {
          throw new HydraRuntimeException( "Unable to invoke " + classname + "." + methodname, e );
        }
      }
      this.sharedMap = AnySharedMapImpl.lookup( this.sharedMapName, this.type );
    }
    return this.sharedMap;
  }
  /**
   *  Prints the contents of the shared map.  Uses the "info" log level.
   */
  public void printSharedMap() {
    // @todo lises print "not exist yet" if null, delegate print to any guy
    Map map = getSharedMap().getMap();
    StringBuffer buf = new StringBuffer( map.size() * 20 );
    buf.append( this.name ).append( "SharedMap:" );
    for ( Iterator i = map.keySet().iterator(); i.hasNext(); ) {
      Object key = i.next();
      Object val = map.get( key );
      buf.append( " " ).append( key ).append( "=" ).append( val ).append('\n');
    }
    log.info( buf.toString() );
  }
  private static int typeStringToInt( String type ) {
    if ( type.equalsIgnoreCase( "RMI" ) )          return RMI;
    else if ( type.equalsIgnoreCase( "GemFire" ) ) return GemFire;
    else throw new IllegalArgumentException( "Illegal transport type: " + type );
  }
  private static String typeIntToString( int type ) {
    switch( type ) {
      case RMI:     return "RMI";
      case GemFire: return "Gemfire";
      default:      throw new IllegalArgumentException( "Illegal transport type: " + type );
    }
  }

  /**
   * Examines the given <code>Class</code> using reflect and creates
   * shared counters from each.  Each <code>public</code>
   * <code>static</code> field of type <code>int</code> is considered
   * to be a counter.  The original value of the counter field is used
   * as the initial value of the counter (new in 2.0.3).  The field's
   * value is set to the "counter number" of the counter.
   */
  private synchronized static void setValues( Class cls, Blackboard bb ) {
    int counternum = 0;
    Field[] fields = cls.getDeclaredFields();
    for ( int i = 0; i < fields.length; i++ ) {
      String fieldname = fields[i].getName();
      if ( fields[i].getDeclaringClass() != cls )
        throw new HydraInternalException( "Unexpected declarer" );
      if ( fields[i].getType() == Integer.TYPE ) {
        int m = fields[i].getModifiers();
        if ( Modifier.isPublic( m ) && Modifier.isStatic( m ) ) {
          fields[i].setAccessible( true );
          ++counternum;
        }
      }
    }
    bb.counterNames = new String[ counternum ];
    long[] initValues = new long[ counternum ];
    counternum = 0;
    for ( int i = 0; i < fields.length; i++ ) {
      if ( fields[i].getType() == Integer.TYPE ) {
        int m = fields[i].getModifiers();
        if ( Modifier.isPublic( m ) && Modifier.isStatic( m ) ) {
          long value;
          try {
            value = fields[i].getLong(null);
            bb.counterNames[counternum] = fields[i].getName();
            fields[i].setInt( null, counternum );

          } catch( IllegalAccessException e ) {
            e.printStackTrace();
            throw new HydraRuntimeException( "Error accessing field", e );
          }

          if ( fields[i].getName().startsWith( "Max" ) )
            initValues[counternum] = Long.MIN_VALUE;
          else if ( fields[i].getName().startsWith( "Min" ) )
            initValues[counternum] = Long.MAX_VALUE;
          else
            initValues[counternum] = value;
          ++counternum;
        }
      }
    }

    if (!initialCounterValues.containsKey(cls)) {
      // Only set the initial values once
      initialCounterValues.put(cls, initValues);
    }
  }

  /**
   *  Returns an array of shared counter names for this blackboard.
   *
   */
  public String[] getCounterNames() {
    return counterNames;
  }

  /**
   *  Creates the shared lock and stores it in a location appropriate
   *  to the blackboard's transport type (rmiregistry for RMI).
   *  @param name the blackboard name.
   *  @param type the transport type.
   */
  public SharedLock createSharedLock(String name, String type) {
    SharedLock sharedLock =
      AnySharedLockImpl.lookup(name + "SharedLock", typeStringToInt(type));
    if (sharedLock == null)
      sharedLock =
        AnySharedLockImpl.bind(name + "SharedLock", typeStringToInt(type));
    return sharedLock;
  }

  /**
   *  Gets the shared lock associated with this blackboard.  Lazily creates it.
   *  @return the shared lock.
   */
  public SharedLock getSharedLock() {
    if (this.sharedLock == null) {
      this.sharedLock =
        AnySharedLockImpl.lookup(this.sharedLockName, this.type);
      if (this.sharedLock == null) {
        // @todo lises need only do this on master if rmi, so branch on type
        String classname = this.getClass().getName();
        String methodname = "createSharedLock";

        Class[] types = new Class[2];
        types[0] = String.class; types[1] = String.class;

        Object[] args = new Object[2];
        args[0] = this.name; args[1] = typeIntToString(this.type);
        try {
          if ( RemoteTestModule.Master == null )
            MasterController.invoke( classname, methodname, types, args );
          else
            RemoteTestModule.Master.invoke(classname, methodname, types, args);
        } catch(RemoteException e) {
          String s = "Unable to invoke " + classname + "." + methodname;
          throw new HydraRuntimeException(s, e);
        }
      }
      this.sharedLock =
        AnySharedLockImpl.lookup(this.sharedLockName, this.type);
    }
    return this.sharedLock;
  }

  private void invoke(String classname, String methodname,
                      Class[] types, Object[] args) {
    try {
      MasterProxyIF master = RemoteTestModule.Master;
      if (master == null) {
        master = RmiRegistryHelper.lookupMaster();
      }
      master.invoke(classname, methodname, types, args);
    }
    catch (RemoteException e) {
      String s = "Unable to invoke " + classname + "." + methodname;
      throw new HydraRuntimeException(s, e);
    }
  }
}
