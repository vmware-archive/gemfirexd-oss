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
import com.gemstone.gemfire.internal.Assert;

import hydra.*;

import java.rmi.*;
import java.rmi.server.*;
import java.util.*;

/**
*
* Maintains a remotely shared thread-safe map.
*
*/

public class RmiSharedMapImpl extends UnicastRemoteObject implements RmiSharedMap {

  private Map map;

  public RmiSharedMapImpl() throws RemoteException {
    super();
    this.map = new Hashtable();
  }
  protected static void bind( String name, RmiSharedMap value ) {
    RmiRegistryHelper.bindInMaster(name, value);
    if (log().finerEnabled()) {
      log().finer( "Bound " + name + "=" + value + " into master registry" );
    }
  }
  protected static RmiSharedMap lookup( String name ) throws RemoteException {
    if (log().finerEnabled()) {
      log().finer( "Looking up " + name + " in master registry" );
    }
    return (RmiSharedMap)RmiRegistryHelper.lookupInMaster(name);
  }
  private static LogWriter log() {
    return Log.getLogWriter();
  }
   
////////////////////////////////////////////////////////////////////////////////
////                          SHARED MAP INTERFACE                         /////
////////////////////////////////////////////////////////////////////////////////

  /**
   *  Implements {@link RmiSharedMap#size}.
   */
  public int size() throws RemoteException {
    return map.size();
  }
  /**
   *  Implements {@link RmiSharedMap#isEmpty}.
   */
  public boolean isEmpty() throws RemoteException {
    return map.isEmpty();
  }
  /**
   *  Implements {@link RmiSharedMap#containsKey(Object)}.
   */
  public boolean containsKey(Object key) throws RemoteException {
    return map.containsKey(key);
  }
  /**
   *  Implements {@link RmiSharedMap#containsValue(Object)}.
   */
  public boolean containsValue(Object value) throws RemoteException {
    return map.containsValue(value);
  }
  /**
   *  Implements {@link RmiSharedMap#get(Object)}.
   */
  public Object get(Object key) throws RemoteException {
    return map.get(key);
  }
  /**
   *  Implements {@link RmiSharedMap#put(Object, Object)}.
   */
  public Object put(Object key, Object value) throws RemoteException {
    return map.put(key,value);
  }
  /**
   *  Implements {@link RmiSharedMap#putIfLarger(Object, long)}.
   */
  public long putIfLarger(Object key, long value) throws RemoteException {
    synchronized( map ) {
      Long current = (Long)map.get(key);
      if ( current == null || current.longValue() < value ) {
        map.put(key, new Long(value));
        return value;
      } else {
        return current.longValue();
      }
    }
  }
  /**
   *  Implements {@link RmiSharedMap#putIfSmaller(Object, long)}.
   */
  public long putIfSmaller(Object key, long value) throws RemoteException {
    synchronized( map ) {
      Long current = (Long)map.get(key);
      if ( current == null || current.longValue() > value ) {
        map.put(key, new Long(value));
        return value;
      } else {
        return current.longValue();
      }
    }
  }
  /**
   *  Implements {@link RmiSharedMap#remove(Object)}.
   */
  public Object remove(Object key) throws RemoteException {
    return map.remove(key);
  }
  /**
   *  Implements {@link RmiSharedMap#putAll(Map)}.
   */
  public void putAll(Map t) throws RemoteException {
    map.putAll(t);
  }
  /**
   *  Implements {@link RmiSharedMap#clear}.
   */
  public void clear() throws RemoteException {
    map.clear();
  }
  /**
   *  Implements {@link RmiSharedMap#getMap}.
   */
  public Map getMap() throws RemoteException {
    // return a copy of the map, but get this for free with
    // rmi since it serializes the result (but is this true on master?)
    return map;
  }

  /**
   * While the map is locked, get the set of keys and select a random
   * element.
   *
   * author David Whitlock
   * @since 2.0.3
   */
  public Object getRandomKey() throws RemoteException {
    GsRandom random = TestConfig.tab().getRandGen();

    synchronized (this.map) {
      if (this.map.size() == 0) {
        return null;
      }

      Object[] array = this.map.keySet().toArray();
      Assert.assertTrue(array.length == this.map.size());
      int index = random.nextInt(array.length - 1);
      if (index >= array.length) {
        Assert.assertTrue(false, "Index is " + index +
                          ", but array length is " + array.length);
      }
      return array[index];
    }
  }

  public boolean replace(Object key, Object expectedValue,
                         Object newValue) throws RemoteException {
    synchronized (this.map) {
      Object currentValue = this.map.get(key);
      if (currentValue == null) {
        if (expectedValue != null) {
          return false;
        }

      } else if (!currentValue.equals(expectedValue)) {
        return false;
      }

      this.map.put(key, newValue);
      return true;
    }
  }

}
