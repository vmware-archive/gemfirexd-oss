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

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.*;

/**
 *  The RMI interface for {@link SharedMap}.
 */
public interface RmiSharedMap extends Remote {

  /**
   *  RMI version of {@link SharedMap#size}.
   */
  public int size() throws RemoteException;

  /**
   *  RMI version of {@link SharedMap#isEmpty}.
   */
  public boolean isEmpty() throws RemoteException;

  /**
   *  RMI version of {@link SharedMap#containsKey(Object)}.
   */
  public boolean containsKey(Object key) throws RemoteException;

  /**
   *  RMI version of {@link SharedMap#containsValue(Object)}.
   */
  public boolean containsValue(Object value) throws RemoteException;

  /**
   *  RMI version of {@link SharedMap#get(Object)}.
   */
  public Object get(Object key) throws RemoteException;

  /**
   *  RMI version of {@link SharedMap#put(Object, Object)}.
   */
  public Object put(Object key, Object value) throws RemoteException;

  /**
   *  RMI version of {@link SharedMap#putIfLarger(Object, long)}.
   */
  public long putIfLarger(Object key, long value) throws RemoteException;

  /**
   *  RMI version of {@link SharedMap#putIfSmaller(Object, long)}.
   */
  public long putIfSmaller(Object key, long value) throws RemoteException;

  /**
   *  RMI version of {@link SharedMap#remove(Object)}.
   */
  public Object remove(Object key) throws RemoteException;

  /**
   *  RMI version of {@link SharedMap#putAll(Map)}.
   */
  public void putAll(Map t) throws RemoteException;

  /**
   *  RMI version of {@link SharedMap#clear}.
   */
  public void clear() throws RemoteException;

  /**
   *  RMI version of {@link SharedMap#getMap}.
   */
  public Map getMap() throws RemoteException;

  /**
   *  RMI version of {@link SharedMap#getRandomKey}.
   */
  public Object getRandomKey() throws RemoteException;

  /**
   *  RMI version of {@link SharedMap#replace}.
   */
  public boolean replace(Object key, Object expectedValue,
                         Object newValue) throws RemoteException;

}
