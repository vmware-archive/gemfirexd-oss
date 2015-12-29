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

/**
 *  The RMI interface for {@link SharedCounters}.
 */
public interface RmiSharedCounters extends Remote {

  /**
   *  RMI version of {@link SharedCounters#read(int)}.
   */
  public long read( int index ) throws RemoteException;

  /**
   *  RMI version of {@link SharedCounters#add(int,long)}.
   */
  public long add( int index, long i ) throws RemoteException;

  /**
   *  RMI version of {@link SharedCounters#subtract(int,long)}.
   */
  public long subtract( int index, long i ) throws RemoteException;

  /**
   *  RMI version of {@link SharedCounters#increment(int)}.
   */
  public void increment( int index ) throws RemoteException;

  /**
   *  RMI version of {@link SharedCounters#decrement(int)}.
   */
  public void decrement( int index ) throws RemoteException;

  /**
   *  RMI version of {@link SharedCounters#incrementAndRead(int)}.
   */
  public long incrementAndRead( int index ) throws RemoteException;

  /**
   *  RMI version of {@link SharedCounters#decrementAndRead(int)}.
   */
  public long decrementAndRead( int index ) throws RemoteException;

  /**
   *  RMI version of {@link SharedCounters#zero(int)}.
   */
  public void zero( int index ) throws RemoteException;

  /**
   *  RMI version of {@link SharedCounters#setIfLarger(int,long)}.
   */
  public void setIfLarger( int index, long i ) throws RemoteException;

  /**
   *  RMI version of {@link SharedCounters#setIfSmaller(int,long)}.
   */
  public void setIfSmaller( int index, long i ) throws RemoteException;

  /**
   *  RMI version of {@link SharedCounters#getCounterValues}.
   */
  public long[] getCounterValues() throws RemoteException;

}
