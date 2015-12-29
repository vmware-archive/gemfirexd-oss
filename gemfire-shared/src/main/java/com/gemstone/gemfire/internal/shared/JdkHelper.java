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

package com.gemstone.gemfire.internal.shared;

import java.io.InputStream;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Map;

/**
 * This interface defines methods that may require different implementations
 * across JVMs like JDK6, GCJ.
 * 
 * @author swale
 * @since 7.0
 */
public interface JdkHelper {

  /** return a {@link Map} implementation for concurrent usage */
  public Map newConcurrentMap(int initialCapacity, int concurrencyLevel);

  /** return an ID for current thread */
  public long getThreadId(Thread currentThread);

  /** return a Byte object for given value */
  public Byte newByte(byte value);

  /** return a Short object for given value */
  public Short newShort(short value);

  /** return an Integer object for given value */
  public Integer newInteger(int value);

  /** return a Long object for given value */
  public Long newLong(long value);

  /** return a Characted object for given value */
  public Character newCharacter(char value);

  /**
   * reads characters without the trailing newline from standard input in case a
   * console is available else reading from the given input stream
   */
  public String readChars(InputStream in, boolean noecho);

  /**
   * Wrap the given array of character as a String avoiding copying if possible.
   */
  public String newWrappedString(final char[] chars, final int offset,
      final int size);

  /**
   * Returns true if the given {@link NetworkInterface} is up.
   */
  public boolean isInterfaceUp(NetworkInterface iface) throws SocketException;
}
