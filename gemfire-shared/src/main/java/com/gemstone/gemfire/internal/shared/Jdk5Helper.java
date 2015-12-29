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

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * {@link JdkHelper} implementation for JDK5.
 * 
 * @author swale
 * @since 7.5
 */
public class Jdk5Helper implements JdkHelper {

  /**
   * @see JdkHelper#newConcurrentMap(int, int)
   */
  public final ConcurrentHashMap<?, ?> newConcurrentMap(
      final int initialCapacity, final int concurrencyLevel) {
    return new ConcurrentHashMap<Object, Object>(initialCapacity, 0.75f,
        concurrencyLevel);
  }

  /**
   * @see JdkHelper#getThreadId(Thread)
   */
  public final long getThreadId(final Thread currentThread) {
    return currentThread.getId();
  }

  /**
   * @see JdkHelper#newByte(byte)
   */
  public final Byte newByte(final byte value) {
    return Byte.valueOf(value);
  }

  /**
   * @see JdkHelper#newShort(short)
   */
  public final Short newShort(final short value) {
    return Short.valueOf(value);
  }

  /**
   * @see JdkHelper#newInteger(int)
   */
  public final Integer newInteger(final int value) {
    return Integer.valueOf(value);
  }

  /**
   * @see JdkHelper#newLong(long)
   */
  public final Long newLong(final long value) {
    return Long.valueOf(value);
  }

  /**
   * @see JdkHelper#newCharacter(char)
   */
  public final Character newCharacter(final char value) {
    return Character.valueOf(value);
  }

  /**
   * @see JdkHelper#readChars(InputStream, boolean)
   */
  public String readChars(final InputStream in, final boolean noecho) {
    final StringBuilder sb = new StringBuilder();
    char c;
    try {
      int value = in.read();
      if (value != -1 && (c = (char)value) != '\n' && c != '\r') {
        // keep waiting till a key is pressed
        sb.append(c);
      }
      while (in.available() > 0 && (value = in.read()) != -1
          && (c = (char)value) != '\n' && c != '\r') {
        // consume till end of line
        sb.append(c);
      }
    } catch (IOException ioe) {
      throw ClientSharedUtils.newRuntimeException(ioe.getMessage(), ioe);
    }
    return sb.toString();
  }

  private static final Constructor<?> stringInternalConstructor;
  private static final int stringInternalConsVersion;

  // JDK5/6 String(int, int, char[])
  private static final int STRING_CONS_VER1 = 1;
  // JDK7 String(char[], boolean)
  private static final int STRING_CONS_VER2 = 2;
  // GCJ String(char[], int, int, boolean)
  private static final int STRING_CONS_VER3 = 3;

  static {
    Constructor<?> constructor = null;
    int version = 0;
    try {
      // first check the JDK7 version since an inefficient version of JDK5/6 is
      // present in JDK7
      constructor = String.class.getDeclaredConstructor(char[].class,
          boolean.class);
      constructor.setAccessible(true);
      version = STRING_CONS_VER2;
    } catch (Exception e1) {
      try {
        // next check the JDK6 version
        constructor = String.class.getDeclaredConstructor(int.class, int.class,
            char[].class);
        constructor.setAccessible(true);
        version = STRING_CONS_VER1;
      } catch (Exception e2) {
        // gcj has a different version
        try {
          constructor = String.class.getDeclaredConstructor(char[].class,
              int.class, int.class, boolean.class);
          constructor.setAccessible(true);
          version = STRING_CONS_VER3;
        } catch (Exception e3) {
          // ignored
        }
      }
    }
    stringInternalConstructor = constructor;
    stringInternalConsVersion = version;
  }

  public String newWrappedString(final char[] chars, final int offset,
      final int size) {
    if (size >= 0) {
      try {
        switch (stringInternalConsVersion) {
          case STRING_CONS_VER1:
            return (String)stringInternalConstructor.newInstance(offset, size,
                chars);
          case STRING_CONS_VER2:
            if (offset == 0 && size == chars.length) {
              return (String)stringInternalConstructor.newInstance(chars, true);
            }
            else {
              return new String(chars, offset, size);
            }
          case STRING_CONS_VER3:
            return (String)stringInternalConstructor.newInstance(chars, offset,
                size, true);
          default:
            return new String(chars, offset, size);
        }
      } catch (Exception ex) {
        throw ClientSharedUtils.newRuntimeException("unexpected exception", ex);
      }
    }
    else {
      throw new AssertionError("unexpected size=" + size);
    }
  }

  public boolean isInterfaceUp(NetworkInterface iface) throws SocketException {
    // no API in JDK5 for this, so always return true
    return true;
  }
}
