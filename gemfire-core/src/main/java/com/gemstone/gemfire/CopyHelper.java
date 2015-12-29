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
package com.gemstone.gemfire;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.UUID;

import com.gemstone.gemfire.internal.HeapDataOutputStream;
import com.gemstone.gemfire.internal.cache.CachedDeserializable;
import com.gemstone.gemfire.internal.cache.Token;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.shared.Version;
import com.gemstone.gemfire.pdx.PdxInstance;
import com.gemstone.gemfire.pdx.WritablePdxInstance;
import com.gemstone.gemfire.pdx.internal.PdxUnreadData;

/**
 * A static helper for optimally creating copies.  Creating copies
 * of cache values provides improved concurrency as well as isolation.
 * For transactions, creating a copy is the guaranteed way to enforce
 * "Read Committed" isolation on changes to cache
 * <code>Entries</code>.

 * <p>Here is a simple example of how to use <code>CopyHelper.copy</code>
 *  <pre>
 *    Object o = r.get("stringBuf");
 *    StringBuffer s = (StringBuffer) CopyHelper.copy(o);
 *    s.append("... and they lived happily ever after.  The End.");
 *    r.put("stringBuf", s);
 *  </pre>
 *
 * @see Cloneable
 * @see Serializable
 * @see DataSerializer
 * @see com.gemstone.gemfire.cache.Cache#setCopyOnRead
 * @see com.gemstone.gemfire.cache.CacheTransactionManager
 *
 * @author Mitch Thomas
 * @since 4.0
 */

public final class CopyHelper {

  // no instances allowed
  private CopyHelper() {
  }

  /** for tests only */
  static boolean isWellKnownImmutableInstance(Object o) {
    return isWellKnownImmutableInstance(o, o.getClass());
  }

  /**
   * Return true if the given object is an instance of a well known
   * immutable class.
   * The well known classes are:
   * <ul>
   * <li>String
   * <li>Byte
   * <li>Character
   * <li>Short
   * <li>Integer
   * <li>Long
   * <li>Float
   * <li>Double
   * <li>BigInteger
   * <li>BigDecimal
   * <li>UUID
   * <li>PdxInstance but not WritablePdxInstance
   * </ul>
   * @param o the object to check
   * @return true if o is an instance of a well known immutable class.
   * @since 6.6.2
   */
  public static boolean isWellKnownImmutableInstance(Object o, Class<?> c) {
    if (c == String.class) {
      return true;
    }
    if (o instanceof Number) {
      if (c == Integer.class) return true;
      if (c == Long.class) return true;
      if (c == Byte.class) return true;
      if (c == Short.class) return true;
      if (c == Float.class) return true;
      if (c == Double.class) return true;
      // subclasses of non-final classes may be mutable
      if (c == BigInteger.class) return true;
      if (c == BigDecimal.class) return true;
    }
    if (o instanceof PdxInstance && !(o instanceof WritablePdxInstance)) {
      // no need to copy since it is immutable
      return true;
    }
    if (c == Character.class) return true;
    if (o instanceof UUID) return true;
    return false;
  }

  /**
   * <p>Makes a copy of the specified object.
   * Copies can only be made if the original is Cloneable or serializable by GemFire.
   * If o is a {@link #isWellKnownImmutableInstance(Object, Class) well known
   *   immutable instance} then it will be returned without copying it.
   *
   * @param o the original object that a copy is needed of
   * @return the new instance that is a copy of of the original
   * @throws CopyException if copying fails because a class could not
   * be found or could not be serialized.
   * @since 4.0
   */
  @SuppressWarnings("unchecked")
  public static <T> T copy(T o) {
    T copy = null;
    try {
    if (o == null) {
      return null;
    } else if (o instanceof Token) {
      return o;
    } else {
      final Class<?> c = o.getClass();
      if (c == byte[].class) {
        copy = (T)((byte[])o).clone();
        return copy;
      }
      if (c == byte[][].class) {
        byte[][] byteArrays = (byte[][])o;
        byte[][] arraysCopy = new byte[byteArrays.length][];
        for (int index = 0; index < byteArrays.length; ++index) {
          arraysCopy[index] = byteArrays[index].clone();
        }
        copy = (T)arraysCopy;
        return copy;
      }
      if (isWellKnownImmutableInstance(o, c)) return o;
      if (o instanceof Cloneable) {
        try {
          // Note that Object.clone is protected so we need to use reflection
          // to call clone even though this guy implements Cloneable
          // By convention, the user should make the clone method public.
          // But even if they don't, let's go ahead and use it.
          // The other problem is that if the class is private, we still
          // need to make the method accessible even if the method is public,
          // because Object.clone is protected.
          Method m = c.getDeclaredMethod("clone", new Class[0]);
          m.setAccessible(true);
          copy = (T)m.invoke(o, (Object[])null);
          return copy;
        } catch (NoSuchMethodException ignore) {
          // try using Serialization
        } catch (IllegalAccessException ignore) {
          // try using Serialization
        } catch (SecurityException ignore) {
          // try using Serialization
        } catch (InvocationTargetException ex) {
          Throwable cause = ex.getTargetException();
          if (cause instanceof CloneNotSupportedException) {
            // try using Serialization
          } else {
            throw new CopyException(LocalizedStrings.CopyHelper_CLONE_FAILED.toLocalizedString(), cause != null ? cause : ex);
          }
        }
      } else if (o instanceof CachedDeserializable) {
        return (T) ((CachedDeserializable) o).getDeserializedWritableCopy(null, null);
      } else if (o.getClass().isArray() && o.getClass().getComponentType().isPrimitive()) {
        if (o instanceof byte[]) {
          byte[] a = (byte[])o;
          return (T) Arrays.copyOf(a, a.length);
        } else if (o instanceof boolean[]) {
          boolean[] a = (boolean[])o;
          return (T) Arrays.copyOf(a, a.length);
        } else if (o instanceof char[]) {
          char[] a = (char[])o;
          return (T) Arrays.copyOf(a, a.length);
        } else if (o instanceof int[]) {
          int[] a = (int[])o;
          return (T) Arrays.copyOf(a, a.length);
        } else if (o instanceof long[]) {
          long[] a = (long[])o;
          return (T) Arrays.copyOf(a, a.length);
        } else if (o instanceof short[]) {
          short[] a = (short[])o;
          return (T) Arrays.copyOf(a, a.length);
        } else if (o instanceof float[]) {
          float[] a = (float[])o;
          return (T) Arrays.copyOf(a, a.length);
        } else if (o instanceof double[]) {
          double[] a = (double[])o;
          return (T) Arrays.copyOf(a, a.length);
        }
      }
      try {
        HeapDataOutputStream hdos = new HeapDataOutputStream(Version.CURRENT);
        DataSerializer.writeObject(o, hdos);
        copy = (T)DataSerializer.readObject(new DataInputStream(hdos.getInputStream()));
        return copy;
      } catch (ClassNotFoundException ex) {
        throw new CopyException(LocalizedStrings.CopyHelper_COPY_FAILED_ON_INSTANCE_OF_0.toLocalizedString(o.getClass()), ex);
      } catch (IOException ex) {
        throw new CopyException(LocalizedStrings.CopyHelper_COPY_FAILED_ON_INSTANCE_OF_0.toLocalizedString(o.getClass()), ex);
      }
    }
  } finally {
    if (copy != null) {
      PdxUnreadData.copy(o, copy);
    }
  }
  }
}
