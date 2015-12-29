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

package com.pivotal.gemfirexd.internal.engine.distributed.utils;

import java.io.DataOutput;
import java.io.IOException;
import java.math.BigInteger;

import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.pivotal.gemfirexd.internal.shared.common.ResolverUtils;

/**
 * Utility methods for SQLInteger/SQLDecimal/... and SQL classes.
 * 
 * @author swale
 * @since 7.0
 */
public final class DataTypeUtils {

  // Neeraj: The below commented piece has been moved to ResolverUtils
//  private static final Constructor<?> stringInternalConstructor;
////
////  private static final Field bigIntMagnitude;
////
//  static {
//    try {
//      stringInternalConstructor = String.class.getDeclaredConstructor(
//          int.class, int.class, char[].class);
//      stringInternalConstructor.setAccessible(true);
//    } catch (Exception ex) {
//      throw new ExceptionInInitializerError(ex);
//    }
//  }
////
////  private DataTypeUtils() {
////  }
////
//  public static String newWrappedString(final char[] chars, final int size) {
//    if (size >= 0) {
//      try {
//        return (String)stringInternalConstructor.newInstance(0, size, chars);
//      } catch (Exception ex) {
//        throw GemFireXDRuntimeException.newRuntimeException(
//            "unexpected exception", ex);
//      }
//    }
//    else {
//      SanityManager.THROWASSERT("unexpected size=" + size);
//      // never reached
//      return null;
//    }
//  }
//
//  public static int[] getBigIntInternalMagnitude(final BigInteger val) {
//    try {
//      return (int[])bigIntMagnitude.get(val);
//    } catch (Exception ex) {
//      throw GemFireXDRuntimeException.newRuntimeException(
//          "unexpected exception", ex);
//    }
//  }
//
//  public static int getBigIntSizeInBytes(final BigInteger val) {
//    return getBigIntMagnitudeSizeInBytes(getBigIntInternalMagnitude(val));
//  }
//
//  public static int getBigIntMagnitudeSizeInBytes(final int[] magnitude) {
//    final int size_1 = magnitude.length - 1;
//    if (size_1 >= 0) {
//      int numBytes = size_1 << 2;
//      // check number of bytes encoded by first int (most significant value)
//      final long firstInt = ((long)magnitude[0] & ResolverUtils.MAG_MASK);
//      return numBytes + ResolverUtils.numBytesWithoutZeros(firstInt);
//    }
//    return 0;
//  }

  /**
   * Serialize the given magnitude array from {@link BigInteger} to given byte
   * array starting at offset. Returns the number of bytes required for
   * serialization. The serialization is compatible with the
   * {@link BigInteger#BigInteger(int, byte[])} constructor.
   */
  public static int serializeBigIntMagnitudeToBytes(final int[] magnitude,
      final byte[] outBytes, final int startPos) {
    final int size_1 = magnitude.length - 1;
    if (size_1 >= 0) {
      // Write in big-endian format to be compatible with
      // BigInteger(int,byte[]) constructor.
      // We don't write from the end to allow using the same strategy when
      // writing to DataOutput and since this may also be slightly more
      // efficient reading from memory in forward direction rather than reverse.
      final long firstInt = ((long)magnitude[0] & ResolverUtils.MAG_MASK);
      int numBytesInFirstInt = ResolverUtils.numBytesWithoutZeros(firstInt);
      // first write the actual number of bytes required for first integer
      int offset = startPos;
      while (numBytesInFirstInt-- > 0) {
        outBytes[offset++] = (byte)(firstInt >>> (8 * numBytesInFirstInt));
      }
      // now write the remaining integers that will each require four bytes
      int intValue;
      for (int index = 1; index <= size_1; ++index) {
        intValue = magnitude[index];
        outBytes[offset++] = (byte)(intValue >>> 24);
        outBytes[offset++] = (byte)(intValue >>> 16);
        outBytes[offset++] = (byte)(intValue >>> 8);
        outBytes[offset++] = (byte)intValue;
      }
      final int numBytes;
      assert (offset - startPos) == (numBytes = ((size_1 << 2) + ResolverUtils
          .numBytesWithoutZeros(firstInt))): "unexpected mismatch of byte"
          + " length, expected=" + numBytes + " actual=" + (offset - startPos);
      return (offset - startPos);
    }
    return 0;
  }
}
