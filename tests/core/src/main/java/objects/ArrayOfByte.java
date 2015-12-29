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

package objects;

import com.gemstone.gemfire.internal.NanoTimer;
import hydra.*;
import java.io.*;

/**
 * An array of <code>byte</code>s that encodes an "index".  Note that
 * this class does not implement the {@link ConfigurableObject} or
 * {@link TimestampedObject} interfaces, and therefore must be special
 * cased in {@link ObjectHelper} methods.
 *
 * @see ArrayOfBytePrms
 *
 * @author Lise Storc
 * @since 3.0
 */
public class ArrayOfByte {

  /**
   * Returns a <code>byte</code> array that optionally encodes the given
   * <code>index</code> and a timestamp.
   *
   * @throws ObjectCreationException
   *         information cannot be encoded
   */
  public static byte[] init(int index) {
    int size = ArrayOfBytePrms.getSize();
    boolean encodeKey = ArrayOfBytePrms.encodeKey();
    boolean encodeTimestamp = ArrayOfBytePrms.encodeTimestamp();
    return init(index, size, encodeKey, encodeTimestamp);
  }

  /**
   * Returns a <code>byte</code> array that optionally encodes the given
   * <code>index</code> and a timestamp.
   *
   * @throws ObjectCreationException
   *         information cannot be encoded
   */
  protected static byte[] init(int index, int size, boolean encodeKey,
                                                    boolean encodeTimestamp) {
    if (encodeKey) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream dos = new DataOutputStream(baos);
      try {
        dos.writeInt(index);
        if (encodeTimestamp) {
          dos.writeLong(NanoTimer.getTime() - RemoteTestModule.getClockSkew());
        }
      } catch(IOException e) {
        String s =  "Unable to write to stream";
        throw new ObjectCreationException(s, e);
      }
      byte[] b = baos.toByteArray();
      if (b.length > size) {
        String s = "Unable to encode into byte array of size " + size;
        throw new ObjectCreationException(s);
      }
      byte[] result = new byte[size];
      System.arraycopy(b, 0, result, 0, b.length);
      return result;
    } else if (encodeTimestamp) {
      throw new HydraInternalException("Should not happen");
    } else {
      return new byte[size];
    }
  }

  /**
   * Returns the index encoded in the give <code>byte</code> array.
   *
   * @throws ObjectAccessException
   *         The index cannot be decoded from <code>bytes</code>
   */
  public static int getIndex(byte[] bytes) {
    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    DataInputStream dis = new DataInputStream(bais);
    try {
      return dis.readInt();
    } catch (IOException e) {
      throw new ObjectAccessException("Unable to read from stream", e);
    }
  }

  /**
   * Returns the timestamp encoded in the give <code>byte</code> array, if any.
   *
   * @throws HydraConfigException
   *         The object is not configured to encode a timestamp.
   * @throws ObjectAccessException
   *         The timestamp cannot be decoded from <code>bytes</code>
   */
  public static long getTimestamp(byte[] bytes) {
    if (bytes == null) {
      // added this check for bug 39587
      throw new IllegalArgumentException("the bytes arg was null");
    }
    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    DataInputStream dis = new DataInputStream(bais);
    try {
      dis.readInt(); // advance past the index
      long timestamp = dis.readLong();
      if (timestamp == 0) {
        String s = "Object is not configured to encode timestamp";
        throw new HydraConfigException(s);
      }
      return timestamp;
    } catch (IOException e) {
      throw new ObjectAccessException("Unable to read from stream", e);
    }
  }

  /**
   * Resets the timestamp to the current time.
   */
  public static void resetTimestamp(byte[] bytes) {
    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    DataInputStream dis = new DataInputStream(bais);
    int index;
    try {
      index = dis.readInt(); // advance past the index
      long timestamp = dis.readLong();
      if (timestamp == 0) {
        // object is not configured to encode timestamp
        return;
      }
    } catch (IOException e) {
      throw new ObjectAccessException("Unable to read from stream", e);
    }
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    try {
      dos.writeInt(index);
      dos.writeLong(NanoTimer.getTime() - RemoteTestModule.getClockSkew());
    } catch(IOException e) {
      String s =  "Unable to write to stream";
      throw new ObjectCreationException(s, e);
    }
    byte[] b = baos.toByteArray();
    System.arraycopy(b, 0, bytes, 0, b.length);
  }

  /**
   * Validates that the given <code>index</code> is encoded in the
   * given <code>byte</code> array.
   *
   * @throws ObjectValidationException
   *         If <code>index</code> is not encoded in <code>bytes</code>
   */
  public static void validate( int index, byte[] bytes ) {
    int encodedIndex = getIndex( bytes );
    if ( encodedIndex != index ) {
      throw new ObjectValidationException( "Expected index " + index + ", got " + encodedIndex + " (make sure that " + BasePrms.nameForKey(ArrayOfBytePrms.encodeKey) + " is set to true)" );
    }
  }
}
