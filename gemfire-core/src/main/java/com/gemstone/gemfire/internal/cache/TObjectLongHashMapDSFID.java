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
package com.gemstone.gemfire.internal.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.shared.Version;
import com.gemstone.gnu.trove.TObjectHashingStrategy;
import com.gemstone.gnu.trove.TObjectLongHashMap;
import com.gemstone.gnu.trove.TObjectLongProcedure;

/**
 * An extension of {@link TObjectLongHashMap} to use optimized
 * DataSerializableFixedID for serialization in GemFire.
 */
public final class TObjectLongHashMapDSFID extends TObjectLongHashMap
    implements DataSerializableFixedID {

  /**
   * Creates a new <code>TObjectLongHashMapDSFID</code> instance with
   * the default capacity and load factor.
   */
  public TObjectLongHashMapDSFID() {
    super();
  }

  /**
   * Creates a new <code>TObjectLongHashMap</code> instance with a prime
   * capacity equal to or greater than <tt>initialCapacity</tt>,
   * with the specified load factor, and having specified defaultValue to be
   * returned by get() when map does not contain a key.
   *
   * @param initialCapacity used to find a prime capacity for the table.
   * @param loadFactor used to calculate the threshold over which
   *                   rehashing takes place.
   * @param strategy used to compute hash codes and to compare keys.
   * @param defaultValue returned by get() when map does not contain given key
   */
  public TObjectLongHashMapDSFID(int initialCapacity, float loadFactor,
      TObjectHashingStrategy strategy, long defaultValue) {
    super(initialCapacity, loadFactor, strategy);
    this.defaultValue = defaultValue;
  }

  @Override
  public int getDSFID() {
    return TOBJECTLONGHASHMAP;
  }

  @Override
  public void toData(final DataOutput out) throws IOException {
    InternalDataSerializer.checkOut(out);
    InternalDataSerializer.writeSignedVL(this.defaultValue, out);
    int size = this.size();
    InternalDataSerializer.writeArrayLength(size, out);
    if (size > 0) {
      forEachEntry(new TObjectLongProcedure() {
        @Override
        public boolean execute(Object a, long b) {
          try {
            DataSerializer.writeObject(a, out);
            InternalDataSerializer.writeSignedVL(b, out);
          } catch (IOException e) {
            return false;
          }
          return true;
        }
      });
    }
  }

  @Override
  public void fromData(DataInput in) throws IOException,
      ClassNotFoundException {
    InternalDataSerializer.checkIn(in);
    this.defaultValue = InternalDataSerializer.readSignedVL(in);
    int size = InternalDataSerializer.readArrayLength(in);
    if (size > 0) {
      for (int i = 0; i < size; i++) {
        Object key = DataSerializer.readObject(in);
        long value = InternalDataSerializer.readSignedVL(in);
        put(key, value);
      }
    }
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }
}
