
/*

 Derived from source files from the Derby project.

 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to you under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.

 */

/*
 * Changes for GemFireXD distributed data platform (some marked by "GemStone changes")
 *
 * Portions Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
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

package com.pivotal.gemfirexd.internal.engine.sql.compile.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Iterator;

import com.gemstone.gemfire.internal.DSCODE;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.shared.Version;
import com.gemstone.gnu.trove.THashSet;
import com.gemstone.gnu.trove.TObjectHashingStrategy;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.types.BooleanDataValue;
import com.pivotal.gemfirexd.internal.iapi.types.DataType;
import com.pivotal.gemfirexd.internal.iapi.types.DataTypeDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.SQLBoolean;
import com.pivotal.gemfirexd.internal.iapi.types.UserType;

/**
 * A class that is used to store a list of DataValueDescriptors and provide
 * ordering capability.
 * 
 * This is required for distinct aggregation of a column.
 * 
 * Keep this class as final since code depends on getClass() comparison in some
 * places.
 * 
 * @author soubhikc
 */
public final class DVDSet extends UserType {

  private ArrayList<Object> values;
  private THashSet valueSet;

  public static final InternalDataSerializer.CollectionCreator<ArrayList<Object>>
    tListCreator = new InternalDataSerializer.CollectionCreator<ArrayList<Object>>() {

    @Override
    public ArrayList<Object> newCollection(int initialCapacity) {
      return new ArrayList<Object>(initialCapacity);
    }
  };

  private DataTypeDescriptor resultDescriptor;

  public static final int DEFAULT_SIZE = 6;

  /** for deserialization only */
  public DVDSet() {
  }

  public DVDSet(int size) {
    this.values = new ArrayList<Object>(size);
    this.valueSet = new THashSet(size);
  }

  public DVDSet(Object valueDescriptor) {
    assert valueDescriptor == null
        || valueDescriptor instanceof DataTypeDescriptor: "DataTypeDescriptor "
        + "is expected but got " + valueDescriptor;
    this.values = new ArrayList<Object>(DEFAULT_SIZE);
    this.valueSet = new THashSet(DEFAULT_SIZE);
    this.resultDescriptor = (DataTypeDescriptor)valueDescriptor;
  }

  public DVDSet(DataTypeDescriptor valueDescriptor) {
    this.values = new ArrayList<Object>(DEFAULT_SIZE);
    this.valueSet = new THashSet(DEFAULT_SIZE);
    this.resultDescriptor = valueDescriptor;
  }
  
  /*
   * For NCJ Use
   */
  public DVDSet(DataTypeDescriptor valueDescriptor,
      TObjectHashingStrategy hashStrategy) {
    this.values = new ArrayList<Object>(DEFAULT_SIZE);
    this.valueSet = new THashSet(DEFAULT_SIZE, hashStrategy);
    this.resultDescriptor = (DataTypeDescriptor)valueDescriptor;
  }

  private DVDSet(final ArrayList<Object> values,
      final DataTypeDescriptor resultDescriptor) {

    /*
     * resultDescriptor during query execution will be null both at query and
     * dataStore nodes. In dataStore nodes we don't care as this is not required
     * to serialize but in query node this is required and is taken care by
     * ResultHolder.setDistinctAggUnderlyingType qInfo calls.
     * 
     * Still this assignment is needed as type needs to be carried on during
     * query compilation (bindExpression()).
     */
    this.resultDescriptor = resultDescriptor;
    final int numValues = values.size();
    if (numValues > 0) {
      this.values = new ArrayList<Object>(values);
      this.valueSet = new THashSet(values);
    }
    else {
      this.values = new ArrayList<Object>(DEFAULT_SIZE);
      this.valueSet = new THashSet(DEFAULT_SIZE);
    }
  }

  public void setResultDescriptor(DataTypeDescriptor valueDescriptor) {
    if (SanityManager.DEBUG) {
      if (GemFireXDUtils.TraceAggreg) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_AGGREG,
            "DVDSet::setResultDescriptor(DataTypeDescriptor) is "
                + valueDescriptor + " for 0x" + System.identityHashCode(this));
      }
    }
    this.resultDescriptor = valueDescriptor;
  }

  public final DataTypeDescriptor getResultDescriptor() {
    return resultDescriptor;
  }

  public int resultTypePrecedence() {
    return resultDescriptor.getTypeId().typePrecedence();
  }

  public void addValue(Object value) {
    if (this.valueSet.add(value)) {
      this.values.add(value);
    }
  }

  /*
   * For NCJ Purpose
   * @see DVDSet.in for notes 
   */
  public void addValueAndCheckType(DataValueDescriptor value) {
    if (value.getTypeFormatId() != resultDescriptor.getDVDTypeFormatId()) {
      Assert.fail("expected type=" + resultDescriptor.toString() + " incoming="
          + value.getTypeName());
    }

    this.values.add(value);
  }

  /*
   * The following methods implement the Orderable protocol.
   */

  @Override
  public int compare(DataValueDescriptor other) throws StandardException {
    if (this == other) return 0;

    final DVDSet arg = (DVDSet)other;
    final int thisObjSize = this.values.size();
    final int argObjSize = arg.values.size();
    final int size = thisObjSize < argObjSize ? thisObjSize
        : argObjSize;

    Iterator<?> thisItr = this.values.iterator();
    Iterator<?> argItr = arg.values.iterator();
    for (int index = 0; index < size; index++) {
      final DataValueDescriptor thisDVD = (DataValueDescriptor)thisItr.next();
      final DataValueDescriptor argDVD = (DataValueDescriptor)argItr.next();
      final int cmp = thisDVD.compare(argDVD);
      if (cmp != 0) {
        return cmp;
      }
    }

    /*
     * if all the values are equal in both the list,
     * then let the size difference decide.
     */
    return thisObjSize - argObjSize;
  }

  /** @see DataValueDescriptor#getClone */
  @Override
  public DataValueDescriptor getClone() {
    // Call constructor with all of our info
    return new DVDSet(this.values, this.resultDescriptor);
  }

  /**
   * @see DataValueDescriptor#getNewNull
   */
  @Override
  public DataValueDescriptor getNewNull() {
    return new DVDSet(this.resultDescriptor);
  }

  @Override
  public void restoreToNull() {
    if (!this.valueSet.isEmpty()) {
      this.values.clear();
      this.valueSet.clear();
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void setValue(Object value) {

    if (SanityManager.DEBUG) {
      if (value.getClass() != ArrayList.class) {
        SanityManager.THROWASSERT("value (" + value + ") not an ArrayList but "
            + value.getClass());
      }
    }
    this.values = (ArrayList<Object>)value;
    if (!this.valueSet.isEmpty()) {
      this.valueSet.clear();
    }
    this.valueSet.addAll(this.values);
  }

  @Override
  public ArrayList<Object> getObject() {
    return this.values;
  }

  @Override
  protected void setFrom(final DataValueDescriptor theValue)
      throws StandardException {
    setValue(theValue.getObject());
  }

  /**
   * @exception IOException
   *              error writing data
   */
  @Override
  public void writeExternal(ObjectOutput out) throws IOException {

    if (SanityManager.DEBUG)
      SanityManager.ASSERT(!isNull(),
          "writeExternal() is not supposed to be called for null values.");

    writeDVDCollection(this.values, out);
  }

  /**
   * @see java.io.Externalizable#readExternal
   * 
   * @exception IOException
   *              Thrown on error reading the object
   * @exception ClassNotFoundException
   *              Thrown if the class of the object is not found
   */
  @Override
  public void readExternal(ObjectInput in) throws IOException,
      ClassNotFoundException {
    this.values = readDVDCollection(in, tListCreator);
    if (!this.valueSet.isEmpty()) {
      this.valueSet.clear();
    }
    this.valueSet.addAll(this.values);
  }

  /**
   * Check if the value is null.
   * 
   * @return Whether or not value is logically null.
   */
  @Override
  public final boolean isNull() {
    return this.values.isEmpty();
  }

  @Override
  public String getTypeName() {
    return "DVDSET";
  }
  
  @Override
  public byte getTypeId() {
    return DSCODE.HASH_SET;
  }

  @Override
  public int hashCode() {
    return this.values.hashCode();
  }

  @Override
  public String toString() {
    if (isNull()) {
      return "NULL";
    }
    else {
      return this.values.toString();
    }
  }

  public DataValueDescriptor getNull() throws StandardException {
    return getNewNull();
  }

  public void merge(DVDSet input) {
    if (SanityManager.DEBUG) {
      if (GemFireXDUtils.TraceAggreg) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_AGGREG,
            "DVDSet#merge (expected to be originating node) before merge "
                + "valueSet.size=" + this.values.size()
                + " incoming merge data size=" + input.values.size());
      }
    }

    // this will happen in dataStore node
    final ArrayList<Object> ivals = input.values;
    final int nvals = ivals.size();
    if (nvals > 0) {
      for (int index = 0; index < nvals; index++) {
        addValue(ivals.get(index));
      }
    }

    if (SanityManager.DEBUG) {
      if (GemFireXDUtils.TraceAggreg) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_AGGREG,
            "DVDSet#merge (expected to be originating node) post merge "
                + "valueSet.size=" + this.values.size()
                + " valueSet merged data " + this.values);
      }
    }
  }

  @Override
  public void toDataForOptimizedResultHolder(final DataOutput dos)
      throws IOException {
    // Asif: we need to filter out DVD with null value . The best would be
    // to filter it out during creation of the DVDSet object itself.
    // [sb]: As its a HashSet, null will be only one entry, which is needed for
    // distinct count. What else is expected to shrink here.. ?
    int numValues = this.values.size();
    InternalDataSerializer.writeArrayLength(numValues, dos);
    if (numValues > 0) {
      // backward compatibility for peer to query node
      // overflow in sorter is broken for older releases (#51018/#51028)
      Version v = InternalDataSerializer.getVersionForDataStream(dos);
      if (v.compareTo(Version.GFXD_13) >= 0) {
        InternalDataSerializer.writeByte(((DataValueDescriptor)this.values
            .iterator().next()).getTypeId(), dos);
      }
      for (Object dvd : this.values) {
        ((DataValueDescriptor)dvd).toDataForOptimizedResultHolder(dos);
      }
    }
  }

  @Override
  public void fromDataForOptimizedResultHolder(DataInput dis)
      throws IOException, ClassNotFoundException {
//    assert this.resultDescriptor != null: "cannot de-serialize without "
//        + "resultDescriptor for 0x" + System.identityHashCode(this);
//
//    if (this.resultDescriptor == null) {
//      throw new IOException(
//          "Cannot de-serialize without resultDescriptor in DVDSet");
//    }
        
    int len = InternalDataSerializer.readArrayLength(dis);
    // during deserialization, no need to do hashcode/equals since source has
    // already applied the Set semantics
    this.values = new ArrayList<Object>(len);
    this.valueSet = new THashSet(len, DVDSetHashingStrategy.getInstance());
    if (len > 0) {
      byte typeId = DSCODE.ILLEGAL;
      // backward compatibility for peer to query node
      // overflow in sorter is broken for older releases (#51018/#51028)
      Version v = InternalDataSerializer.getVersionForDataStream(dis);
      if (v.compareTo(Version.GFXD_13) >= 0) {
        typeId = InternalDataSerializer.readByte(dis);
      }
      if (this.resultDescriptor != null) {
        try {
          for (int i = 0; i < len; i++) {
            DataValueDescriptor dvd = this.resultDescriptor.getNull();
            dvd.fromDataForOptimizedResultHolder(dis);
            this.values.add(dvd);
            this.valueSet.add(dvd);
          }
        } catch (StandardException se) {
          throw new IOException(se.toString(), se);
        }
      }
      else if (typeId != DSCODE.ILLEGAL) {
        for (int i = 0; i < len; i++) {
          DataValueDescriptor dvd = DataType.readNullDVD(typeId, dis);
          dvd.fromDataForOptimizedResultHolder(dis);
          this.values.add(dvd);
          this.valueSet.add(dvd);
        }
      }
      else {
        throw new IOException(
            "Cannot de-serialize without resultDescriptor in DVDSet");
      }
    }
    // reset the hashing strategy just in case for later add calls
    this.valueSet.setHashingStrategy(this.valueSet);

    if (GemFireXDUtils.TraceAggreg) {
      SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_AGGREG,
          "De-serialized the value set of size " + this.values.size() + " to "
              + this.values + " for " + System.identityHashCode(this));
    }
  }

  @Override
  public String getString() {
    if (isNull())
      return null;
    else
      return values.toString();
  }
    
  /*
   * For NCJ Purpose
   * Overrides @see DataType#in
   *  
   * @param left 
   * @param inList - ignored
   * @param ordering - ignored - DOesnt matter values are ordered or not.
   */
  @Override
  public BooleanDataValue in(DataValueDescriptor left,
      DataValueDescriptor[] inList, boolean orderedList)
      throws StandardException {
    if (SanityManager.DEBUG) {
      if (GemFireXDUtils.TraceNCJIter) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_NCJ_ITER,
            "DVDSet#in "
                + getTypeName()
                + " of type "
                + (this.resultDescriptor == null ? "null"
                    : this.resultDescriptor.getTypeName()) + " ,hash-set-size="
                + this.values.size() + " ,hash-set-hashing-strategy="
                + this.valueSet.getHashingStrategyName() + " ,left=" + left
                + " ,left-type=" + left.getClass().getSimpleName());
      }

      if (GemFireXDUtils.TraceNCJDump) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_NCJ_DUMP, "DVDSet#in "
            + getTypeName() + " of size=" + this.values.size() + " ,hash-set"
            + this.values);
      }
    }
    
    // If no element, just return false
    // if left is null then just return false
    if (this.isNull() || left.isNull()) {
      return SQLBoolean.unknownTruthValue(); 
    }

    /**
     * Please note that we need to be careful about "values.contains"; It should
     * use a correct Hashing strategy like DVDSetHashingStrategy especially
     * because of possible comparison between different data types like int and
     * big-int. We have also added an additional safeguard as @see
     * DVDSet#addValueAndCheckType
     **/
    if (this.values.contains(left)) {
      if (SanityManager.DEBUG) {
        if (GemFireXDUtils.TraceNCJIter) {
          SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_NCJ_ITER,
              "DVDSet#in return true for left=" + left);
        }
      }

      return SQLBoolean.trueTruthValue();
    }

    if (SanityManager.DEBUG) {
      if (GemFireXDUtils.TraceNCJIter) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_NCJ_ITER,
            "DVDSet#in return false for left=" + left);
      }
    }

    return SQLBoolean.falseTruthValue();
  }

  /*
   * For NCJ use
   */
  public DataValueDescriptor[] getValues() {
    SanityManager.ASSERT(!this.isNull(), "Values missing");

    Object[] objArr = values.toArray();
    DataValueDescriptor[] newList = new DataValueDescriptor[objArr.length];
    for (int idx = 0; idx < objArr.length; idx++) {
      newList[idx] = (DataValueDescriptor)objArr[idx];
    }

    return newList;
  }
}
