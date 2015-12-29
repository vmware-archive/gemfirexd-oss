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

package com.pivotal.gemfirexd.internal.engine.ddl;

import java.util.AbstractList;
import java.util.Arrays;
import java.util.RandomAccess;

import com.gemstone.gemfire.internal.cache.Token;
import com.pivotal.gemfirexd.internal.engine.jdbc.GemFireXDRuntimeException;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.engine.store.RowFormatter;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.impl.sql.GenericParameterValueSet;

/**
 * 
 * @author kneeraj
 * 
 */
public abstract class JavaObjectsList extends AbstractList<Object> implements
    RandomAccess {

  protected final Object[] cachedObjArray;

  private final int size;

  protected JavaObjectsList(int size) {
    this.size = size;
    this.cachedObjArray = new Object[size];
    Arrays.fill(this.cachedObjArray, Token.INVALID);
  }

  @Override
  public final Object get(int index) {
    if (index < this.size) {
      Object o;
      if ((o = this.cachedObjArray[index]) == Token.INVALID) {
        try {
          o = this.cachedObjArray[index] = getFromObjectOrDVD(index);
        } catch (StandardException e) {
          throw GemFireXDRuntimeException.newRuntimeException(
              "Exception while getting object from "
                  + getClass().getSimpleName(), e);
        }
      }
      return o;
    }
    else {
      throw new IllegalArgumentException("size of the list: " + this.size
          + " is less than index: " + index + " passed");
    }
  }

  @Override
  public final int size() {
    return this.size;
  }

  protected abstract Object getFromObjectOrDVD(int index)
      throws StandardException;

  public static final class BytesJavaObjectsList extends JavaObjectsList {

    private final RowFormatter rf;

    private final byte[] bytes;

    private final byte[][] byteArrays;

    public BytesJavaObjectsList(int size, byte[] bytes,
        GemFireContainer container) {
      super(size);
      this.rf = container.getRowFormatter(bytes);
      this.bytes = bytes;
      this.byteArrays = null;
    }

    public BytesJavaObjectsList(int size, byte[][] byteArrays,
        GemFireContainer container) {
      super(size);
      this.rf = container.getRowFormatter(byteArrays != null ? byteArrays[0]
          : null);
      this.bytes = null;
      this.byteArrays = byteArrays;
    }

    @Override
    protected Object getFromObjectOrDVD(int index) throws StandardException {
      return this.bytes != null ? this.rf.getAsObject(index + 1, this.bytes,
          null) : this.rf.getAsObject(index + 1, this.byteArrays, null);
    }
  }

  public static final class DVDArrayJavaObjectsList extends JavaObjectsList {

    private final DataValueDescriptor[] rowDVDArray;

    public DVDArrayJavaObjectsList(final DataValueDescriptor[] row) {
      super(row.length);
      this.rowDVDArray = row;
    }

    @Override
    protected Object getFromObjectOrDVD(int index) throws StandardException {
      DataValueDescriptor dvd = this.rowDVDArray[index];
      // Asif:If null that means value is not available.
      return dvd != null ? dvd.getObject() : null;
      // Should there be some other token to specify that?
    }
  }

  public static final class PVSJavaObjectsList extends JavaObjectsList {

    private final GenericParameterValueSet pvs;

    public PVSJavaObjectsList(final GenericParameterValueSet pvs) {
      super(pvs.getParameterCount());
      this.pvs = pvs;
    }

    @Override
    protected Object getFromObjectOrDVD(int index) throws StandardException {
      DataValueDescriptor dvd = this.pvs.getParameter(index + 1);
      // Asif:If null that means value is not available.
      return dvd != null ? dvd.getObject() : null;
      // Should there be some other token to specify that?
    }
  }
}
