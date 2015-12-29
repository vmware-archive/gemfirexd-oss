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
package com.pivotal.gemfirexd.internal.shared.common;

import java.math.BigDecimal;
import java.sql.SQLException;


/**
 * 
 * @author kneeraj
 * 
 */
public class DoubleColumnRoutingObjectInfo extends ColumnRoutingObjectInfo {

  private Double actualValue;

  public DoubleColumnRoutingObjectInfo(int isParameter, Object val,
      Object resolver) {
    super(isParameter, val, resolver);
  }

  public DoubleColumnRoutingObjectInfo() {

  }

  public int computeHashCode(int hash, int resolverType,
      boolean requiresSerializedHash) {
    Double val = null;
    if (this.isValueAConstant()) {
      val = (Double)this.value;
      this.actualValue = val;
    }
    else {
      val = this.actualValue;
    }
    if ((requiresSerializedHash && (resolverType == SingleHopInformation.COLUMN_RESOLVER_FLAG))
        || (resolverType == SingleHopInformation.LIST_RESOLVER_FLAG)) {
      int typeId = getTypeFormatId();
      if (this.actualValue != null) {
        final long bits = Double.doubleToLongBits(this.actualValue
            .doubleValue());
        return ResolverUtils.addLongToBucketHash(bits, hash, typeId);
      }
      else {
        // convention is to add a single 0 byte for null value
        // This is compatible with the computeHashCode method of
        // resolvers. If they change in the way of how they handle
        // nulls these should also change.
        return ResolverUtils.addByteToBucketHash((byte)0, hash, typeId);
      }
    }
    else {
      return this.dvdEquivalenthashCode();
    }
  }

  public void setActualValue(Object[] parameters, Converter crossConverter)
      throws SQLException {
    if (!this.isValueAConstant()) {
      this.actualValue = (Double)crossConverter.getJavaObjectOfType(
          java.sql.Types.DOUBLE, parameters[((Integer)this.value).intValue()]);
    }
  }

  public int getTypeFormatId() {
    return StoredFormatIds.SQL_DOUBLE_ID;
  }

  public Object getActualValue() {
    if (this.actualValue != null) {
      return this.actualValue;
    }
    if (this.isValueAConstant()) {
      this.actualValue = (Double)this.value;
      return this.actualValue;
    }
    return null;
  }

  // Almost duplicate of SQLDouble hashcode implementation
  // look for bringing that to common class like ResolverUtils.
  public int dvdEquivalenthashCode() {
    double localdoublevalue = this.actualValue != null ? this.actualValue
        .doubleValue() : 0;
    long longVal = (long)localdoublevalue;
    double doubleLongVal = (double)longVal;

    /*
    ** NOTE: This is coded to work around a bug in Visual Cafe 3.0.
    ** If longVal is compared directly to value on that platform
    ** with the JIT enabled, the values will not always compare
    ** as equal even when they should be equal. This happens with
    ** the value Long.MAX_VALUE, for example.
    **
    ** Assigning the long value back to a double and then doing
    ** the comparison works around the bug.
    **
    ** This fixes Cloudscape bug number 1757.
    **
    **              -       Jeff Lichtman
    */
    if (doubleLongVal != localdoublevalue) {
      longVal = Double.doubleToLongBits(localdoublevalue);
    }

    return (int)(longVal ^ (longVal >> 32));
  }

  public static void dummy() {
  }
}
