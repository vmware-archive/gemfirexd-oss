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
import java.math.BigInteger;
import java.sql.SQLException;

import com.gemstone.gemfire.internal.shared.ClientSharedUtils;

/**
 * 
 * @author kneeraj
 * 
 */
public class DecimalColumnRoutingObjectInfo extends ColumnRoutingObjectInfo {

  private BigDecimal actualValue;

  public DecimalColumnRoutingObjectInfo(int isParameter, Object val,
      Object resolver) {
    super(isParameter, val, resolver);
  }

  public DecimalColumnRoutingObjectInfo() {

  }

  public int computeHashCode(int hash, int resolverType,
      boolean requiresSerializedHash) {
    BigDecimal val = null;
    if (this.isValueAConstant()) {
      val = (BigDecimal)this.value;
      this.actualValue = val;
    }
    else {
      val = this.actualValue;
    }
    if ((requiresSerializedHash && (resolverType == SingleHopInformation.COLUMN_RESOLVER_FLAG))
        || (resolverType == SingleHopInformation.LIST_RESOLVER_FLAG)) {
      if (this.actualValue != null) {
        int scale = this.actualValue.scale();
        if (scale < 0) {
          this.actualValue = this.actualValue.setScale(0);
          scale = 0;
        }
        final BigInteger bi = this.actualValue.unscaledValue();

        // compute for scale and signum, then magnitude
        hash = ResolverUtils.addByteToBucketHash((byte)scale, hash,
            StoredFormatIds.SQL_DECIMAL_ID);
        hash = ResolverUtils.addByteToBucketHash((byte)bi.signum(), hash,
            StoredFormatIds.SQL_DECIMAL_ID);
        final int[] magnitude = ClientSharedUtils
            .getBigIntInternalMagnitude(bi);
        if (magnitude != null) {
          return ResolverUtils.computeHashCode(magnitude, hash,
              StoredFormatIds.SQL_DECIMAL_ID);
        }
        else {
          return ResolverUtils.addBytesToBucketHash(bi.abs().toByteArray(),
              hash, StoredFormatIds.SQL_DECIMAL_ID);
        }
      }
      else {
        // convention is to add a single 0 byte for null value
        // This is compatible with the computeHashCode method of
        // resolvers. If they change in the way of how they handle
        // nulls these should also change.
        return ResolverUtils.addByteToBucketHash((byte)0, hash,
            StoredFormatIds.SQL_DECIMAL_ID);
      }
    }
    else {
      return this.dvdEquivalenthashCode();
    }
  }

  public void setActualValue(Object[] parameters, Converter crossConverter)
      throws SQLException {
    if (!this.isValueAConstant()) {
      this.actualValue = (BigDecimal)crossConverter.getJavaObjectOfType(
          java.sql.Types.DECIMAL, parameters[((Integer)this.value).intValue()]);
    }
  }

  public int getTypeFormatId() {
    return StoredFormatIds.SQL_DECIMAL_ID;
  }

  public Object getActualValue() {
    if (this.actualValue != null) {
      return this.actualValue;
    }
    if (this.isValueAConstant()) {
      this.actualValue = (BigDecimal)this.value;
      return this.actualValue;
    }
    return null;
  }

  // Almost duplicate of SQLDecimal hashcode implementation
  // look for bringing that to common class like ResolverUtils.
  public int dvdEquivalenthashCode() {
    long longVal;
    double doubleVal = (this.actualValue != null) ? this.actualValue
        .doubleValue() : 0;

    if (Double.isInfinite(doubleVal)) {
      /*
       ** This loses the fractional part, but it probably doesn't
       ** matter for numbers that are big enough to overflow a double -
       ** it's probably rare for numbers this big to be different only in
       ** their fractional parts.
       */
      longVal = this.actualValue.longValue();
    }
    else {
      longVal = (long)doubleVal;
      if (longVal != doubleVal) {
        longVal = Double.doubleToLongBits(doubleVal);
      }
    }
    return (int)(longVal ^ (longVal >> 32));
  }

  public static void dummy() {
  }
}
