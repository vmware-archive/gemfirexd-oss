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

import java.sql.SQLException;

/**
 * 
 * @author kneeraj
 * 
 */
public class IntColumnRoutingObjectInfo extends ColumnRoutingObjectInfo {

  private Integer actualValue;

  public IntColumnRoutingObjectInfo(int isParameter, Object val, Object resolver) {
    super(isParameter, val, resolver);
  }

  public IntColumnRoutingObjectInfo() {

  }

  public void setActualValue(Object[] parameters, Converter crossConverter)
      throws SQLException {
    if (!this.isValueAConstant()) {
      this.actualValue = (Integer)crossConverter.getJavaObjectOfType(
          java.sql.Types.INTEGER, parameters[((Integer)this.value).intValue()]);
    }
  }

  public int computeHashCode(int hash, int resolverType,
      boolean requiresSerializedHash) {
    Integer val = null;
    if (this.isValueAConstant()) {
      val = (Integer)this.value;
      this.actualValue = val;
    }
    else {
      val = this.actualValue;
    }
    if ((requiresSerializedHash && (resolverType == SingleHopInformation.COLUMN_RESOLVER_FLAG))
        || (resolverType == SingleHopInformation.LIST_RESOLVER_FLAG)) {
      if (val != null) {
        return ResolverUtils.addIntToBucketHash(val.intValue(), hash,
            StoredFormatIds.SQL_INTEGER_ID);
      }
      else {
        // convention is to add a single 0 byte for null value
        // This is compatible with the computeHashCode method of
        // resolvers. If they change in the way of how they handle
        // nulls these should also change.
        return ResolverUtils.addByteToBucketHash((byte)0, hash,
            StoredFormatIds.SQL_INTEGER_ID);
      }
    }
    else {
      return this.dvdEquivalenthashCode();
    }
  }

  public int getTypeFormatId() {
    return StoredFormatIds.SQL_INTEGER_ID;
  }

  public Object getActualValue() {
    if (this.actualValue != null) {
      return this.actualValue;
    }
    if (this.isValueAConstant()) {
      this.actualValue = (Integer)this.value;
      return this.actualValue;
    }
    return null;
  }

  public int dvdEquivalenthashCode() {
    if (this.actualValue != null) {
      return this.actualValue.intValue();
    }
    return 0;
  }
  
  public static void dummy() {
  }
}
