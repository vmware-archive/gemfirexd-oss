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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.SQLException;


/**
 * 
 * @author kneeraj
 * 
 */
public class CharColumnRoutingObjectInfo extends ColumnRoutingObjectInfo {

  protected String actualValue;

  protected int maxWidth;

  public CharColumnRoutingObjectInfo() {

  }

  public CharColumnRoutingObjectInfo(int isParameter, Object val,
      Object resolver) {
    super(isParameter, val, resolver);
  }

  public int computeHashCode(int hash, int resolverType,
      boolean requiresSerializedHash) {
    String val = null;
    if (this.isValueAConstant()) {
      val = (String)this.value;
      this.actualValue = val;
    }
    else {
      val = this.actualValue;
    }
    if ((requiresSerializedHash && (resolverType == SingleHopInformation.COLUMN_RESOLVER_FLAG))
        || (resolverType == SingleHopInformation.LIST_RESOLVER_FLAG)) {
      int typeId = getTypeFormatId();
      if (this.actualValue != null) {
        final int strlen = this.actualValue.length();
        final char[] data = ResolverUtils.getInternalChars(this.actualValue,
            strlen);
//        if (this instanceof VarCharColumnRoutingObjectInfo && strlen > maxWidth) {
//          strlen = maxWidth;
//        }
        //it is possible that strlen > maxWidth 
        // remove the trailing blanks when computing hash code, particularly for
        // varchar
        int index = strlen - 1;
        while (index >= 0 && data[index] == ' ') {
          index--;
        }
        hash = ResolverUtils.getComputeHashOfCharArrayData(hash, index + 1,
            data, typeId);
        // for char, blank padding needed
        if (this.getTypeFormatId() == StoredFormatIds.SQL_CHAR_ID) {
          while (index + 1 < maxWidth) {
            hash = ResolverUtils.addByteToHash((byte)0x20, hash);
            index++;
          }
        }
//        hash = ResolverUtils.getComputeHashOfCharArrayData(hash, strlen, data,
//            typeId);
//        while (strlen++ < maxWidth) {
//          //KN: ASK JIANXIA why blank padding is still necessary
//          //blank padding for CHAR/VARCHAR
//          //hash = ResolverUtils.addByteToHash((byte) 0x20, hash);
//        }
        return hash;
      }
      else {
        // convention is to add a single 0 byte for null value
        // This is compatible with the computeHashCode method of
        // resolvers. If they change in the way of how they handle
        // nulls these should also change.
        return ResolverUtils.addByteToBucketHash((byte)0, hash,
            StoredFormatIds.SQL_CHAR_ID);
      }
    }
    else {
      return this.dvdEquivalenthashCode();
    }
  }

  public void setActualValue(Object[] parameters, Converter crossConverter)
      throws SQLException {
    if (!this.isValueAConstant()) {
      this.actualValue = (String)crossConverter.getJavaObjectOfType(
          java.sql.Types.CHAR, parameters[((Integer)this.value).intValue()]);
    }
  }

  public int getTypeFormatId() {
    return StoredFormatIds.SQL_CHAR_ID;
  }

  public Object getActualValue() {
    if (this.actualValue != null) {
      return this.actualValue;
    }
    if (this.isValueAConstant()) {
      this.actualValue = (String)this.value;
      return this.actualValue;
    }
    return null;
  }

  public int dvdEquivalenthashCode() {
    if (this.actualValue != null) {
      final int strlen = this.actualValue.length();
      final char[] data = ResolverUtils.getInternalChars(this.actualValue,
          strlen);
//      return ResolverUtils.getHashCodeOfCharArrayData(data, this.actualValue,
//          this.actualValue.length());
      //ResolverUtils.getHashCodeOfCharArrayData() doesn't consider trailing blanks
      int hash = ResolverUtils.getHashCodeOfCharArrayData(data,
          this.actualValue, strlen);
      // for char, blank padding still needed
      if (this.getTypeFormatId() == StoredFormatIds.SQL_CHAR_ID) {
        // remove the trailing blanks when computing hash code, it is possible
        // that data.length > maxWidth
        // so first trim the trailing blanks and do blank padding up to maxWidth
        int index = data.length - 1;
        while (index >= 0 && data[index] == ' ') {
          index--;
        }
        while (index + 1 < maxWidth) {
          hash = ResolverUtils.addByteToHash((byte)0x20, hash);
          index++;
        }
      }
      return hash;
    }
    return 0;
  }

  public void writeExternal(ObjectOutput out) throws IOException {
    super.writeExternal(out);
    out.writeInt(this.maxWidth);
  }

  public void readExternal(ObjectInput in) throws IOException,
      ClassNotFoundException {
    super.readExternal(in);
    this.maxWidth = in.readInt();
  }
  
  public void setMaxWidth(int maxWidth) {
    this.maxWidth = maxWidth;
  }

  public int getMaxWidth() {
    return this.maxWidth;
  }

  public static void dummy() {
  }
}
