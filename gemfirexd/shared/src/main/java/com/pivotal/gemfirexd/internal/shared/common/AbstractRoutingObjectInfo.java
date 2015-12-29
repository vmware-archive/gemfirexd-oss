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
public abstract class AbstractRoutingObjectInfo implements RoutingObjectInfo {

  private int valueType;

  protected Object value;

  // TODO: to be removed later once it is verified that the same
  // resolver is picked for all the routing object info objects.
  private transient Object resolver;

  public AbstractRoutingObjectInfo() {

  }

  public AbstractRoutingObjectInfo(int isParameter, Object val, Object resolver) {
    this.valueType = isParameter;
    this.value = val;
    this.resolver = resolver;
  }

  public boolean isValueAConstant() {
    return !(this.valueType == RoutingObjectInfo.PARAMETER);
  }

  public Object getConstantObjectValue() {
    if (valueType == RoutingObjectInfo.CONSTANT) {
      return this.value;
    }
    return null;
  }

  public int getParameterNumber() {
    if (valueType == RoutingObjectInfo.PARAMETER) {
      return ((Integer)this.value).intValue();
    }
    return -1;
  }

  // public abstract Object getRoutingObject(Object value, ClientResolver
  // resolver);

  public Object getResolver() {
    return this.resolver;
  }

  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeByte(valueType);
    out.writeObject(value);
  }

  public void readExternal(ObjectInput in) throws IOException,
      ClassNotFoundException {
    this.valueType = in.readByte();
    this.value = in.readObject();
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("value type: ");
    sb.append(getValueType());
    sb.append(", value: ");
    sb.append(this.value);
    return sb.toString();
  }

  private String getValueType() {
    if (this.valueType == RoutingObjectInfo.PARAMETER) {
      return "PARAMETER";
    }

    if (this.valueType == RoutingObjectInfo.CONSTANT) {
      return "CONSTANT";
    }

    return "INVALID";
  }

  public abstract int computeHashCode(int hash, int resolverType,
      boolean requiresSerializedHash);

  public abstract void setActualValue(Object[] parameters_, Converter crossConverter) throws SQLException;

  public abstract int getTypeFormatId();

  public abstract Object getActualValue();

  public abstract int dvdEquivalenthashCode();

  public boolean isListRoutingObjectInfo() {
    return false;
  }
}
