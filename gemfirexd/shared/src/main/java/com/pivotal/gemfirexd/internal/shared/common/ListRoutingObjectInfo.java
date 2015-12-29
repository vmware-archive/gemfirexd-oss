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


public class ListRoutingObjectInfo extends AbstractRoutingObjectInfo {

  private ColumnRoutingObjectInfo[] listOfValues;

  public ListRoutingObjectInfo() {
    
  }

  public ListRoutingObjectInfo(ColumnRoutingObjectInfo[] listOfValues,
      Object resolver) {
    super(RoutingObjectInfo.INVALID, null, resolver);
    this.listOfValues = listOfValues;
  }

  public boolean isValueAConstant() {
    // TODO Auto-generated method stub
    return false;
  }

  public Object getConstantObjectValue() {
    // TODO Auto-generated method stub
    return null;
  }

  public int getParameterNumber() {
    // TODO Auto-generated method stub
    return 0;
  }

  public Object getRoutingObject(Object value, ClientResolver resolver) {
    // TODO Auto-generated method stub
    return null;
  }
  
  public void writeExternal(ObjectOutput out) throws IOException {
//    if (this.listOfValues != null) {
//      int size = this.listOfValues.length;
//      out.writeInt(size);
//      for(int i=0; i<size; i++) {
//        this.listOfValues[i].writeExternal(out);
//      }
//    } else {
//      out.writeInt(0);
//    }
    out.writeObject(this.listOfValues);
  }

  public void readExternal(ObjectInput in) throws IOException,
      ClassNotFoundException {
//    int size = in.readInt();
//    if (size > 0) {
//      this.listOfValues = new ColumnRoutingObjectInfo[size];
//      for(int i=0; i<size; i++) {
//        ColumnRoutingObjectInfo cInfo = new ColumnRoutingObjectInfo();
//        cInfo.readExternal(in);
//        this.listOfValues[i] = cInfo;
//      }
//    }
//    else {
//      this.listOfValues = null;
//    }
    this.listOfValues = (ColumnRoutingObjectInfo[])in.readObject();
  }

  public int computeHashCode(int hash, int resolverType, boolean requiresSerializedHash) {
    // TODO Auto-generated method stub
    return 0;
  }

  public void setActualValue(Object[] parameters, Converter crossConverter) throws SQLException {
    if (this.listOfValues != null) {
      int len = this.listOfValues.length;
      for(int i=0; i<len; i++) {
        this.listOfValues[i].setActualValue(parameters, crossConverter);
      }
    }
  }

  public boolean isListRoutingObjectInfo() {
    return true;
  }
  
  public ColumnRoutingObjectInfo[] getListOfInfos() {
    return this.listOfValues;
  }

  public int getTypeFormatId() {
    // TODO Auto-generated method stub
    return 0;
  }

  public Object getActualValue() {
    // TODO Auto-generated method stub
    return null;
  }

  public int dvdEquivalenthashCode() {
    // TODO Auto-generated method stub
    return 0;
  }
}
