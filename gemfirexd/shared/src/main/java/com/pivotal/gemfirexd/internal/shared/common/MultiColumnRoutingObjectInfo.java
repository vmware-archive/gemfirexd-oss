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
public class MultiColumnRoutingObjectInfo extends AbstractRoutingObjectInfo {

  private ColumnRoutingObjectInfo[] allPartitoningColumnsInfo;

  public MultiColumnRoutingObjectInfo(ColumnRoutingObjectInfo[] allColumnInfos) {
    super(-1, null, null);
    this.allPartitoningColumnsInfo = allColumnInfos;
  }

  public MultiColumnRoutingObjectInfo() {

  }

  public int computeHashCode(int hash, int resolverType,
      boolean requiresSerializedHash) {
    for (int i = 0; i < this.allPartitoningColumnsInfo.length; i++) {
      hash = this.allPartitoningColumnsInfo[i].computeHashCode(hash,
          resolverType, requiresSerializedHash);
    }
    return hash;
  }

  public void setActualValue(Object[] parameters_, Converter crossConverter) throws SQLException {
    for (int i = 0; i < this.allPartitoningColumnsInfo.length; i++) {
      ColumnRoutingObjectInfo cr = this.allPartitoningColumnsInfo[i];
      cr.setActualValue(parameters_, crossConverter);
    }
  }

  public int getTypeFormatId() {
    // TODO Auto-generated method stub
    return 0;
  }

  public Object getActualValue() {
    // TODO: KN should not be called throw proper exception.
    return null;
  }

  public int dvdEquivalenthashCode() {
    // TODO Auto-generated method stub
    return 0;
  }

  public void writeExternal(ObjectOutput out) throws IOException {
    super.writeExternal(out);
    out.writeObject(this.allPartitoningColumnsInfo);
  }

  public void readExternal(ObjectInput in) throws IOException,
      ClassNotFoundException {
    super.readExternal(in);
    Object obj = in.readObject();
    this.allPartitoningColumnsInfo = (ColumnRoutingObjectInfo[])obj;
  }
  
  public static void dummy() {
  }
}
