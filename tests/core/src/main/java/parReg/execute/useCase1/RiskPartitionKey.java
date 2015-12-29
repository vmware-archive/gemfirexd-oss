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
package parReg.execute.useCase1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.EntryOperation;
import com.gemstone.gemfire.cache.PartitionResolver;

public class RiskPartitionKey implements DataSerializable, PartitionResolver {

  public final static int TYPE_POSITION = 0;

  public final static int TYPE_POSITION_RISK = 1;

  public final static int TYPE_INSTRUMENT = 2;

  public final static int TYPE_INSTRUMENT_RISK_SENSITIVITY = 3;

  public static final int TYPE_UND_RISK_SENSITIVITY = 4;

  private String key;

  private Integer id_imnt;

  private int type;

  public RiskPartitionKey() {

  }

  public RiskPartitionKey(String k, int id, int t) {
    this.key = k;
    this.id_imnt = new Integer(id);
    this.type = t;
  }

  public String getKey() {
    return key;
  }

  public Integer getInstrumentId() {
    return id_imnt;
  }

  public int getType() {
    return type;
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {

    key = DataSerializer.readString(in);
    id_imnt = DataSerializer.readInteger(in);
    type = in.readInt();
  }

  public void toData(DataOutput out) throws IOException {

    DataSerializer.writeString(key, out);
    DataSerializer.writeInteger(id_imnt, out);
    out.writeInt(type);
  }

  public String getName() {
    return null;
  }

//  public Properties getProperties() {
//    return null;
//  }

  public Serializable getRoutingObject(EntryOperation opDetails) {
    RiskPartitionKey key = (RiskPartitionKey)opDetails.getKey();
    hydra.Log.getLogWriter().info(
        "Returning getRoutingObject as " + key.id_imnt.toString());
    return key.id_imnt;
  }

  public boolean equals(Object obj) {
    if (obj instanceof RiskPartitionKey) {
      if (this.id_imnt.intValue() == ((RiskPartitionKey)obj).id_imnt.intValue()
          && this.key.equals(((RiskPartitionKey)obj).key)
          && this.type == ((RiskPartitionKey)obj).type) {
        return true;
      }
      else {
        return false;
      }
    }
    else
      return false;
  }

  public void close() {

  }

  public int hashCode() {
    return this.key.hashCode() + this.id_imnt.hashCode() + this.type;
  }

  public String toString() {
    return key + "|" + id_imnt + "|" + type;
  }

//  public void init(Properties props) {
//
//  }
}
