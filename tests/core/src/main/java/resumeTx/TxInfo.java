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
package resumeTx;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.internal.InternalDataSerializer;

/** 
 * Represents an active transation.  Includes txId, number of suspend/resumes (executeTxOps), and filter info
 * (region, key and routing object).  Must be serializable to be maintained on the BB. 
 */
public class TxInfo implements DataSerializable {
   /**
   * 
   */
  private static final long serialVersionUID = 5222971164824200765L;
  TransactionId txId;
   int numExecutions;
   String regionName;
   Object key;
   ModRoutingObject routingObject;

   public TxInfo() {
      this.numExecutions = 0;
   }

   public TransactionId getTxId() {
      return this.txId;
   }

   public int getNumExecutions() {
      return this.numExecutions;
   }

   public String getRegionName() {
      return this.regionName;
   }

   public Object getKey() {
      return this.key;
   }

   public ModRoutingObject getRoutingObject() {
      return this.routingObject;
   } 

   public void setTxId(TransactionId txId) {
      this.txId = txId;
   }

   public void setNumExecutions(int executions) {
      this.numExecutions = executions;
   }

   public void incrementNumExecutions() {
      setNumExecutions(++this.numExecutions);
   }

   public void setRegionName(String regionName) {
      this.regionName = regionName;
   } 

   public void setKey(Object aKey) {
      this.key = aKey;
   }

   public void setRoutingObject(ModRoutingObject aRoutingObject) {
      this.routingObject = aRoutingObject;
   }
   
   @Override
   public void toData(DataOutput out) throws IOException {
     DataSerializer.writeObject(this.txId, out);
     DataSerializer.writeInteger(this.numExecutions, out);
     DataSerializer.writeString(this.regionName, out);
     DataSerializer.writeObject(this.key, out);
     DataSerializer.writeObject(this.routingObject, out);
   }

   @Override
   public void fromData(DataInput in) 
     throws IOException, ClassNotFoundException {
     this.txId = DataSerializer.readObject(in);
     this.numExecutions = DataSerializer.readInteger(in);
     this.regionName = DataSerializer.readString(in);
     this.key = DataSerializer.readObject(in);
     this.routingObject = DataSerializer.readObject(in);
   }
   
   public String toString() {
      StringBuffer aStr = new StringBuffer();
      aStr.append("TxInfo {" + txId + ", numExecutions: " + numExecutions + ", FilterInfo {" + regionName + ", " + key + ", " + routingObject + "}");
      return aStr.toString();
   }
}
