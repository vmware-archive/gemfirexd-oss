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
import java.util.Arrays;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;

public class ActiveTxWrapper implements DataSerializable {
  
  /**
   * 
   */
  private static final long serialVersionUID = 5613663162155998692L;
  byte[] serializedTXId = null; 
  
  public ActiveTxWrapper() {
    
  }
  
  public ActiveTxWrapper(byte[] serializedTXId) {
    this.serializedTXId = serializedTXId;
  }
  
  /**
   * This method should be used to obtain instances of KeyWrapper.
   * @param key the key to wrap
   * @return an instance of KeyWrapper that can be used as a key in Region/Map
   */
  public static ActiveTxWrapper getWrappedActiveTx(byte[] serializedTXId) {
    return new ActiveTxWrapper(serializedTXId);
  }

  public byte[] getSerializedTXId() {
    return this.serializedTXId;
  }
  
  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeByteArray(this.serializedTXId, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.serializedTXId = DataSerializer.readByteArray(in);
  }


  @Override
  public boolean equals(Object obj) {
    if (obj instanceof ActiveTxWrapper) {
      ActiveTxWrapper other = (ActiveTxWrapper) obj;
      return Arrays.equals(this.serializedTXId, other.serializedTXId);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(this.serializedTXId);
  }
  
  @Override
  public String toString() {
    StringBuilder str = new StringBuilder();
    str.append(getClass().getCanonicalName()).append("@").append(System.identityHashCode(this));
    str.append(" key:").append(Arrays.toString(this.serializedTXId));
    str.append(" hashCode:").append(hashCode());
    return str.toString();
  }
  
}
