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

public class TxInfoWrapper implements DataSerializable {

  /**
   * 
   */
  private static final long serialVersionUID = 5231864547262896266L;
  /**
   * the key being wrapped
   */
  private byte[] txInfoByteArray;

  public TxInfoWrapper() {
  }

  private TxInfoWrapper(byte[] txInfoByteArray) {
    this.txInfoByteArray = txInfoByteArray;
  }

  /**
   * This method should be used to obtain instances of KeyWrapper.
   * @param key the key to wrap
   * @return an instance of KeyWrapper that can be used as a key in Region/Map
   */
  public static TxInfoWrapper getWrappedTxInfo(byte[] txInfoByteArray) {
    return new TxInfoWrapper(txInfoByteArray);
  }

  public byte[] getTxInfoByteArray() {
    return this.txInfoByteArray;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeByteArray(this.txInfoByteArray, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.txInfoByteArray = DataSerializer.readByteArray(in);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof TxInfoWrapper) {
      TxInfoWrapper other = (TxInfoWrapper) obj;
      return Arrays.equals(this.txInfoByteArray, other.txInfoByteArray);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(this.txInfoByteArray);
  }
  
  @Override
  public String toString() {
    StringBuilder str = new StringBuilder();
    str.append(getClass().getCanonicalName()).append("@").append(System.identityHashCode(this));
    str.append(" txInfoByteArray:").append(Arrays.toString(this.txInfoByteArray));
    str.append(" hashCode:").append(hashCode());
    return str.toString();
  }

}
