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
package objects;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.Instantiator;
import com.gemstone.gemfire.cache.util.ObjectSizer;
import com.gemstone.gemfire.internal.NanoTimer;
import com.gemstone.gemfire.internal.util.Sizeof;
import hydra.BasePrms;
import hydra.Log;
import hydra.RemoteTestModule;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * An account containing assets, useful for queries.
 */
public class FastAssetAccount implements ConfigurableObject, TimestampedObject, UpdatableObject, ObjectSizer, DataSerializable {

  // INSTANTIATORS DO NOT WORK ON GATEWAYS due to bug 35646
  static {
    Instantiator.register(new Instantiator(FastAssetAccount.class, (byte)23) {
      public DataSerializable newInstance() {
        return new FastAssetAccount();
      }
    });
  }

  protected static boolean fineEnabled = Log.getLogWriter().fineEnabled();

  protected static final boolean encodeTimestamp =
            FastAssetAccountPrms.encodeTimestamp();
  protected int acctId;
  protected String customerName;
  protected double netWorth = 0.0;
  protected Map assets;
  protected long timestamp;

  public FastAssetAccount() {
  }

  public void init(int index) {
    this.acctId = index;
    this.customerName = "Milton Moneybags";

    this.assets = new HashMap();
    int size = FastAssetAccountPrms.getSize();
    this.netWorth = 0.0;
    for (int i = 0; i < size; i++) {
      FastAsset asset = new FastAsset();
      asset.init(i);
      this.assets.put(new Integer(i), asset);
      this.netWorth += asset.getValue();
    }

    if (encodeTimestamp) {
      this.timestamp = NanoTimer.getTime() - RemoteTestModule.getClockSkew();
    }
  }

  public int getAcctId() {
    return this.acctId;
  }

  public String getCustomerName() {
    return this.customerName;
  }

  public double getNetWorth() {
    return this.netWorth;
  }

  public void incrementNetWorth() {
    ++this.netWorth;
  }

  public Map getAssets(){
    return this.assets;
  }

//------------------------------------------------------------------------------
// ConfigurableObject

  public int getIndex() {
    return this.acctId;
  }

  public void validate( int index ) {
    String s = this.getClass().getName() + " does not support validation";
    throw new UnsupportedOperationException(s);
  }

//------------------------------------------------------------------------------
// TimestampedObject

  public long getTimestamp() {
    if (encodeTimestamp) {
      return this.timestamp;
    } else {
      String s = BasePrms.nameForKey(FastAssetAccountPrms.encodeTimestamp)
               + " is false, cannot get timestamp";
      throw new ObjectAccessException(s);
    }
  }

  public void resetTimestamp() {
    if (encodeTimestamp) {
      this.timestamp = NanoTimer.getTime() - RemoteTestModule.getClockSkew();
    } else {
      String s = BasePrms.nameForKey(FastAssetAccountPrms.encodeTimestamp)
               + " is false, cannot reset timestamp";
      throw new ObjectAccessException(s);
    }
  }

//------------------------------------------------------------------------------
// UpdatableObject

  public synchronized void update() {
    incrementNetWorth();
    if (encodeTimestamp) {
      resetTimestamp();
    }
  }

//------------------------------------------------------------------------------
// ObjectSizer

  public int sizeof(Object o) {
    if (o instanceof FastAssetAccount) {
      FastAssetAccount obj = (FastAssetAccount)o;
      int mapSize = 0;
      if (obj.assets != null) {
        for (Iterator i = obj.assets.keySet().iterator(); i.hasNext();) {
          Object key = i.next();
          FastAsset asset = (FastAsset)obj.assets.get(key);
          mapSize += Sizeof.sizeof(key) + Sizeof.sizeof(asset) + 16; // 32-bit
          // add guess at hashmap entry overhead
        }
      }
      return Sizeof.sizeof(obj.acctId)
                   + Sizeof.sizeof(obj.customerName)
                   + Sizeof.sizeof(obj.netWorth)
                   + mapSize
                   + Sizeof.sizeof(obj.timestamp);
    } else {
      return Sizeof.sizeof(o);
    }
  }

//------------------------------------------------------------------------------
// DataSerializable

  public void toData(DataOutput out)
  throws IOException {
    out.writeInt(this.acctId);
    DataSerializer.writeString(this.customerName, out);
    out.writeDouble(this.netWorth);
    DataSerializer.writeHashMap((HashMap)this.assets, out);
    out.writeLong(this.timestamp);
    if (fineEnabled) {
      Log.getLogWriter().fine("INVOKED: toData on key " + this.acctId);
    }
  }
  public void fromData(DataInput in)
  throws IOException, ClassNotFoundException {
    this.acctId = in.readInt();
    this.customerName = DataSerializer.readString(in);
    this.netWorth = in.readDouble();
    this.assets = DataSerializer.readHashMap(in);
    this.timestamp = in.readLong();
    if (fineEnabled) {
      Log.getLogWriter().fine("INVOKED: fromData on key " + this.acctId);
    }
  }

//------------------------------------------------------------------------------
// miscellaneous

  public boolean equals(Object obj) {
    if (obj != null && obj instanceof FastAssetAccount) { 
      FastAssetAccount acct = (FastAssetAccount)obj;
      if (this.acctId == acct.acctId) {
        return true;
      }
    }
    return false;
  }

  public int hashCode() {
    return this.acctId;
  }

  public String toString() {
    StringBuffer buf = new StringBuffer();
    buf.append(this.getClass().getName() + " [acctId=" + this.acctId
              + " customerName=" + this.customerName
              + " netWorth=" + this.netWorth
              + " timestamp=" + this.timestamp);
    for (Iterator i = assets.keySet().iterator(); i.hasNext();) {
      Object key = i.next();
      buf.append(" " + key + "=" + assets.get(key));
    }
    return buf.toString();
  }
}
