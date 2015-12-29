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

import cacheperf.comparisons.replicated.delta.DeltaPrms;
import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.Delta;
import com.gemstone.gemfire.Instantiator;
import com.gemstone.gemfire.InvalidDeltaException;
import com.gemstone.gemfire.cache.util.ObjectSizer;
import hydra.Log;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;

/**
 * A {@link objects.FastAssetAccount} that implements the Delta interface.
 */
public class DeltaFastAssetAccount extends FastAssetAccount implements ConfigurableObject, TimestampedObject, ObjectSizer, DataSerializable, Delta, Cloneable {

  // INSTANTIATORS DO NOT WORK ON GATEWAYS due to bug 35646
  static {
    Instantiator.register(new Instantiator(DeltaFastAssetAccount.class, (byte)41) {
      public DataSerializable newInstance() {
        return new DeltaFastAssetAccount();
      }
    });
  }

  protected static final boolean encodeTimestamp =
            FastAssetAccountPrms.encodeTimestamp();

  protected static final boolean getBeforeUpdate =
            DeltaPrms.getBeforeUpdate();

  public DeltaFastAssetAccount() {
  }

//------------------------------------------------------------------------------
// Delta

  public boolean hasDelta() {
    if (fineEnabled) {
      Log.getLogWriter().info("INVOKED: hasDelta on key " + this.acctId);
    }
    return true;
  }

  public void toDelta(DataOutput out)
  throws IOException {
    out.writeDouble(this.netWorth);
    if (encodeTimestamp) {
      out.writeLong(this.timestamp);
    }
    if (fineEnabled) {
      Log.getLogWriter().info("INVOKED: toDelta on key " + this.acctId);
    }
  }

  public void fromDelta(DataInput in)
  throws IOException {
    if (getBeforeUpdate) {
      this.netWorth = in.readDouble();
    } else {
      this.netWorth += in.readDouble();
    }
    if (encodeTimestamp) {
      this.timestamp = in.readLong();
    }
    if (fineEnabled) {
      Log.getLogWriter().info("INVOKED: fromDelta on key " + this.acctId);
    }
  }

//------------------------------------------------------------------------------
// Cloneable

  /**
   * Makes a deep copy of this account.
   */
  public Object clone() throws CloneNotSupportedException {
    DeltaFastAssetAccount acct = (DeltaFastAssetAccount)super.clone();
    acct.assets = new HashMap();
    for (Iterator i = this.assets.keySet().iterator(); i.hasNext();) {
      Integer key = (Integer)i.next();
      FastAsset asset = (FastAsset)this.assets.get(key);
      acct.assets.put(key, asset.copy());
    }
    if (fineEnabled) {
      Log.getLogWriter().info("INVOKED: clone on key " + this.acctId);
    }
    return acct;
  }

//------------------------------------------------------------------------------
// miscellaneous

  public boolean equals(Object obj) {
    if (obj != null && obj instanceof DeltaFastAssetAccount) { 
      DeltaFastAssetAccount acct = (DeltaFastAssetAccount)obj;
      if (this.acctId == acct.acctId) {
        return true;
      }
    }
    return false;
  }
}
