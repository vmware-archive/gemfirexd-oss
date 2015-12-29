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
package com.pivotal.gemfirexd.internal.engine.distributed.metadata;

import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.engine.store.RegionKey;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;

/**
 * Concrete clas representing the parameterized Composite Key 
 * (multiple Keys).
 * 
 * @since Sql Fabric
 * @author Asif
 * @see DynamicKey
 * @see PrimaryDynamicKey
 */
public class CompositeDynamicKey implements DynamicKey {

  private final Object[] pks;

  public CompositeDynamicKey(Object cks) {
    this.pks = (Object[])cks;
  }
  
  @Override
  public DataValueDescriptor[] getEvaluatedIndexKey(Activation activation)
      throws StandardException {
    DataValueDescriptor[] iks = getEvaluatedKey(activation);
    return iks;
  }

  private DataValueDescriptor[] getEvaluatedKey(Activation activation)
      throws StandardException {
    // Copy the array and at the same time replace the parameters with the
    // values
    int len = this.pks.length;
    // TODO: SW: for byte array store, avoid creating DVD[] and create
    /// the key directly from the DVDs incrementally
    DataValueDescriptor[] copyPks = new DataValueDescriptor[len];
    for (int i = 0; i < len; ++i) {
      Object temp = this.pks[i];
      DataValueDescriptor key = null;
      if (temp instanceof DynamicKey) {
        assert temp instanceof PrimaryDynamicKey;
        PrimaryDynamicKey pdk = (PrimaryDynamicKey)temp;
        key = pdk.getEvaluatedAsDataValueDescriptor(activation);
      }
      else {
        key = (DataValueDescriptor)temp;
      }
      copyPks[i] = key;
    }

    return copyPks;
  }
  
  @Override
  public RegionKey getEvaluatedPrimaryKey(Activation activation,
      GemFireContainer container, boolean doClone) throws StandardException {
    DataValueDescriptor[] copyPks = getEvaluatedKey(activation);

    // GemFireXDUtils.getBytesForCompositeKeys(copyPks);
    return GemFireXDUtils
        .convertIntoGemfireRegionKey(copyPks, container, doClone);
  }
}
