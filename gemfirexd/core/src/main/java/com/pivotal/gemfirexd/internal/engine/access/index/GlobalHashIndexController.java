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

package com.pivotal.gemfirexd.internal.engine.access.index;

import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.pivotal.gemfirexd.internal.engine.access.GemFireTransaction;
import com.pivotal.gemfirexd.internal.engine.access.MemConglomerate;
import com.pivotal.gemfirexd.internal.engine.access.operations.GlobalHashIndexInsertOperation;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;

final class GlobalHashIndexController extends MemIndexController {

  GlobalHashIndexController() {
  }

  @Override
  public int getType() {
    return MemConglomerate.GLOBALHASHINDEX;
  }

  @Override
  protected void postInitialize() {
    // get index region
    GemFireContainer container = this.open_conglom.getGemFireContainer();
    assert container != null && container.getRegion() != null
        && container.getRegion() instanceof PartitionedRegion;
  }

  @Override
  protected int doInsert(DataValueDescriptor[] row) throws StandardException {
    // currently cloning the row always though this can be avoided if
    // this insert is from an accessor since it will only go to remote
    // nodes but avoiding cloning in that case may not buy much
    final Object key = this.open_conglom.getKey(row, true, false);
    final AbstractRowLocation baseRowLoc = (AbstractRowLocation)this.open_conglom
        .getValue(row);
    assert baseRowLoc instanceof GlobalExecRowLocation;
    final GemFireContainer container = this.open_conglom.getBaseContainer();
    GlobalExecRowLocation baseRowLocation = (GlobalExecRowLocation)baseRowLoc;
    AbstractRowLocation rowLocation = baseRowLocation.getRowLocation(container);

    final GemFireTransaction tran = this.open_conglom.getTransaction();
    GlobalHashIndexInsertOperation.doMe(tran, container.getActiveTXState(tran),
        this.open_conglom.getGemFireContainer(), key, rowLocation,
        false /*isPutDML*/, false /*wasPutDML*/);
    return 0;
  }
}
