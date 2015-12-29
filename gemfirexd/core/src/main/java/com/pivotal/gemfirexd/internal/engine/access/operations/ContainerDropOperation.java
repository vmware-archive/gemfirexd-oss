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

package com.pivotal.gemfirexd.internal.engine.access.operations;

import java.io.IOException;

import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.io.LimitObjectInput;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.store.raw.Compensation;
import com.pivotal.gemfirexd.internal.iapi.store.raw.ContainerKey;
import com.pivotal.gemfirexd.internal.iapi.store.raw.Transaction;
import com.pivotal.gemfirexd.internal.iapi.store.raw.log.LogInstant;

/**
 * Class to encapsulate a {@link GemFireContainer} drop operation.
 * 
 * @author swale
 */
public final class ContainerDropOperation extends MemOperation {

  private final ContainerKey containerKey;

  private final boolean ignoreRegionDestroyed;

  private boolean isNoOp;

  public ContainerDropOperation(ContainerKey containerId,
      boolean ignoreRegionDestroyed) {
    super(null);
    this.containerKey = containerId;
    this.ignoreRegionDestroyed = ignoreRegionDestroyed;
    this.memcontainer = Misc.getMemStore().getContainer(this.containerKey);
  }

  @Override
  public void doMe(Transaction xact, LogInstant instant, LimitObjectInput in)
      throws StandardException, IOException {
    if (!this.isNoOp) {
      // region may still not have been created during undo
      // i.e. GemFireContainer#initialize may still not have been invoked,
      // so this flag is set by the CreateContainerOperation#generateUndo
      // to ignore such a case
      if (this.ignoreRegionDestroyed) {
        try {
          doMe(xact, this.containerKey);
        } catch (RegionDestroyedException ex) {
          // expected for this case
          if (GemFireXDUtils.TraceConglom) {
            SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_CONGLOM,
                "ContainerDropOperation#doMe: got ignorable "
                    + "RegionDestroyedException for container "
                    + this.memcontainer + ": " + ex.toString());
          }
        }
      }
      else {
        doMe(xact, this.containerKey);
      }
    }
  }

  public static void doMe(Transaction xact, ContainerKey containerKey)
      throws StandardException {
    Misc.getMemStore().dropConglomerate(xact, containerKey);
  }

  @Override
  public boolean doAtCommitOrAbort() {
    return (this.memcontainer == null || this.memcontainer.getBaseId() != null);
  }

  @Override
  public Compensation generateUndo(Transaction xact, LimitObjectInput in)
      throws StandardException, IOException {
    if (this.memcontainer == null || this.memcontainer.getBaseId() != null) {
      // for this just skip doing anything useful during abort
      this.isNoOp = true;
      return null;
    }
    else {
      throw new UnsupportedOperationException("ContainerDropOperation: undo "
          + "unimplemented; require GFE region rename support");
    }
  }

  @Override
  public boolean shouldBeConflated() {
    return (this.memcontainer != null);
  }

  @Override
  protected StringBuilder toStringBuilder(StringBuilder sb, String regionName) {
    return super.toStringBuilder(sb, regionName).append(" containerKey=")
        .append(this.containerKey).append(" ignoreRegionDestroyed=").append(
            this.ignoreRegionDestroyed).append(" isNoOp=").append(this.isNoOp);
  }
}
