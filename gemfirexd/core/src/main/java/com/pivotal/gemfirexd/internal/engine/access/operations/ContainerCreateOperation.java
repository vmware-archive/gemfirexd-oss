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
import java.util.Properties;

import com.gemstone.gemfire.cache.RegionAttributes;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.access.GemFireTransaction;
import com.pivotal.gemfirexd.internal.engine.access.MemConglomerate;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.io.LimitObjectInput;
import com.pivotal.gemfirexd.internal.iapi.store.raw.Compensation;
import com.pivotal.gemfirexd.internal.iapi.store.raw.ContainerKey;
import com.pivotal.gemfirexd.internal.iapi.store.raw.LockingPolicy;
import com.pivotal.gemfirexd.internal.iapi.store.raw.Transaction;
import com.pivotal.gemfirexd.internal.iapi.store.raw.log.LogInstant;

/**
 * Class to encapsulate a {@link GemFireContainer} create operation.
 * 
 * @author swale
 */
public final class ContainerCreateOperation extends MemOperation {

  private final MemConglomerate conglom;

  private final Properties containerProps;

  public ContainerCreateOperation(MemConglomerate conglom, Properties props) {
    super(null);
    this.conglom = conglom;
    this.containerProps = props;
  }

  @Override
  public void doMe(Transaction xact, LogInstant instant, LimitObjectInput in)
      throws StandardException, IOException {
    this.memcontainer = doMe((GemFireTransaction)xact, this.conglom,
        this.containerProps);
  }

  public static GemFireContainer doMe(GemFireTransaction tran,
      MemConglomerate conglom, Properties properties) throws StandardException {
    final ContainerKey containerKey = conglom.getId();
    final GemFireContainer container;
    if (conglom.requiresContainer()) {
      if (!tran.skipLocks(conglom, null)) {
        final LockingPolicy locking = GemFireContainer
            .getContainerLockingPolicy(containerKey.getContainerId(),
                properties, tran.getLanguageConnectionContext());
        if (locking != null) {
          // take the write lock (distributed lock for non-temporary tables)
          // before creating the container
          locking.lockContainer(tran, null, true, true);
        }
      }
      container = new GemFireContainer(containerKey, properties);
      RegionAttributes<?, ?> attrs = container.getRegionAttributes();
      if (container.getBaseId() != null
          || attrs == null
          || (container.getSchemaName().equalsIgnoreCase(
              GfxdConstants.SYSTEM_SCHEMA_NAME) || container.getSchemaName()
              .equalsIgnoreCase(GfxdConstants.SESSION_SCHEMA_NAME)
              && attrs.getScope().isLocal())) {
        container.initialize(properties, null, null, null);
      }
    }
    else {
      container = null;
    }
    Misc.getMemStore().addConglomerate(containerKey, conglom);
    if (container != null) {
      container.setConglomerate(conglom);
      conglom.setGemFireContainer(container);
    }
    // the write lock on container is released at the end of transaction
    return container;
  }

  @Override
  public Compensation generateUndo(Transaction xact, LimitObjectInput in)
      throws StandardException, IOException {
    return new ContainerDropOperation(this.conglom.getId(), true);
  }

  @Override
  protected StringBuilder toStringBuilder(StringBuilder sb, String regionName) {
    return super.toStringBuilder(sb, regionName).append(" containerKey=")
        .append(this.conglom.getId()).append(" containerProps=").append(
            this.containerProps);
  }
}
