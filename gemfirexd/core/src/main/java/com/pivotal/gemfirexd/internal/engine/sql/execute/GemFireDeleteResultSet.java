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

package com.pivotal.gemfirexd.internal.engine.sql.execute;

import com.pivotal.gemfirexd.internal.engine.distributed.ReferencedKeyCheckerMessage;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.impl.sql.execute.TriggerEvent;
import com.pivotal.gemfirexd.internal.impl.sql.execute.xplain.XPLAINUtil;

public final class GemFireDeleteResultSet extends GemFireUpdateResultSet {

  GemFireDeleteResultSet(AbstractGemFireActivation act)
      throws StandardException {
    super(act);
  }

  @Override
  public void setup(final Object results, int numMembers)
      throws StandardException {
    final long beginTime = statisticsTimingOn ? XPLAINUtil.recordTiming(-1) : 0;

    this.numRowsModified = results != null ? ((Integer)results).intValue() : 0;

    if (statisticsTimingOn) {
      nextTime = XPLAINUtil.recordTiming(beginTime);
    }
  }

  @Override
  protected void openContainers() throws StandardException {
    super.openContainers();
    final GemFireContainer[] refContainers = this.gfContainer
        .getExtraTableInfo().getReferencedContainers();
    if (refContainers != null) {
      ReferencedKeyCheckerMessage.openReferencedContainersForRead(this.tran,
          refContainers);
    }
  }

  @Override
  public void finishResultSet(final boolean cleanupOnError) throws StandardException {
    final GemFireContainer[] refContainers = this.gfContainer
        .getExtraTableInfo().getReferencedContainers();
    if (refContainers != null) {
      ReferencedKeyCheckerMessage.closeReferencedContainersAfterRead(
          this.tran, refContainers);
    }
    super.finishResultSet(cleanupOnError);
  }

  @Override
  int getAfterTriggerEvent() {
    return TriggerEvent.AFTER_DELETE;
  }
}
