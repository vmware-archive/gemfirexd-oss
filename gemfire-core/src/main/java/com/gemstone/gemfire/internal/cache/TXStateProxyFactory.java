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

package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.cache.IsolationLevel;
import com.gemstone.gemfire.cache.TransactionFlag;
import java.util.EnumSet;

/**
 * Factory to allow GFXD layer to create custom extensions to
 * {@link TXStateProxy}.
 * 
 * @author swale
 * @since 7.0
 */
public interface TXStateProxyFactory {

  /**
   * Create an instance of {@link TXStateProxy}.
   */
  public TXStateProxy newTXStateProxy(TXManagerImpl txMgr, TXId txId,
      IsolationLevel isolationLevel, boolean isJTA,
      EnumSet<TransactionFlag> flags, boolean initLocalTXState);
}
