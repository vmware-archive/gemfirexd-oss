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
/**
 * 
 */
package com.pivotal.gemfirexd.internal.engine.access.index;

import java.util.Properties;

import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.shared.common.reference.SQLState;

/**
 * @author yjing
 * 
 */
public final class Hash1Index extends MemIndex {

  /**
   * 
   */
  public Hash1Index() {
  }

  @Override
  protected void allocateMemory(Properties properties, int tmpFlag)
      throws StandardException {
    // hash index does not support case-sensitivity setting
    if (properties.containsKey(GfxdConstants.INDEX_CASE_SENSITIVE_PROP)) {
      throw StandardException.newException(
          SQLState.INDEX_CASE_INSENSITIVITY_NOT_SUPPORTED,
          "primary key constraint");
    }
  }

  @Override
  public boolean requiresContainer() {
    return false;
  }

  public int getType() {
    return HASH1INDEX;
  }

  @Override
  public String getIndexTypeName() {
    return LOCAL_HASH_INDEX;
  }

  /* (non-Javadoc)
   *
   */
  @Override
  protected MemIndexCostController newMemIndexCostController() {
    MemIndexCostController costController = new Hash1IndexCostController();
    return costController;
  }

  /* (non-Javadoc)
   * 
   */
  @Override
  protected MemIndexScanController newMemIndexScanController() {
    MemIndexScanController scanController = new Hash1IndexScanController();
    return scanController;
  }

  @Override
  public void dumpIndex(String marker) {
    // nothing to be done
  }
}
