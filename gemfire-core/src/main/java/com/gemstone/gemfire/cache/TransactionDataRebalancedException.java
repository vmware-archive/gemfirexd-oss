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
package com.gemstone.gemfire.cache;

import com.gemstone.gemfire.cache.control.RebalanceOperation;

/**
 * Thrown when a {@link RebalanceOperation} occurs concurrently with a transaction.
 * This can be thrown while doing transactional operations or during commit.
 *
 * <p>This exception only occurs when a transaction
 * involves partitioned regions or if region is destroyed during transaction.
 * 
 * @author gregp
 * @since 6.6
 */
public class TransactionDataRebalancedException extends TransactionException {

  private static final long serialVersionUID = -2217135580436381984L;

  private String regionPath;

  public TransactionDataRebalancedException(String s, String regionPath) {
    super(s);
    this.regionPath = regionPath;
  }

  public String getRegionPath() {
    return this.regionPath;
  }
}
