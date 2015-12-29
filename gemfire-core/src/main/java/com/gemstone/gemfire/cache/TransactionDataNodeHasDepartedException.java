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

/**
 * Thrown when the transactional data host has shutdown or no longer has the data
 * being modified by the transaction.
 * This can be thrown while doing transactional operations or during commit.
 *
 * <p>This exception only occurs when a transaction
 * is hosted on a member that is not
 * the initiator of the transaction.
 *
 * @author gregp
 * @since 6.5
 */
public class TransactionDataNodeHasDepartedException extends TransactionException {
  
  private static final long serialVersionUID = -2217135580436381984L;

  public TransactionDataNodeHasDepartedException(String s) {
    super(s);
  }
  
  public TransactionDataNodeHasDepartedException(Throwable e) {
    super(e);
  }
}
