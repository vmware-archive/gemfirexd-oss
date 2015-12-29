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
 * Indicates that an unexpected runtime exception occurred
 * during a cache operation on the transactional data host.
 *
 * <p>This exception only occurs when a transaction
 * is hosted on a member that is not
 * the initiator of the transaction.
 *
 * @author gregp
 * @since 6.5
 * @deprecated as of 6.6 exceptions from a remote node are no longer wrapped in this exception.  Instead of this, {@link TransactionDataNodeHasDepartedException} is thrown.
 */
public class RemoteTransactionException extends TransactionException {
  
  private static final long serialVersionUID = -2217135580436381984L;

  public RemoteTransactionException(String s) {
    super(s);
  }
  
  public RemoteTransactionException(Exception e) {
    super(e);
  }
}
