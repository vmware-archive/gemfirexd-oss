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

package com.pivotal.gemfirexd.internal.shared.common.error;

import java.sql.SQLException;

import com.pivotal.gemfirexd.internal.shared.common.reference.SQLState;

/**
 * Common interface for creating new exceptions on client and server side.
 * 
 * @author swale
 * @since gfxd 1.1
 */
public interface ExceptionFactory {

  /**
   * Get an appropriate SQLException given message, error code, next exception
   * and cause.
   */
  public SQLException getSQLException(String message, String sqlState,
      int errCode, SQLException next, Throwable t);

  /**
   * Get an appropriate SQLException given messageId (from {@link SQLState}),
   * next exception, errorCode, cause and arguments
   */
  public SQLException getSQLException(String messageId, SQLException next,
      Throwable t, Object... args);
}
