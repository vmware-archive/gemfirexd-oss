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

import com.pivotal.gemfirexd.internal.shared.common.i18n.SQLMessageFormat;

/**
 * Default JDBC 3.0 SQLException factory implementation used for client/server.
 * 
 * @author swale
 * @since gfxd 1.1
 */
public class DefaultExceptionFactory30 implements ExceptionFactory {

  protected final SQLMessageFormat formatter;

  public DefaultExceptionFactory30(SQLMessageFormat formatter) {
    this.formatter = formatter;
  }

  /**
   * {@inheritDoc}
   */
  public SQLException getSQLException(String message, String sqlState,
      int errCode, SQLException next, Throwable t) {
    SQLException sqle = new SQLException(
        getMessage(message, sqlState, errCode), sqlState, errCode);
    if (next != null) {
      sqle.setNextException(next);
    }
    if (t != null) {
      sqle.initCause(t);
    }
    return sqle;
  }

  /**
   * {@inheritDoc}
   */
  public SQLException getSQLException(String messageId, SQLException next,
      Throwable t, Object... args) {
    final String message = this.formatter.getCompleteMessage(messageId, args);
    final String sqlState = ExceptionUtil.getSQLStateFromIdentifier(messageId);
    final int errCode = ExceptionUtil.getSeverityFromIdentifier(messageId);

    return getSQLException(message, sqlState, errCode, next, t);
  }

  protected String getMessage(String message, String sqlState, int errCode) {
    if (message.contains("SQLSTATE=")) {
      return message;
    } else {
      return "(SQLState=" + sqlState + " Severity=" + errCode + ") " + message;
    }
  }
}
