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

package com.pivotal.gemfirexd.internal.impl.tools.ij;

import java.sql.SQLException;
import java.sql.SQLWarning;

/**
 * Extension to indicate result of help command in ij.
 * 
 * @author swale
 * @since 7.0
 */
class gfxdHelpResult extends ijResultImpl {

  private final String message;

  private final boolean pageResult;

  gfxdHelpResult(final String message, final boolean pageResult) {
    this.message = message;
    this.pageResult = pageResult;
  }

  @Override
  public final boolean isHelp() {
    return true;
  }

  @Override
  public final String getHelpMessage() {
    return this.message;
  }

  @Override
  public final boolean pageResult() {
    return this.pageResult;
  }

  @Override
  public SQLWarning getSQLWarnings() throws SQLException {
    return null;
  }

  @Override
  public void clearSQLWarnings() throws SQLException {
  }
}
