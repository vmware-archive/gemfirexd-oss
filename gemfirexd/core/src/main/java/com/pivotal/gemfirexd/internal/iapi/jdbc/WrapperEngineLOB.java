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

package com.pivotal.gemfirexd.internal.iapi.jdbc;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;

/**
 * Base class to encapsulate {@link Blob}, {@link Clob} for {@link EngineLOB}
 * interface.
 * 
 * @author swale
 * @since 7.0
 */
public abstract class WrapperEngineLOB implements EngineLOB {

  private final EngineConnection localConn;

  private int locator;

  public WrapperEngineLOB(final EngineConnection localConn) {
    this.localConn = localConn;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final int getLocator() throws SQLException {
    if (this.locator == 0) {
      this.locator = this.localConn.addLOBMapping(this);
    }
    return this.locator;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void free() throws SQLException {
    if (this.locator != 0) {
      this.localConn.removeLOBMapping(this.locator);
    }
  }
}
