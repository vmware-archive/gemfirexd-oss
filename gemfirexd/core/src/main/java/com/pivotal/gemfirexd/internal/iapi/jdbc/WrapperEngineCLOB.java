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

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

/**
 * @author swale
 * 
 */
public class WrapperEngineCLOB extends WrapperEngineLOB implements Clob {

  private final Clob clob;

  public WrapperEngineCLOB(final EngineConnection localConn, final Clob clob) {
    super(localConn);
    this.clob = clob;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long length() throws SQLException {
    return this.clob.length();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getSubString(long pos, int length) throws SQLException {
    return this.clob.getSubString(pos, length);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Reader getCharacterStream() throws SQLException {
    return this.clob.getCharacterStream();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public InputStream getAsciiStream() throws SQLException {
    return this.clob.getAsciiStream();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long position(String searchstr, long start) throws SQLException {
    return this.clob.position(searchstr, start);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long position(Clob searchstr, long start) throws SQLException {
    return this.clob.position(searchstr, start);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int setString(long pos, String str) throws SQLException {
    return this.clob.setString(pos, str);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int setString(long pos, String str, int offset, int len)
      throws SQLException {
    return this.clob.setString(pos, str, offset, len);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public OutputStream setAsciiStream(long pos) throws SQLException {
    return this.clob.setAsciiStream(pos);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Writer setCharacterStream(long pos) throws SQLException {
    return this.clob.setCharacterStream(pos);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void truncate(long len) throws SQLException {
    this.clob.truncate(len);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Reader getCharacterStream(long pos, long length) throws SQLException {
    return this.clob.getCharacterStream(pos, length);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void free() throws SQLException {
    super.free();
    try {
      this.clob.free();
    } catch (SQLFeatureNotSupportedException ignore) {
      // ignore if free is not supported by the Blob implementation
    }
  }
}
