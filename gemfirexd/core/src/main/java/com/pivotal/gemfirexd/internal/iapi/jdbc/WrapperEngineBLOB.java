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
import java.sql.Blob;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

/**
 * Encapsulate {@link Blob} for {@link EngineLOB} interface.
 * 
 * @author swale
 * @since 7.0
 */
public class WrapperEngineBLOB extends WrapperEngineLOB implements Blob {

  private final Blob blob;

  public WrapperEngineBLOB(final EngineConnection localConn, final Blob blob) {
    super(localConn);
    this.blob = blob;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long length() throws SQLException {
    return this.blob.length();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public byte[] getBytes(long pos, int length) throws SQLException {
    return this.blob.getBytes(pos, length);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public InputStream getBinaryStream() throws SQLException {
    return this.blob.getBinaryStream();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long position(byte[] pattern, long start) throws SQLException {
    return this.blob.position(pattern, start);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long position(Blob pattern, long start) throws SQLException {
    return this.blob.position(pattern, start);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int setBytes(long pos, byte[] bytes) throws SQLException {
    return this.blob.setBytes(pos, bytes);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int setBytes(long pos, byte[] bytes, int offset, int len)
      throws SQLException {
    return this.blob.setBytes(pos, bytes, offset, len);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public OutputStream setBinaryStream(long pos) throws SQLException {
    return this.blob.setBinaryStream(pos);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void truncate(long len) throws SQLException {
    this.blob.truncate(len);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public InputStream getBinaryStream(long pos, long length) throws SQLException {
    return this.blob.getBinaryStream(pos, length);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void free() throws SQLException {
    super.free();
    try {
      this.blob.free();
    } catch (SQLFeatureNotSupportedException ignore) {
      // ignore if free is not supported by the Blob implementation
    }
  }
}
