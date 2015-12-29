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

package com.pivotal.gemfirexd.thrift.common;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;

import com.pivotal.gemfirexd.thrift.BlobChunk;
import com.pivotal.gemfirexd.thrift.ClobChunk;

/**
 * Interface for getting LOB chunks from remote server.
 * 
 * @author swale
 */
public interface LobService {

  public Blob createBlob(BlobChunk firstChunk) throws SQLException;

  public Clob createClob(ClobChunk firstChunk) throws SQLException;

  public BlobChunk getBlobChunk(int lobId, long offset, int chunkSize,
      boolean freeLobAtEnd) throws SQLException;

  public ClobChunk getClobChunk(int lobId, long offset, int chunkSize,
      boolean freeLobAtEnd) throws SQLException;
}
