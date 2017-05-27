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
package com.gemstone.gemfire.internal.cache.persistence.soplog.nofile;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map.Entry;
import java.util.NavigableMap;

import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedOplog.SortedOplogWriter;
import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedOplogFactory.SortedOplogConfiguration;
import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedReader;
import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedReaderTestCase;

public class NoFileSortedOplogJUnitTest extends SortedReaderTestCase {
  private NoFileSortedOplog mfile;

  @Override
  protected SortedReader<ByteBuffer> createReader(NavigableMap<byte[], byte[]> data) throws IOException {
    mfile = new NoFileSortedOplog(new SortedOplogConfiguration("nofile"));

    SortedOplogWriter wtr = mfile.createWriter();
    for (Entry<byte[], byte[]> entry : data.entrySet()) {
      wtr.append(entry.getKey(), entry.getValue());
    }
    wtr.close(null);

    return mfile.createReader();
  }
}
