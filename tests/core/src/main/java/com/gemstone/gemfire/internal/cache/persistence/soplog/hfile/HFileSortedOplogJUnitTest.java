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
package com.gemstone.gemfire.internal.cache.persistence.soplog.hfile;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map.Entry;
import java.util.NavigableMap;

import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedOplog.SortedOplogWriter;
import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedOplogFactory.SortedOplogConfiguration;
import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedReader;
import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedReader.SortedIterator;
import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedReaderTestCase;
import com.gemstone.gemfire.internal.util.Bytes;


public class HFileSortedOplogJUnitTest extends SortedReaderTestCase {
  private HFileSortedOplog hfile;

  public void testMultiblockReverseScan() throws Exception {
    int entries = 10000;
    
    SortedOplogConfiguration config = new SortedOplogConfiguration("backwards");
    config.setBlockSize(4096);
    
    HFileSortedOplog rev = new HFileSortedOplog(new File("backwards.soplog"), config);
    SortedOplogWriter wtr = rev.createWriter();
    for (int i = 0; i < entries; i++) {
      byte[] value = new byte[1028];
      Bytes.putInt(i, value, 0);
      Arrays.fill(value, 4, value.length, (byte) 42);

      wtr.append(SortedReaderTestCase.wrapInt(i), value);
    }
    wtr.close(null);

    SortedReader<ByteBuffer> rdr = rev.createReader().withAscending(false);
    SortedIterator<ByteBuffer> iter = rdr.scan();
    try {
      for (int i = entries - 1; i >= 0; i--) {
        assertTrue(iter.hasNext());
        iter.next();
        
        assertEquals(i, iter.key().getInt());
        assertEquals(i, iter.value().getInt());
      }
    } finally {
      iter.close();
    }
    
    iter = rdr.scan(SortedReaderTestCase.wrapInt(4000), true, SortedReaderTestCase.wrapInt(1000), true);
    try {
      for (int i = 4000; i >= 1000; i--) {
        assertTrue(iter.hasNext());
        iter.next();
        
        assertEquals(i, iter.key().getInt());
        assertEquals(i, iter.value().getInt());
      }
    } finally {
      iter.close();
    }
    
    rdr.close();
  }
  
  public void testMultiblockReverseScan2() throws Exception {
    SortedOplogConfiguration config = new SortedOplogConfiguration("backwards2");
    config.setBlockSize(4096);
    
    HFileSortedOplog rev = new HFileSortedOplog(new File("backwards2.soplog"), config);
    SortedOplogWriter wtr = rev.createWriter();
    for (int i = 2000; i < 18000; i += 2) {
      byte[] value = new byte[1028];
      Bytes.putInt(i, value, 0);
      Arrays.fill(value, 4, value.length, (byte) 42);

      wtr.append(SortedReaderTestCase.wrapInt(i), value);
    }
    wtr.close(null);

    SortedReader<ByteBuffer> rdr = rev.createReader().withAscending(false);
    SortedIterator<ByteBuffer> iter = rdr.scan(
        SortedReaderTestCase.wrapInt(20000), true, 
        SortedReaderTestCase.wrapInt(0), true);
    try {
      for (int i = 17998; i >= 2000; i -= 2) {
        assertTrue(iter.hasNext());
        iter.next();
        
        assertEquals(i, iter.key().getInt());
        assertEquals(i, iter.value().getInt());
      }
    } finally {
      iter.close();
    }

    iter = rdr.scan(
        SortedReaderTestCase.wrapInt(17997), true, 
        SortedReaderTestCase.wrapInt(2001), true);
    try {
      for (int i = 17996; i >= 2002; i -= 2) {
        assertTrue(iter.hasNext());
        iter.next();
        
        assertEquals(i, iter.key().getInt());
        assertEquals(i, iter.value().getInt());
      }
    } finally {
      iter.close();
    }

    rdr.close();
  }
  
  @Override
  protected SortedReader<ByteBuffer> createReader(NavigableMap<byte[], byte[]> data) 
      throws IOException {
    SortedOplogConfiguration config = new SortedOplogConfiguration("test");
    hfile = new HFileSortedOplog(new File("test.soplog"), config);

    SortedOplogWriter wtr = hfile.createWriter();
    for (Entry<byte[], byte[]> entry : data.entrySet()) {
      wtr.append(entry.getKey(), entry.getValue());
    }
    wtr.close(null);

    return hfile.createReader();
  }
}
