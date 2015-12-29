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
package com.pivotal.gemfirexd.internal.engine.store.entry;

import java.io.IOException;
import java.util.Iterator;

import com.gemstone.gemfire.cache.hdfs.HDFSIOException;
import com.gemstone.gemfire.cache.hdfs.internal.PersistedEventImpl;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.mapreduce.HDFSSplitIterator;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.pivotal.gemfirexd.internal.iapi.types.RowLocation;

/**
 * Wraps a HDFSSplitIterator construct region entry objects. This
 * iterator can be used by the MemHeapScanController as an iterator
 * over region entries.
 * 
 * @author dsmith
 *
 */
public final class HDFSSplitIteratorWrapper implements Iterator<RowLocation> {

  private HDFSSplitIterator reader;
  private LocalRegion region;
 
  private HDFSSplitIteratorWrapper(LocalRegion region, HDFSSplitIterator splitIterator) {
    this.region = region;
    this.reader = splitIterator;
  }
  
  /**
   * Return an iterator on an HDFS split
   */
  public static Iterator<?> getIterator(LocalRegion region, Object split) {
    HDFSSplitIterator splitIterator = (HDFSSplitIterator) split;
    
    return new HDFSSplitIteratorWrapper(region, splitIterator);
  };

  @Override
  public boolean hasNext() {
    try {
      return reader.hasNext();
    } catch (IOException e) {
      throw new HDFSIOException("Error reading from HDFS", e);
    }
  }

  @Override
  public RowLocation next() {
    try {
      boolean hasNext = reader.next();
      if(!hasNext) {
        return null;
      }
      byte[] key = reader.getKey();
      PersistedEventImpl event = reader.getDeserializedValue();
      return new HDFSEventRowLocationRegionEntry(region, key, event);
    } catch (IOException e) {
      throw new HDFSIOException("Error reading from HDFS", e);
    } catch (ClassNotFoundException e) {
      throw new HDFSIOException("Error reading from HDFS", e);
    } 
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
    
  }

}
