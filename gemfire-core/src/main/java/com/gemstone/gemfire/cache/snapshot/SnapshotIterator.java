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
package com.gemstone.gemfire.cache.snapshot;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.Map.Entry;

/**
 * Iterates over the entries in a region snapshot.  Holds resources that must
 * be freed via {@link #close()}.
 * 
 * @param <K> the key type of the snapshot region
 * @param <V> the value type the snapshot region
 * 
 * @see SnapshotReader
 * 
 * @author bakera
 * @since 7.0
 */
public interface SnapshotIterator<K, V> {
  /**
   * Returns true if there are more elements in the iteration.
   * 
   * @return true if the iterator has more elements.
   * 
   * @throws IOException error reading the snapshot
   * @throws ClassNotFoundException error deserializing the snapshot element
   */
  boolean hasNext() throws IOException, ClassNotFoundException;
  
  /**
   * Returns the next element in the iteration.
   * 
   * @return the next element
   * 
   * @throws NoSuchElementException there are no further elements
   * @throws IOException error reading the snapshot
   * @throws ClassNotFoundException error deserializing the snapshot element
   */
  Entry<K, V> next() throws IOException, ClassNotFoundException;

  /**
   * Closes the iterator and its underlying resources.
   * @throws IOException error closing the iterator
   */
  void close() throws IOException;
}
