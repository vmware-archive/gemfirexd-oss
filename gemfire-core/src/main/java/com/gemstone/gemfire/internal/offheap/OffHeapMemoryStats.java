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
package com.gemstone.gemfire.internal.offheap;

import com.gemstone.gemfire.Statistics;

/**
 * Statistics for off-heap memory storage.
 * 
 * @author Kirk Lund
 * @since 7.5
 */
public interface OffHeapMemoryStats {

  public void incFreeMemory(long value);
  public void incMaxMemory(long value);
  public void incUsedMemory(long value);
  public void incObjects(int value);
  public void incReads();
  public void setFragments(long value);
  public void setLargestFragment(int value);
  public long startCompaction();
  public void endCompaction(long start);
  public void setFragmentation(int value);
  
  public long getFreeMemory();
  public long getMaxMemory();
  public long getUsedMemory();
  public long getReads();
  public int getObjects();
  public int getCompactions();
  public long getFragments();
  public int getLargestFragment();
  public int getFragmentation();
  public long getCompactionTime();
  
  public Statistics getStats();
  public void close();
  public void initialize(OffHeapMemoryStats stats);
}
