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

import java.util.List;

/**
 * Provides for inspection of meta-data for off-heap memory blocks.
 * 
 * @author Kirk Lund
 */
public interface MemoryInspector {

  public void clearInspectionSnapshot();
  
  public void createInspectionSnapshot();

  public MemoryBlock getFirstBlock();
  
  public List<MemoryBlock> getAllBlocks();
  
  public List<MemoryBlock> getAllocatedBlocks();
  
  public List<MemoryBlock> getDeallocatedBlocks();
  
  public List<MemoryBlock> getUnusedBlocks();
  
  public MemoryBlock getBlockContaining(long memoryAddress);
  
  public MemoryBlock getBlockAfter(MemoryBlock block);
  
  public List<MemoryBlock> getOrphans();
}
