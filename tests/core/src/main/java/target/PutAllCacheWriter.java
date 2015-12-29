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
package target;

import hydra.Log;

import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.util.CacheWriterAdapter;


public class PutAllCacheWriter extends CacheWriterAdapter{ 
  /** Counts the number of creates in the region
   */
  static private long numCreates = 0;
  
  /** Delay to be introduced in performing a put operation.
   */
  private long delayInBetweenPutsMS = 0; 
  
  /** Flag to decide the usage of black board
   * Black board is used for writing the numCreates during the Client/Server test
   */
  private boolean useBlackBoard = false;
  
  public PutAllCacheWriter(long putDelay, boolean useBlackBoard) {
    this.delayInBetweenPutsMS = putDelay;
    this.useBlackBoard        = useBlackBoard;
    TargetBB.getBB().getSharedMap().put("numCreates", new Integer(0));
  }
  
  public PutAllCacheWriter(long putDelay) {
    this.delayInBetweenPutsMS = putDelay;
  }
  
  
  /** Logs the number of entries created, delays any put/putAll operation by the specified delay
   * @see com.gemstone.gemfire.cache.util.CacheWriterAdapter#beforeCreate(com.gemstone.gemfire.cache.EntryEvent)
   */
  public void beforeCreate(EntryEvent event) throws CacheWriterException {
    ++numCreates;
    Log.getLogWriter().info("Number of creates " + numCreates);
    
    if (useBlackBoard)
      TargetBB.getBB().getSharedMap().put("numCreates", new Integer((int) numCreates));
    
    try {
      Thread.sleep(delayInBetweenPutsMS);
    } catch (Exception e) {
      Log.getLogWriter().info("beforeCreate : " + e.getMessage());
    }
  }
  
  /** Gets the number of entries created in a region
   *
   */
  public long getNumCreates() {
    return numCreates;
  }
  
  /** Set the delay in-between puts
   *  
   */
  public void setDelay(long delay) {
    this.delayInBetweenPutsMS = delay;
  }
  
  public void useBlackBoard(boolean useBlackBoard) {
    this.useBlackBoard = useBlackBoard; 
  }

}
