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

package com.gemstone.gemfire.cache.query;

/**
 * This interface gives information on the state of a CqQuery. 
 * It is provided by the getState method of the CqQuery instance. 
 * 
 * @author anil
 * @since 5.5
 */

public interface CqState {
          
  /**
   * Returns the state in string form.
   */
  public String toString();
  
  /**
   * Returns true if the CQ is in Running state.
   */
  public boolean isRunning();

  /**
   * Returns true if the CQ is in Stopped state.
   */
  public boolean isStopped();
  
  /**
   * Returns true if the CQ is in Closed state.
   */
  public boolean isClosed();
  
  /**
   * Returns true if the CQ is in Closing state.
   */
  public boolean isClosing();
  
}
