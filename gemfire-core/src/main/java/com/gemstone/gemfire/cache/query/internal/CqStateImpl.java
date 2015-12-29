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

package com.gemstone.gemfire.cache.query.internal;

import com.gemstone.gemfire.cache.query.CqState;

/**
 * Offers methods to get CQ state.
 *
 * @author anil
 * @since 5.5
 */

public class CqStateImpl implements CqState {

  public static final int STOPPED = 0;

  public static final int RUNNING = 1;

  public static final int CLOSED = 2;
  
  public static final int CLOSING = 3;
  
  private volatile int state = STOPPED;
  
  
  /**
   * Returns true if the CQ is in Running state.
   */
  public boolean isRunning(){
    return (this.state == RUNNING);
  }

  /**
   * Returns true if the CQ is in Stopped state.
   */
  public boolean isStopped() {
    return (this.state == STOPPED);
  }
  
  /**
   * Returns true if the CQ is in Closed state.
   */
  public boolean isClosed() {
    return (this.state == CLOSED);    
  }
  
  /**
   * Returns true if the CQ is in the Closing state.
   */
  public boolean isClosing() {
    return (this.state == CLOSING);
  }
  
  /**
   * Sets the state of CQ.
   * @param state
   */
  public void setState(int state){
    this.state = state;
  }

  /**
   * Returns the integer state of CQ.
   */
  public int getState() {
    return this.state;
  }
  
  /**
   * Returns the state in string form.
   */
  @Override
  public String toString() {
    switch (this.state){    
      case STOPPED:
        return "STOPPED";
      case RUNNING:
        return "RUNNING";
      case CLOSED:
        return "CLOSED";
      default:
        return "UNKNOWN";
    }
  }

}
