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
package com.gemstone.gemfire.cache.asyncqueue;

import com.gemstone.gemfire.cache.util.Gateway.OrderPolicy;

/**
 * Interface of AsyncEventQueue. 
 * This represents the channel over which the events are delivered to the <code>AsyncEventListener</code>. 
 * 
 * @author pdeole
 * @since 7.0
 */
public interface AsyncEventQueue {

  /**
   * @return String  Id of the AsyncEventQueue  
   */
  public String getId();
  
  /**
   * The Disk store that is required for overflow and persistence
   * @return    String
   */
  public String getDiskStoreName();//for overflow and persistence
  
  /**
   * The maximum memory after which the data needs to be overflowed to disk
   * @return    int
   */
  public int getMaximumQueueMemory();//for overflow
  
  /**
   * Represents the size of a batch that gets delivered over the AsyncEventQueue 
   * @return    int
   */
  public int getBatchSize();
  
  /**
   * Represents the maximum time interval that can elapse before a batch is sent 
   * from <code>AsyncEventQueue</code>
   * 
   * @return    int
   */
  public int getBatchTimeInterval();
  
  /**
   * Represents whether batch conflation is enabled for batches sent 
   * from <code>AsyncEventQueue</code>
   * @return    boolean
   */
  public boolean isBatchConflationEnabled();
  
  /**
   * Represents whether the AsyncEventQueue is configured to be persistent or non-persistent
   * @return    boolean
   */
  public boolean isPersistent();
  
  /**
   * Represents whether writing to disk is synchronous or not.
   * @return    boolean
   */
  public boolean isDiskSynchronous();
  
  /**
   * Represents whether the queue is primary or secondary. 
   * Events get delivered only by the primary queue. 
   * If the primary queue goes down then the secondary queue first becomes primary 
   * and then starts delivering the events.  
   * @return    boolean
   */
  public boolean isPrimary();

  /**
   * The <code>AsyncEventListener</code> that is attached to the queue. 
   * All the event passing over the queue are delivered to attached listener.
   * @return    AsyncEventListener      Implementation of AsyncEventListener
   */
  public AsyncEventListener getAsyncEventListener();
  
  /**
   * Represents whether this queue is parallel (higher throughput) or serial.
   * @return    boolean    True if the queue is parallel, false otherwise.
   */
  public boolean isParallel();

  /**
   * Starts the event sender for this AsyncEventQueue. Once the GatewaySender is
   * running, its configuration cannot be changed.
   */
  public void start();

  /**
   * Stops the event sender for this AsyncEventQueue.
   */
  public void stop();

  /**
   * Destroys the event sender for this AsyncEventQueue.
   */
  public void destroy();

  /**
   * Pauses the event sender for this AsyncEventQueue.
   */
  public void pause();

  /**
   * Resumes the paused event sender for this AsyncEventQueue.
   */
  public void resume();

  /**
   * Returns whether or not this AsyncEventQueue is running.
   */
  public boolean isRunning();

  /**
   * Returns whether or not this AsyncEventQueue is paused.
   */
  public boolean isPaused();

  /**
   * Returns the number of dispatcher threads working for this <code>AsyncEventQueue</code>
   * 
   * @return the number of dispatcher threads working for this <code>AsyncEventQueue</code>
   */
  public int getDispatcherThreads();

  /**
   * Returns the order policy followed while dispatching the events to AsyncEventListener.
   * Order policy is set only when dispatcher threads are > 1
   * @return the order policy followed while dispatching the events to AsyncEventListener.
   */
  public OrderPolicy getOrderPolicy();

  /**
   * Returns the number of entries in this <code>AsyncEventQueue</code>.
   * @return the number of entries in this <code>AsyncEventQueue</code>.
   */
  public int size();
}
