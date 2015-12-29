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
package com.pivotal.gemfirexd.internal.engine.procedure.coordinate;

import java.sql.ResultSetMetaData;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import com.pivotal.gemfirexd.internal.iapi.sql.ResultDescription;
import com.pivotal.gemfirexd.procedure.IncomingResultSet;

/**
 *  I thought this is not a good implementation. However, it reuses the code awkwardly and
 *  ensures the correctness of added methods (peekRow with time and waitPeekRow). 
 * @author yjing
 *
 */

public class IncomingResultSetImpl implements IncomingResultSet {
  private ReentrantLock lock=new ReentrantLock();
  private Condition  notEmpty=lock.newCondition(); 
  private LinkedBlockingQueue<List<Object>> queue;
  private boolean finished;
 
  
  public IncomingResultSetImpl() {
      this.queue=new LinkedBlockingQueue<List<Object>> ();
      this.finished=false;
  }

  public ResultSetMetaData getMetaData() {   
    return null;
  }

  public List<Object> peekRow() {  
      return this.queue.peek();
  }

  public List<Object> peekRow(long timeout, TimeUnit unit)
  throws InterruptedException {
    long remainingTime = unit.toNanos(timeout);
    List<Object> retValue = null;
    lock.lock();
    try {
      while (true) {      
          retValue = this.queue.peek();
          if (retValue != null) {
            // to make sure the entry still exists in the queue, the lock should
            // be used
            // to other queue modification operator;
            break;
          }
          // queue.peek==null
          if (remainingTime <= 0) {
            break;
          }
          remainingTime = notEmpty.awaitNanos(remainingTime);     
      }
      return retValue;
    }
    finally {

      lock.unlock();
    }

  }

  public List<Object> pollRow() {
    //lock.lock();
    try {
     return this.queue.poll();
    }
    finally {
     // this.lock.unlock();
    }
  }

  public List<Object> pollRow(long timeout, TimeUnit unit)
      throws InterruptedException {
   // lock.lock();
    try {
      return this.queue.poll(timeout, unit);
    }
    finally {
     // this.lock.unlock();
    }
    
      
  }

  public List<Object> takeRow() throws InterruptedException {
     
   // lock.lock();
    try {
      return this.queue.take();
    }
    finally {
    //  this.lock.unlock();
    }
          
  }

  public List<Object> waitPeekRow() throws InterruptedException {    
    List<Object> retValue = null;
    lock.lock();
    try {
      while (true) {      
          retValue = this.queue.peek();
          if (retValue != null) {
            // to make sure the entry still exists in the queue, the lock should
            // be used
            // to other queue modification operator;
            break;
          }              
          notEmpty.await();     
      }
      return retValue;
    }
    finally {

      lock.unlock();
    }
        
  }
  
  public void addRow(List<Object> row) {
    lock.lock();
    try {
      this.queue.add(row);
      this.notEmpty.signalAll();
    }
    finally {
      lock.unlock();
    }
  }

 public void finish() {
    this.finished=true;
 }
 public boolean isFinished() {
      return this.finished;
 }

}

