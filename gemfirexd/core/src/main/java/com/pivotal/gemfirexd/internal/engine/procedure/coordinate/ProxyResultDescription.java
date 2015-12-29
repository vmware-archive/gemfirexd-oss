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
import java.util.concurrent.CountDownLatch;

import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.iapi.sql.ResultColumnDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.ResultDescription;

/***
 * This class is concurrent version of the GenericResultDescription.
 * In a distributed environment,  the dynamic result sets
 * could still not be ready when users access them. For the same reason, we do not have any idea
 * on these column meta information untill the incoming result set is ready to be accessed.  
 * @author yjing
 *
 */
public class ProxyResultDescription implements ResultDescription {      
   private volatile ResultDescription rd;
   //latch is not null when it is in coordinator; otherwise, it is null;
   private CountDownLatch latch;  
   
   private volatile boolean ready=false;
   

  public ProxyResultDescription(boolean isCoordinator) {
    if(isCoordinator) {
      this.latch=new CountDownLatch(1);
      
    }
    else {
      this.latch=null;
    }              
  }
  
  public int findColumnInsensitive(String name) {
    checkReady();
    if(this.rd==null) {
      return 0;
   }
    return this.rd.findColumnInsensitive(name);
    
  }

  public int getColumnCount() {
     checkReady();
     if(this.rd==null) {
       return 0;
    }
     return this.rd.getColumnCount();
  }

  public ResultColumnDescriptor getColumnDescriptor(int position) {
    checkReady();
    if(this.rd==null) {
      return null;
   }
    return this.rd.getColumnDescriptor(position);
  }

  public ResultColumnDescriptor[] getColumnInfo() {
    checkReady();
    if(this.rd==null) {
      return null;
   }
    return this.rd.getColumnInfo();
  }

  public ResultSetMetaData getMetaData() {
    checkReady();
    if(this.rd==null) {
       return null;
    }
    return this.rd.getMetaData();
  }

  public String getStatementType() {
    checkReady();
    if(this.rd==null) {
      return null;
    }
    return this.rd.getStatementType();
  }

  public void setMetaData(ResultSetMetaData rsmd) {
    checkReady();
    if(this.rd!=null) {
      this.rd.setMetaData(rsmd);
    }

  }

  public boolean isSet() {
    return this.ready;
  }

  /***
   * This method is used to set the real result description.
   * @param rd
   */
  public synchronized void setResultDescription(ResultDescription rd) {
    //if the input result description is null or
    //the result description has been set;
    if(rd==null || this.ready) {
      return;
    }
    this.rd=rd;
    this.ready=true;
    if(this.latch!=null) {
       this.latch.countDown();
    }
  }
  /***
   * this method is called when no data node executes the procedure; so we need to
   * make all related stuff, such as result set, result description, and parameters.
   */
  
  public void setReady() {
    this.ready=true;
    this.latch.countDown();
  }

  private void checkReady() {
    if (this.latch == null || this.ready) {
      return;
    }
    while (true) {
      try {
        this.latch.await();
        break;
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        Misc.getGemFireCache().getCancelCriterion().checkCancelInProgress(ie);
      }
    }
  }
}
