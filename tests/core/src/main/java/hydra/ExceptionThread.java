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

package hydra;

import com.gemstone.gemfire.SystemFailure;

/**
 *
 *  A thread subclass whose instances contain results and exceptions obtained
 *  during execution.
 *
 */
public class ExceptionThread extends Thread {

  //////////////////////////////////////////////////////////////////////////////
  ////    INSTANCE FIELDS                                                   ////
  //////////////////////////////////////////////////////////////////////////////

  /** result obtained by this instance */
  private Object result; 

  /** exception thrown by this instance */
  private Throwable exception; 

  /** thread group used by all instances of this thread */
  static ThreadGroup group = new ThreadGroup( "ExceptionThreadGroup" ) {
    public void uncaughtException( Thread thread, Throwable ex ) {
      if (ex instanceof VirtualMachineError) {
        SystemFailure.setFailure((VirtualMachineError)ex); // don't throw
      }
      ( (ExceptionThread) thread ).setException( ex );
    }
  };

  //////////////////////////////////////////////////////////////////////////////
  ////    CONSTRUCTORS                                                      ////
  //////////////////////////////////////////////////////////////////////////////

  public ExceptionThread( Runnable target, String name ) {
     super( group, target, name );
     this.exception = null;
     this.result = null;
  }

  //////////////////////////////////////////////////////////////////////////////
  ////    ACCESSORS                                                         ////
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Fetches the result, if any, generated during this thread's execution.
   */
  public Object getResult() {
    return this.result;
  }

  /**
   * Stores the result, if any, generated during this thread's execution.
   */
  public void setResult(Object obj) {
    this.result = obj;
  }

  /**
   *  Fetches the exception, if any, generated during this thread's execution.
   */
  public Throwable getException() {
    return this.exception;
  }

  /**
   *  Stores the exception, if any, generated during this thread's execution.
   */
  public void setException( Throwable ex ) {
    this.exception = ex;
  }
}
