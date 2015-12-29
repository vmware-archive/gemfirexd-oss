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

import java.io.*;

/**
 *  A TestTaskResult represents the result of executing a test task, taking
 *  into account the error status.
 */

public class TestTaskResult implements Serializable {

  /** The result value */
  protected Object result;

  /** How long it took to execute the task */
  protected long elapsedTime;

  /** True if there was an error (unexpected exception) */
  protected boolean errorStatus = false;

  /** String describing the error */
  protected String errorString;

  /**
   *  Creates a new test task result based on the actual result of test
   *  task execution.  Sets error conditions as appropriate.
   */
  public TestTaskResult( MethExecutorResult actualResult, long elapsedTime ) {
    initialize( actualResult, elapsedTime );
  }

  /**
   *  Protected constructor
   */
  protected TestTaskResult( long elapsedTime ) {
    this.elapsedTime = elapsedTime;
  }

  private void initialize( MethExecutorResult result, long elapsedTime ) {
    Object o = result.getResult();
    if ( result.exceptionOccurred() ) {
      Throwable t = result.getException();
      if ( t instanceof SchedulingOrder ) { // not a real exception
        setResult( t );
      } else if ( t instanceof HydraTimeoutException ) { // a hang
        setResult( t );
        setErrorStatus(true);
        setErrorString("HANG " + result.getExceptionClassName() + ": " +
                        result.getExceptionMessage() + "\n\n" +
                        result.getStackTrace() );
      } else { // a non-hang exception
        setResult(o);
        setErrorStatus(true);
        setErrorString("ERROR " + result.getExceptionClassName() + ": " +
                        result.getExceptionMessage() + "\n\n" +
                        result.getStackTrace() );
      }
    } else { // normal result
      if ( o == null ) {
        setResult("void");
      } else {
        setResult(o);
      }
    }
    setElapsedTime( elapsedTime );
  }

  //// Accessors

  public Object getResult() {
    return result;
  }
  public void setResult(Object anObj) {
    result = anObj;
  }

  public long getElapsedTime() {
    return elapsedTime;
  }
  public void setElapsedTime(long aLong) {
    elapsedTime = aLong;
  }

  /**
   * Returns <code>true</code> if there was an error
   */
  public boolean getErrorStatus() {
    return errorStatus;
  }
  private void setErrorStatus(boolean aBool) {
    errorStatus = aBool;
  }

  public String getErrorString() {
    return errorString;
  }
  public void setErrorString(String aString) {
    errorString = aString;
  }

  public String toString() {
    StringBuffer buf = new StringBuffer(100);
    buf.append( this.result );
    if ( this.errorStatus ) {
      buf.append( "\n" );
      if ( this.errorString != null )
        buf.append( this.errorString );
    }
    return buf.toString();
  }
}
