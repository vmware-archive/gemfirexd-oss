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

import java.util.*;

public class ResourceOperation {

  public static Object doOp( Runnable op, String opname, String resourcetype, Object record, int timeoutMs, boolean verbose ) {
    ExceptionThread thread = new ExceptionThread( op, opname );
    if (verbose) {
      Log.getLogWriter().info("Executing operation \"" + opname + "\" on "
                             + resourcetype + " " + record);
    }
    thread.start();
    return joinUpWith( thread, opname, resourcetype, record, timeoutMs, verbose );
  }
  public static Object[] doOps( Vector ops, String opname, String resourcetype, Vector records, int timeoutMs, boolean concurrent, int delayMs, boolean verbose ) {
    int numOps = ops.size();
    if ( numOps == 0 )
      return null;
    if ( numOps != records.size() )
      throw new HydraInternalException( "Number of ops (" + numOps +
            ") not equal to number of records (" + records.size() + ")" );

    ExceptionThread[] threads = new ExceptionThread[ numOps ];
    Object[] results = new Object[numOps];
    for ( int i = 0; i < numOps; i++ ) {
      Runnable op = (Runnable) ops.elementAt(i);
      threads[i] = new ExceptionThread( op, opname );
      if (verbose) {
        Log.getLogWriter().info("Executing operation \"" + opname + "\" on "
                                + resourcetype + " " + records.elementAt(i));
      }
      threads[i].start();
      if ( ! concurrent ) {
        results[i] = joinUpWith( threads[i], opname, resourcetype, records.elementAt(i), timeoutMs, verbose );
      } else if ( delayMs > 0 ) {
        MasterController.sleepForMs( delayMs );
      }
    }
    if ( concurrent ) {
      for ( int i = 0; i < numOps; i++ ) {
        results[i] = joinUpWith( threads[i], opname, resourcetype, records.elementAt(i), timeoutMs, verbose );
      }
    }
    return results;
  }
  private static Object joinUpWith( ExceptionThread thread, String opname, String resourcetype, Object record, int timeoutMs, boolean verbose ) {
    try {
      thread.join( timeoutMs );
    } catch( InterruptedException e ) {
      throw new HydraRuntimeException( "Interrupted during operation \"" +
                                        opname +
                                      "\" on " + resourcetype + " " + record, e );
    }
    Throwable e = thread.getException();
    if ( e instanceof HydraTimeoutException ) {
      String s = "Timeout exception during operation \"" + opname + "\" on " +
        resourcetype + " " + record;
      throw new HydraTimeoutException( s, e );

    } else if ( e != null ) {
      throw new HydraRuntimeException( "Exception during operation \"" +
                                        opname +
                                      "\" on " + resourcetype + " " + record, e );

    } else if ( thread.isAlive() ) {
      String s = "Timed out after waiting " + timeoutMs +
        " milliseconds for operation \"" + opname + "\" on " +
        resourcetype + " " + record;
      throw new HydraTimeoutException( s, e );

    } else if (verbose) {
      Log.getLogWriter().info( "Executed operation \"" +
                                opname +
                              "\" on " + resourcetype + " " + record );
    }
    return thread.getResult();
  }
}
