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

package util;

import com.gemstone.gemfire.*;

import java.io.*;

/**
 *  Generic utility methods.
 */

public class Util {
  /**
   *  Returns the identity hash code of the object.  Used to uniquely identify objects in JOM.
   */
  public static int jomoid( Object obj ) {
    return System.identityHashCode( obj );
  }
  /**
   *  Prints a debug stack trace with the message to the log, using the log level.
   *  @throws IllegalArgumentException if the log level is invalid.
   */
  public static void printStackTrace( String msg, LogWriter log, String logLevel ) {
    if ( logLevel.equals( "severe" ) && log.severeEnabled() )
      log.severe( getStackTrace( msg ) );
    else if ( logLevel.equals( "warning" ) && log.warningEnabled() )
      log.warning( getStackTrace( msg ) );
    else if ( logLevel.equals( "info" ) && log.infoEnabled() )
      log.info( getStackTrace( msg ) );
    else if ( logLevel.equals( "config" ) && log.configEnabled() )
      log.config( getStackTrace( msg ) );
    else if ( logLevel.equals( "fine" ) && log.fineEnabled() )
      log.fine( getStackTrace( msg ) );
    else if ( logLevel.equals( "finer" ) && log.finerEnabled() )
      log.finer( getStackTrace( msg ) );
    else if ( logLevel.equals( "finest" ) && log.finestEnabled() )
      log.finest( getStackTrace( msg ) );
    else
      throw new IllegalArgumentException( "Invalid log level: " + logLevel );
  }
  private static String getStackTrace( String msg ) {
    if ( msg == null ) msg = "";
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintWriter s = new PrintWriter( baos, true );
    new Exception( msg ).printStackTrace( s );
    return baos.toString();
  }
}
