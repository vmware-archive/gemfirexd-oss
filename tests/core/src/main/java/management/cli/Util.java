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
package management.cli;

import management.util.HydraUtil;

/*-
 import org.apache.log4j.FileAppender;
 import org.apache.log4j.Level;
 import org.apache.log4j.Logger;
 import org.apache.log4j.PatternLayout;\
 */

public class Util {
  /*-
  private static Logger logger = Logger.getLogger(Util.class);
  public static final String LOGGING_PATTERN = "[%p %d{mm/dd/yy HH:mm:ss.SSS} %M %C] %m%n";;
   */
  static {
    /*-
    Logger logger = Logger.getRootLogger();
    	
    	PatternLayout layout = new PatternLayout(LOGGING_PATTERN);
        FileAppender appender = null;
        try {
           String logFileprop="testable-shell-logging.log";
           appender = new FileAppender(layout,logFileprop,false);
        } catch(Exception e) {e.printStackTrace();}

        logger.addAppender(appender);
        logger.setLevel((Level) Level.DEBUG);*/
  }

  public static String printChar(int i) {

    switch (i) {
    case EventedInputStream.TAB:
      return "TAB";
    case -1:
      return "EOF";
    default:
      return "" + (char) i;
    }

  }
  
  private static String ct(){
    return Thread.currentThread().getName();
  }

  public static void log(String message) {
	  HydraUtil.logInfo(message);
  }

  public static void debug(String message) {
	  HydraUtil.logFinest(message);
  }

  public static void error(Exception e) {
	  HydraUtil.logError("Error",e);
  }
  
  public static void error(String e) {
	  HydraUtil.logError(e);
  }

  public static void error(String string, Throwable e) {
	 HydraUtil.logError(string,e);
  }

}
