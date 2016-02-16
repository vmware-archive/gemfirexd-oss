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
package com.gemstone.gemfire.internal.cache;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

import io.snappydata.test.dunit.DistributedTestBase;

/**
 * @author dsmith
 *
 */
public class TestUtils {
  public static <T> Set<T> asSet(T ... objects) {
    return new HashSet<T> (Arrays.asList(objects));
  }
  
  /**
   * Add an expected exception to the log message for junit tests. 
   * 
   * For dunit
   * tests, use {@link DistributedTestBase#addExpectedException(String)}
   * 
   * Be sure to all remove on the exception when you are done. 
   */
  public static ExpectedException addExpectedException(String message) {
    ExpectedException ex = new ExpectedException(message);
    ex.add();
    return ex;
  }
  
  public static void removeExpectedException(String message) {
    ExpectedException ex = new ExpectedException(message);
    ex.remove();
  }
  
  public static class ExpectedException {
    private final String string;
    
    
    public ExpectedException(String string) {
      this.string = string;
    }
    
    private void add() {
      writeToLoggers("<ExpectedException action=add>" + string + "</ExpectedException>");
    }


    private void writeToLoggers(String message) {
      LogWriterI18n logger = InternalDistributedSystem.getLoggerI18n();
      if(logger != null) {
        logger.info(LocalizedStrings.DEBUG, message);
      }
      System.out.println(message);
       
    }

    public void remove() {
      writeToLoggers("<ExpectedException action=remove>" + string + "</ExpectedException>");
    }
  }

}
